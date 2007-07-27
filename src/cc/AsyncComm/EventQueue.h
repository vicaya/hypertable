/** -*- C++ -*-
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#ifndef HYPERTABLE_EVENTQUEUE_H
#define HYPERTABLE_EVENTQUEUE_H

#include <cassert>
#include <list>
#include <ext/hash_map>

#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include "CallbackHandler.h"
#include "Event.h"

namespace hypertable {

  class EventQueue {

    class UsageRec {
    public:
      UsageRec() : threadGroup(0), running(false), outstanding(1) { return; }
      long  threadGroup;
      bool  running;
      int   outstanding;
    };

    typedef __gnu_cxx::hash_map<uint64_t, UsageRec *> UsageRecMapT;

    class WorkRec {
    public:
      WorkRec() : handler(0), usage(0) { return; }
      EventPtr         eventPtr;
      CallbackHandler *handler;
      UsageRec        *usage;
    };

    class EventQueueState {
    public:
      EventQueueState() : queue(), usageMap(), queueMutex(), usageMutex(), cond(), shutdown(false) { return; }
      list<WorkRec *>     queue;
      UsageRecMapT        usageMap;
      boost::mutex        queueMutex;
      boost::mutex        usageMutex;
      boost::condition    cond;
      bool                shutdown;
    };

    class Worker {

    public:

      Worker(EventQueueState &qstate) : mState(qstate) {
	return;
      }

      void operator()() {
	WorkRec *rec = 0;
	list<WorkRec *>::iterator iter;

	while (true) {

	  {  // !!! maybe ditch this block specifier
	    boost::mutex::scoped_lock lock(mState.queueMutex);

	    while (mState.queue.empty()) {
	      if (mState.shutdown) {
		cerr << "shutdown!!!" << endl;
		return;
	      }
	      mState.cond.wait(lock);
	    }

	    {
	      boost::mutex::scoped_lock ulock(mState.usageMutex);

	      for (iter = mState.queue.begin(); iter != mState.queue.end(); iter++) {
		rec = (*iter);
		if (rec->usage == 0 || !rec->usage->running) {
		  if (rec->usage)
		    rec->usage->running = true;
		  mState.queue.erase(iter);
		  break;
		}
		rec = 0;
	      }
	    }
	  }
		    
	  if (rec) {
	    rec->handler->handle(rec->eventPtr);
	    if (rec->usage) {
	      boost::mutex::scoped_lock ulock(mState.usageMutex);
	      rec->usage->running = false;
	      rec->usage->outstanding--;
	      if (rec->usage->outstanding == 0) {
		mState.usageMap.erase(rec->usage->threadGroup);
		delete rec->usage;
	      }
	    }
	    delete rec;
	  }
	}

	cerr << "thread exit" << endl;
      }

    private:
      EventQueueState    &mState;
    };

    EventQueueState     mState;
    boost::thread_group mThreads;

  public:

    EventQueue(int workerCount) {
      Worker worker(mState);
      assert (workerCount > 0);
      for (int i=0; i<workerCount; ++i)
	mThreads.create_thread(worker);
      //threads
    }

    void Shutdown() {
      mState.shutdown = true;
      mState.cond.notify_all();
      mThreads.join_all();
    }

    void Add(EventPtr &eventPtr, CallbackHandler *cbh) {
      WorkRec *rec = new WorkRec;
      rec->eventPtr = eventPtr;
      rec->handler = cbh;
      rec->usage = 0;
      {
	boost::mutex::scoped_lock lock(mState.queueMutex);
	mState.queue.push_back(rec);
	mState.cond.notify_one();
      }
    }

    void Add(uint64_t threadGroup, EventPtr &eventPtr, CallbackHandler *cbh) {
      UsageRecMapT::iterator uiter;
      WorkRec *rec = new WorkRec;
      rec->eventPtr = eventPtr;
      rec->handler = cbh;
      {
	boost::mutex::scoped_lock ulock(mState.usageMutex);
	if ((uiter = mState.usageMap.find(threadGroup)) != mState.usageMap.end()) {
	  rec->usage = (*uiter).second;
	  rec->usage->outstanding++;
	}
	else {
	  rec->usage = new UsageRec();
	  rec->usage->threadGroup = threadGroup;
	  mState.usageMap[threadGroup] = rec->usage;
	}
      }
      {
	boost::mutex::scoped_lock lock(mState.queueMutex);
	mState.queue.push_back(rec);
	mState.cond.notify_one();
      }
    }
  };

}


#endif // HYPERTABLE_EVENTQUEUE_H
