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

#ifndef HYPERTABLE_APPLICATIONQUEUE_H
#define HYPERTABLE_APPLICATIONQUEUE_H

#include <cassert>
#include <list>
#include <ext/hash_map>

#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include "ApplicationHandler.h"

namespace __gnu_cxx {
  template<> struct hash< uint64_t > {
    size_t operator()( const uint64_t val ) const {
      return size_t(val);
    }
  };
}


namespace hypertable {

  class ApplicationQueue {

    class UsageRec {
    public:
      UsageRec() : threadGroup(0), running(false), outstanding(1) { return; }
      uint64_t threadGroup;
      bool     running;
      int      outstanding;
    };

    typedef __gnu_cxx::hash_map<uint64_t, UsageRec *> UsageRecMapT;

    class WorkRec {
    public:
      WorkRec(ApplicationHandlerPtr &ahPtr) : appHandlerPtr(ahPtr), usage(0) { return; }
      ApplicationHandlerPtr appHandlerPtr;
      UsageRec             *usage;
    };

    class ApplicationQueueState {
    public:
      ApplicationQueueState() : queue(), usageMap(), queueMutex(), usageMutex(), cond(), shutdown(false) { return; }
      list<WorkRec *>     queue;
      UsageRecMapT        usageMap;
      boost::mutex        queueMutex;
      boost::mutex        usageMutex;
      boost::condition    cond;
      bool                shutdown;
    };

    class Worker {

    public:

      Worker(ApplicationQueueState &qstate) : mState(qstate) {
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
	    rec->appHandlerPtr->run();
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
      ApplicationQueueState &mState;
    };

    ApplicationQueueState  mState;
    boost::thread_group    mThreads;

  public:

    ApplicationQueue(int WorkerCount) {
      Worker Worker(mState);
      assert (WorkerCount > 0);
      for (int i=0; i<WorkerCount; ++i)
	mThreads.create_thread(Worker);
      //threads
    }

    void Shutdown() {
      mState.shutdown = true;
      mState.cond.notify_all();
      mThreads.join_all();
    }

    void Add(ApplicationHandlerPtr &appHandlerPtr) {
      UsageRecMapT::iterator uiter;
      uint64_t threadGroup = appHandlerPtr->GetThreadGroup();
      WorkRec *rec = new WorkRec(appHandlerPtr);
      rec->usage = 0;

      if (threadGroup != 0) {
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


#endif // HYPERTABLE_APPLICATIONQUEUE_H
