/**
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

#ifndef HYPERTABLE_WORKQUEUE_H
#define HYPERTABLE_WORKQUEUE_H

#include <cassert>
#include <list>
#include <queue>

#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include "Runnable.h"

namespace hypertable {

  class WorkQueue {

  public:

    WorkQueue(int workerCount) : mThreadList(), mMutex(), mCond(), mQueue() {
      Worker worker(&mMutex, &mCond, &mQueue);
      assert (workerCount > 0);
      for (int i=0; i<workerCount; i++) {
	boost::thread *thread = new boost::thread(worker);
	mThreadList.push_back(thread);
      }
    }

    void AddRequest(Runnable *task) {
      boost::mutex::scoped_lock lock(mMutex);
      mQueue.push(task);
      mCond.notify_one();
    }

    void Join() {
      for (std::list<boost::thread *>::iterator iter = mThreadList.begin(); iter != mThreadList.end(); iter++)
	(*iter)->join();
    }

  private:

    std::list<boost::thread *> mThreadList;
    boost::mutex               mMutex;
    boost::condition           mCond;
    std::queue<Runnable *>     mQueue;

    class Worker {
    public:

      Worker(boost::mutex *mutexp, boost::condition *condp, std::queue<Runnable *> *queuep) : mMutexPtr(mutexp), mCondPtr(condp), mQueuePtr(queuep) {
	return;
      }

      void operator()() {
	Runnable *task = 0;

	while (true) {

	  {
	    boost::mutex::scoped_lock lock(*mMutexPtr);
	    while (mQueuePtr->empty())
	      mCondPtr->wait(lock);

	    task = mQueuePtr->front();
	    mQueuePtr->pop();
	  }

	  task->run();
	  delete task;
	}
      }

    private:
      boost::mutex            *mMutexPtr;
      boost::condition        *mCondPtr;
      std::queue<Runnable *>  *mQueuePtr;
    };
  };

}

#endif // HYPERTABLE_WORKQUEUE_H
