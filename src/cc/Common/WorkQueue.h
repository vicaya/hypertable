/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
