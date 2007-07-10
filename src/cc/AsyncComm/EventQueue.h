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


#ifndef HYPERTABLE_EVENTQUEUE_H
#define HYPERTABLE_EVENTQUEUE_H

#include <queue>
using namespace std;

#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>

#include "CallbackHandler.h"
#include "Event.h"

namespace hypertable {

  class EventQueue {

    class EventData {
    public:
      EventData() : event(0), handler(0) { return; }
      EventData(Event *e, CallbackHandler *h) : event(e), handler(h) { return; }
      Event           *event;
      CallbackHandler *handler;
    };

  public:

    void Add(Event *event, CallbackHandler *cbh) {
      boost::mutex::scoped_lock lock(mMutex);
      mQueue.push(EventData(event,cbh));
      mCond.notify_one();
    }

    void HandlerLoop() {
      EventData ed;
      while (true) {

	{
	  boost::mutex::scoped_lock lock(mMutex);
	  while (mQueue.empty())
	    mCond.wait(lock);
	  EventData &tdata = mQueue.front();
	  mQueue.pop();
	  ed.event = tdata.event;
	  ed.handler = tdata.handler;
	}

	ed.handler->handle(*(ed.event));
      }

    }

  private:
    queue<EventData>  mQueue;
    boost::mutex     mMutex;
    boost::condition mCond;
  };

}


#endif // HYPERTABLE_EVENTQUEUE_H
