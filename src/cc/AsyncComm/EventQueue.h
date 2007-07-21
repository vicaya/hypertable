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
