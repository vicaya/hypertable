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


#ifndef HYPERTABLE_IOHANDLER_H
#define HYPERTABLE_IOHANDLER_H

extern "C" {
#include <errno.h>
#include <sys/types.h>
#include <sys/time.h>
#if defined(__APPLE__)
#include <sys/event.h>
#endif
#if defined(__linux__)
#include <sys/epoll.h>
#endif
}

#include "Common/Logger.h"

#include "CallbackHandler.h"
#include "EventQueue.h"
#include "ReactorFactory.h"

namespace hypertable {

  class ConnectionMap;

  /**
   *
   */
  class IOHandler {

  public:

    IOHandler(int sd, CallbackHandler *cbh, ConnectionMap &cm, EventQueue *eq) : mSd(sd), mCallback(cbh), mConnMap(cm), mEventQueue(eq) {
      mReactor = ReactorFactory::GetReactor();
      mPollInterest = 0;
    }

#if defined(__APPLE__)
    virtual bool HandleEvent(struct kevent *event) = 0;
#elif defined(__linux__)
    virtual bool HandleEvent(struct epoll_event *event) = 0;
#else
    ImplementMe;
#endif

    virtual ~IOHandler() { return; }

    void DeliverEvent(Event *event, CallbackHandler *cbh=0) {
      CallbackHandler *handler = (cbh == 0) ? mCallback : cbh;
      if (handler == 0) {
	LOG_VA_INFO("%s", event->toString().c_str());
	delete event;
      }
      else {
	if (mEventQueue)
	  mEventQueue->Add(event, handler);
	else {
	  handler->handle(*event);
	  event->header = 0;
	  delete event;
	}
      }
    }

    void StartPolling() {
#if defined(__APPLE__)
      AddPollInterest(Reactor::READ_READY);
#elif defined(__linux__)
      struct epoll_event event;
      memset(&event, 0, sizeof(struct epoll_event));
      event.data.ptr = this;
      event.events = EPOLLIN | EPOLLERR | EPOLLHUP;
      if (epoll_ctl(mReactor->pollFd, EPOLL_CTL_ADD, mSd, &event) < 0) {
	LOG_VA_ERROR("epoll_ctl(%d, EPOLL_CTL_ADD, %d, EPOLLIN|EPOLLERR|EPOLLHUP) failed : %s",
		     mReactor->pollFd, mSd, strerror(errno));
	exit(1);
      }
      mPollInterest |= Reactor::READ_READY;
#endif
    }

    void AddPollInterest(int mode);

    void RemovePollInterest(int mode);

    int GetSd() { return mSd; }

    Reactor *GetReactor() { return mReactor; }

  protected:
    int            mSd;
    CallbackHandler *mCallback;
    ConnectionMap &mConnMap;
    EventQueue    *mEventQueue;
    Reactor       *mReactor;
    int            mPollInterest;

#if defined(__APPLE__)
    void DisplayEvent(struct kevent *event);
#elif defined(__linux__)
    void DisplayEvent(struct epoll_event *event);
#endif
  };

}


#endif // HYPERTABLE_IOHANDLER_H
