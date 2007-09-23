/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This file is part of Hypertable.
 * 
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
#include "Common/ReferenceCount.h"

#include "DispatchHandler.h"
#include "ReactorFactory.h"
#include "Timer.h"

namespace hypertable {

  class HandlerMap;

  /**
   *
   */
  class IOHandler : public ReferenceCount {

  public:

    IOHandler(int sd, struct sockaddr_in &addr, DispatchHandler *dh, HandlerMap &hmap) : mAddr(addr), mSd(sd), mDispatchHandler(dh), mHandlerMap(hmap) {
      mReactor = ReactorFactory::GetReactor();
      mPollInterest = 0;
      socklen_t namelen = sizeof(mLocalAddr);
      getsockname(mSd, (sockaddr *)&mLocalAddr, &namelen);
    }

#if defined(__APPLE__)
    virtual bool HandleEvent(struct kevent *event) = 0;
#elif defined(__linux__)
    virtual bool HandleEvent(struct epoll_event *event) = 0;
#else
    ImplementMe;
#endif

    virtual ~IOHandler() { return; }

    void DeliverEvent(Event *event, DispatchHandler *dh=0) {
      DispatchHandler *handler = (dh == 0) ? mDispatchHandler : dh;
      memcpy(&event->localAddr, &mLocalAddr, sizeof(mLocalAddr));
      if (handler == 0) {
	LOG_VA_INFO("%s", event->toString().c_str());
	delete event;
      }
      else {
	EventPtr eventPtr(event);
	handler->handle(eventPtr);
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

    struct sockaddr_in &GetAddress() { return mAddr; }

    void GetLocalAddress(struct sockaddr_in *addrPtr) {
      memcpy(addrPtr, &mLocalAddr, sizeof(struct sockaddr_in));
    }

    int GetSd() { return mSd; }

    Reactor *GetReactor() { return mReactor; }

    HandlerMap &GetHandlerMap() { return mHandlerMap; }

    void Shutdown() { 
      struct TimerT timer;
      mReactor->ScheduleRemoval(this);
      boost::xtime_get(&timer.expireTime, boost::TIME_UTC);
      timer.expireTime.nsec += 200000000LL;
      timer.handler = 0;
      mReactor->AddTimer(timer);
    }

  protected:

    void StopPolling() {
#if defined(__APPLE__)
      RemovePollInterest(Reactor::READ_READY|Reactor::WRITE_READY);
#elif defined(__linux__)
      struct epoll_event event;  // this is necessary for < Linux 2.6.9
      if (epoll_ctl(mReactor->pollFd, EPOLL_CTL_DEL, mSd, &event) < 0) {
	LOG_VA_ERROR("epoll_ctl(%d, EPOLL_CTL_DEL, %d) failed : %s",
		     mReactor->pollFd, mSd, strerror(errno));
	exit(1);
      }
      mPollInterest = 0;
#endif
    }

    struct sockaddr_in  mAddr;
    struct sockaddr_in  mLocalAddr;
    int                 mSd;
    DispatchHandler    *mDispatchHandler;
    HandlerMap         &mHandlerMap;
    Reactor            *mReactor;
    int                 mPollInterest;

#if defined(__APPLE__)
    void DisplayEvent(struct kevent *event);
#elif defined(__linux__)
    void DisplayEvent(struct epoll_event *event);
#endif
  };
  typedef boost::intrusive_ptr<IOHandler> IOHandlerPtr;

  struct ltiohp {
    bool operator()(const IOHandlerPtr &p1, const IOHandlerPtr &p2) const {
      return p1.get() < p2.get();
    }
  };

}


#endif // HYPERTABLE_IOHANDLER_H
