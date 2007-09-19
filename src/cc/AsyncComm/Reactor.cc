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

#include <cassert>
#include <cstdio>
#include <iostream>
#include <set>
using namespace std;

extern "C" {
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#if defined(__APPLE__)
#include <sys/event.h>
#endif
}

#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/Logger.h"

#include "IOHandlerData.h"
#include "Reactor.h"
using namespace hypertable;

const int Reactor::READ_READY   = 0x01;
const int Reactor::WRITE_READY  = 0x02;


/**
 * 
 */
Reactor::Reactor() : mMutex(), mInterruptInProgress(false) {
  struct sockaddr_in addr;

#if defined(__linux__)
  if ((pollFd = epoll_create(256)) < 0) {
    perror("epoll_create");
    exit(1);
  }
#elif defined(__APPLE__)
  kQueue = kqueue();
#endif  

  /**
   * The following logic creates a UDP socket that is used to
   * interrupt epoll_wait so that it can reset its timeout
   * value
   */
  if ((mInterruptSd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    LOG_VA_ERROR("socket() failure: %s", strerror(errno));
    exit(1);
  }

  // Set to non-blocking
  FileUtils::SetFlags(mInterruptSd, O_NONBLOCK);

  // create address structure to bind to - any available port - any address
  memset(&addr, 0 , sizeof(sockaddr_in));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = inet_addr("127.0.0.1");
  addr.sin_port = 0;

  // bind socket
  if ((bind(mInterruptSd, (const sockaddr *)&addr, sizeof(sockaddr_in))) < 0) {
    LOG_VA_ERROR("bind() failure: %s", strerror(errno));
    exit(1);
  }

#if defined(__linux__)
  struct epoll_event event;
  memset(&event, 0, sizeof(struct epoll_event));
  event.events = EPOLLERR | EPOLLHUP;
  if (epoll_ctl(pollFd, EPOLL_CTL_ADD, mInterruptSd, &event) < 0) {
    LOG_VA_ERROR("epoll_ctl(%d, EPOLL_CTL_ADD, %d, EPOLLERR|EPOLLHUP) failed : %s",
		 pollFd, mInterruptSd, strerror(errno));
    exit(1);
  }
#endif

}


void Reactor::HandleTimeouts(PollTimeout &nextTimeout) {
  boost::mutex::scoped_lock lock(mMutex);

  struct timeval tval;

  if (gettimeofday(&tval, 0) < 0) {
    LOG_VA_ERROR("gettimeofday() failed : %s", strerror(errno));
    return;
  }
  
  IOHandler       *handler;
  DispatchHandler *dh;

  while ((dh = mRequestCache.GetNextTimeout(tval.tv_sec, handler)) != 0) {
    handler->DeliverEvent( new Event(Event::ERROR, 0, ((IOHandlerData *)handler)->GetAddress(), Error::COMM_REQUEST_TIMEOUT), dh );
  }

  nextTimeout.SetIndefinite();

  if (!mTimerHeap.empty()) {
    struct TimerT timer;
    boost::xtime now;
    EventPtr eventPtr;

    boost::xtime_get(&now, boost::TIME_UTC);

    while (!mTimerHeap.empty()) {
      timer = mTimerHeap.top();
      if (xtime_cmp(timer.expireTime, now) > 0) {
	nextTimeout.Set(now, timer.expireTime);
	break;
      }
      eventPtr.reset( new Event(Event::TIMER, Error::OK) );
      timer.handler->handle(eventPtr);
      mTimerHeap.pop();
    }

    PollLoopContinue();
  }
}



/**
 * 
 */
void Reactor::PollLoopInterrupt() {

  mInterruptInProgress = true;

#if defined(__linux__)
  struct epoll_event event;

  memset(&event, 0, sizeof(struct epoll_event));
  event.events = EPOLLOUT | EPOLLERR | EPOLLHUP;

  if (epoll_ctl(pollFd, EPOLL_CTL_MOD, mInterruptSd, &event) < 0) {
    /**
    LOG_VA_ERROR("epoll_ctl(%d, EPOLL_CTL_MOD, sd=%d) : %s", 
                 pollFd, mInterruptSd, strerror(errno));
    DUMP_CORE;
    **/
  }

#elif defined(__APPLE__)
  struct kevent event;

  EV_SET(&event, mInterruptSd, EVFILT_WRITE, EV_ADD | EV_ENABLE, 0, 0, this);

  if (kevent(kQueue, &event, 1, 0, 0, 0) == -1) {
    LOG_VA_ERROR("kevent(sd=%d) : %s", mInterruptSd, strerror(errno));
    exit(1);
  }
}
#endif
}



/**
 * 
 */
void Reactor::PollLoopContinue() {

  if (!mInterruptInProgress)
    return;

#if defined(__linux__)
  struct epoll_event event;

  memset(&event, 0, sizeof(struct epoll_event));
  event.events = EPOLLERR | EPOLLHUP;

  if (epoll_ctl(pollFd, EPOLL_CTL_MOD, mInterruptSd, &event) < 0) {
    LOG_VA_ERROR("epoll_ctl(EPOLL_CTL_MOD, sd=%d) : %s", mInterruptSd, strerror(errno));
    exit(1);
  }
#elif defined(__APPLE__)
  struct kevent devent;

  EV_SET(&devent, mInterruptSd, EVFILT_WRITE, EV_DELETE, 0, 0, 0);

  if (kevent(kQueue, &devent, 1, 0, 0, 0) == -1 && errno != ENOENT) {
    LOG_VA_ERROR("kevent(sd=%d) : %s", mInterruptSd, strerror(errno));
    exit(1);
  }
#endif
  mInterruptInProgress = false;
}

