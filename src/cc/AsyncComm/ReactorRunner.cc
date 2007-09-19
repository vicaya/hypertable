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

extern "C" {
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/time.h>
#if defined(__APPLE__)
#include <sys/event.h>
#endif
}

#define DISABLE_LOG_DEBUG 1

#include "Common/Logger.h"

#include "HandlerMap.h"
#include "IOHandler.h"
#include "IOHandlerData.h"
#include "ReactorFactory.h"
#include "ReactorRunner.h"
using namespace hypertable;


/**
 * 
 */
void ReactorRunner::operator()() {
  int n;
  IOHandler *handler;
  set<void *> removedHandlers;
  PollTimeout timeout;

#if defined(__linux__)
  struct epoll_event events[256];

  while ((n = epoll_wait(mReactor->pollFd, events, 256, timeout.GetMillis())) >= 0 || errno == EINTR) {
    removedHandlers.clear();
    LOG_VA_DEBUG("epoll_wait returned %d events", n);
    for (int i=0; i<n; i++) {
      if (removedHandlers.count(events[i].data.ptr) == 0) {
	handler = (IOHandler *)events[i].data.ptr;
	if (handler && handler->HandleEvent(&events[i])) {
	  UnregisterHandler(handler);
	  removedHandlers.insert(handler);
	}
      }
    }
    if (!removedHandlers.empty()) {
      for (set<void *>::iterator iter=removedHandlers.begin(); iter!=removedHandlers.end(); iter++) {
	mReactor->CancelRequests((IOHandler*)(*iter));
      }
    }
    mReactor->HandleTimeouts(timeout);
  }

  LOG_VA_ERROR("epoll_wait(%d) failed : %s", mReactor->pollFd, strerror(errno));

#elif defined(__APPLE__)
  struct kevent events[32];

  while ((n = kevent(mReactor->kQueue, NULL, 0, events, 32, timeout.GetTimespec())) >= 0 || errno == EINTR) {
    removedHandlers.clear();
    for (int i=0; i<n; i++) {
      handler = (IOHandler *)events[i].udata;
      if (removedHandlers.count(handler) == 0) {
	if (handler && handler->HandleEvent(&events[i])) {
	  UnregisterHandler(handler);
	  removedHandlers.insert(handler);
	}
      }
    }
    if (!removedHandlers.empty()) {
      for (set<void *>::iterator iter=removedHandlers.begin(); iter!=removedHandlers.end(); iter++) {
	mReactor->CancelRequests((IOHandler*)(*iter));
      }
    }
    mReactor->HandleTimeouts(timeout);
  }

  LOG_VA_ERROR("kevent(%d) failed : %s", mReactor->kQueue, strerror(errno));

#endif
}


/**
 *
 */
void ReactorRunner::UnregisterHandler(IOHandler *handler) {
#if defined(__linux__)
  struct epoll_event event;
  memset(&event, 0, sizeof(struct epoll_event));
  if (epoll_ctl(mReactor->pollFd, EPOLL_CTL_DEL, handler->GetSd(), &event) < 0) {
    LOG_VA_ERROR("epoll_ctl(EPOLL_CTL_DEL, %d) failure, %s", handler->GetSd(), strerror(errno));
    exit(1);
  }
#elif defined(__APPLE__)
  struct kevent devents[2];
  EV_SET(&devents[0], handler->GetSd(), EVFILT_READ, EV_DELETE, 0, 0, 0);
  EV_SET(&devents[1], handler->GetSd(), EVFILT_WRITE, EV_DELETE, 0, 0, 0);
  if (kevent(mReactor->kQueue, devents, 2, NULL, 0, NULL) == -1 && errno != ENOENT)
    LOG_VA_ERROR("kevent(%d) : %s", handler->GetSd(), strerror(errno));
#else
  ImplementMe;
#endif
  close(handler->GetSd());
  struct sockaddr_in addr = handler->GetAddress();
  HandlerMap &handlerMap = handler->GetHandlerMap();
  handlerMap.RemoveHandler(addr);
}
