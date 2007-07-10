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

#include "IOHandler.h"
#include "ReactorFactory.h"
#include "ReactorRunner.h"
using namespace hypertable;

#define EVENT_LOOP_TIMEOUT_S   5
#define EVENT_LOOP_TIMEOUT_MS  5000


/**
 * 
 */
void ReactorRunner::operator()() {
  int n;
  IOHandler *handler;
  set<void *> removedHandlers;

#if defined(__linux__)
  struct epoll_event events[256];

  while ((n = epoll_wait(mReactor->pollFd, events, 256, EVENT_LOOP_TIMEOUT_MS)) >= 0 || errno == EINTR) {
    removedHandlers.clear();
    LOG_VA_DEBUG("epoll_wait returned %d events", n);
    for (int i=0; i<n; i++) {
      if (removedHandlers.count(events[i].data.ptr) == 0) {
	handler = (IOHandler *)events[i].data.ptr;
	if (handler->HandleEvent(&events[i])) {
	  int sd = ((IOHandler *)handler)->GetSd();
	  struct epoll_event event;
	  memset(&event, 0, sizeof(struct epoll_event));
	  if (epoll_ctl(mReactor->pollFd, EPOLL_CTL_DEL, sd, &event) < 0) {
	    LOG_VA_ERROR("epoll_ctl(EPOLL_CTL_DEL, %d) failure, %s", sd, strerror(errno));
	    exit(1);
	  }
	  close(sd);
	  removedHandlers.insert(handler);
	}
      }
    }
    if (!removedHandlers.empty()) {
      for (set<void *>::iterator iter=removedHandlers.begin(); iter!=removedHandlers.end(); iter++) {
	mReactor->CancelRequests((IOHandler*)(*iter));
	//delete (IOHandler *)(*iter);
      }
    }
    mReactor->HandleTimeouts();
  }

  LOG_VA_ERROR("epoll_wait(%d) failed : %s", mReactor->pollFd, strerror(errno));

#elif defined(__APPLE__)
  struct kevent events[32];
  struct timespec maxWait;

  maxWait.tv_sec = EVENT_LOOP_TIMEOUT_S;
  maxWait.tv_nsec = 0;

  while ((n = kevent(mReactor->kQueue, NULL, 0, events, 32, &maxWait)) >= 0 || errno == EINTR) {
    removedHandlers.clear();
    for (int i=0; i<n; i++) {
      handler = (IOHandler *)events[i].udata;
      if (removedHandlers.count(handler) == 0) {
	if (handler->HandleEvent(&events[i])) {
	  struct kevent devents[2];
	  EV_SET(&devents[0], events[i].ident, EVFILT_READ, EV_DELETE, 0, 0, 0);
	  EV_SET(&devents[1], events[i].ident, EVFILT_WRITE, EV_DELETE, 0, 0, 0);
	  if (kevent(mReactor->kQueue, devents, 2, NULL, 0, NULL) == -1 && errno != ENOENT)
	    LOG_VA_ERROR("kevent(%d) : %s", events[i].ident, strerror(errno));
	  close(events[i].ident);
	  removedHandlers.insert(handler);
	}
      }
    }
    if (!removedHandlers.empty()) {
      for (set<void *>::iterator iter=removedHandlers.begin(); iter!=removedHandlers.end(); iter++) {
	mReactor->CancelRequests((IOHandler*)(*iter));
	//delete (IOHandler *)(*iter);
      }
    }
    mReactor->HandleTimeouts();
  }

  LOG_VA_ERROR("kevent(%d) failed : %s", mReactor->kQueue, strerror(errno));

#endif
}
