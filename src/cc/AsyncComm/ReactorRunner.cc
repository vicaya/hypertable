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
  set<IOHandler *> removedHandlers;
  PollTimeout timeout;

#if defined(__linux__)
  struct epoll_event events[256];

  while ((n = epoll_wait(m_reactor->pollFd, events, 256, timeout.get_millis())) >= 0 || errno == EINTR) {
    m_reactor->get_removed_handlers(removedHandlers);
    LOG_VA_DEBUG("epoll_wait returned %d events", n);
    for (int i=0; i<n; i++) {
      if (removedHandlers.count((IOHandler *)events[i].data.ptr) == 0) {
	handler = (IOHandler *)events[i].data.ptr;
	if (handler && handler->handle_event(&events[i])) {
	  HandlerMap &handlerMap = handler->get_handler_map();
	  handlerMap.decomission_handler(handler->get_address());
	  removedHandlers.insert(handler);
	}
      }
    }
    if (!removedHandlers.empty())
      cleanup_and_remove_handlers(removedHandlers);
    m_reactor->handle_timeouts(timeout);
  }

  LOG_VA_ERROR("epoll_wait(%d) failed : %s", m_reactor->pollFd, strerror(errno));

#elif defined(__APPLE__)
  struct kevent events[32];

  while ((n = kevent(m_reactor->kQueue, NULL, 0, events, 32, timeout.get_timespec())) >= 0 || errno == EINTR) {
    m_reactor->get_removed_handlers(removedHandlers);
    for (int i=0; i<n; i++) {
      handler = (IOHandler *)events[i].udata;
      if (removedHandlers.count(handler) == 0) {
	if (handler && handler->handle_event(&events[i])) {
	  HandlerMap &handlerMap = handler->get_handler_map();
	  handlerMap.decomission_handler(handler->get_address());
	  removedHandlers.insert(handler);
	}
      }
    }
    if (!removedHandlers.empty())
      cleanup_and_remove_handlers(removedHandlers);
    m_reactor->handle_timeouts(timeout);
  }

  LOG_VA_ERROR("kevent(%d) failed : %s", m_reactor->kQueue, strerror(errno));

#endif
}



void ReactorRunner::cleanup_and_remove_handlers(set<IOHandler *> &handlers) {
  IOHandler *handler;

  for (set<IOHandler *>::iterator iter=handlers.begin(); iter!=handlers.end(); iter++) {
    handler = *iter;

#if defined(__linux__)
    struct epoll_event event;
    memset(&event, 0, sizeof(struct epoll_event));
    if (epoll_ctl(m_reactor->pollFd, EPOLL_CTL_DEL, handler->get_sd(), &event) < 0) {
      LOG_VA_ERROR("epoll_ctl(EPOLL_CTL_DEL, %d) failure, %s", handler->get_sd(), strerror(errno));
    }
#elif defined(__APPLE__)
    struct kevent devents[2];
    EV_SET(&devents[0], handler->get_sd(), EVFILT_READ, EV_DELETE, 0, 0, 0);
    EV_SET(&devents[1], handler->get_sd(), EVFILT_WRITE, EV_DELETE, 0, 0, 0);
    if (kevent(m_reactor->kQueue, devents, 2, NULL, 0, NULL) == -1 && errno != ENOENT) {
      LOG_VA_ERROR("kevent(%d) : %s", handler->get_sd(), strerror(errno));
    }
#else
    ImplementMe;
#endif
    close(handler->get_sd());
    m_reactor->cancel_requests(handler);
    HandlerMap &handlerMap = handler->get_handler_map();
    handlerMap.purge_handler(handler);
  }
}
