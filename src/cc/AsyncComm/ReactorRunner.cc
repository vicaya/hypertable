/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
#include "Common/Config.h"
#include "Common/Time.h"

extern "C" {
#include <errno.h>
#include <poll.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/time.h>
#if defined(__APPLE__)
#include <sys/event.h>
#endif
}

#define HT_DISABLE_LOG_DEBUG 1

#include "Common/Logger.h"

#include "HandlerMap.h"
#include "IOHandler.h"
#include "IOHandlerData.h"
#include "ReactorFactory.h"
#include "ReactorRunner.h"
using namespace Hypertable;
using namespace Hypertable::Config;

bool Hypertable::ReactorRunner::ms_shutdown = false;
bool Hypertable::ReactorRunner::ms_record_arrival_clocks = false;
HandlerMapPtr Hypertable::ReactorRunner::ms_handler_map_ptr;



/**
 *
 */
void ReactorRunner::operator()() {
  int n;
  IOHandler *handler;
  std::set<IOHandler *> removed_handlers;
  PollTimeout timeout;
  bool did_delay = false;
  clock_t arrival_clocks = 0;
  bool got_clocks = false;

  HT_EXPECT(properties, Error::FAILED_EXPECTATION);

  uint32_t dispatch_delay = (uint32_t)properties->get_i32("Comm.DispatchDelay");

#if defined(__linux__)
  struct epoll_event events[256];

  while ((n = epoll_wait(m_reactor_ptr->poll_fd, events, 256,
          timeout.get_millis())) >= 0 || errno == EINTR) {

    if (ms_record_arrival_clocks)
      got_clocks = false;

    if (dispatch_delay)
      did_delay = false;

    m_reactor_ptr->get_removed_handlers(removed_handlers);
    if (!ms_shutdown)
      HT_DEBUGF("epoll_wait returned %d events", n);
    for (int i=0; i<n; i++) {
      if (removed_handlers.count((IOHandler *)events[i].data.ptr) == 0) {
        handler = (IOHandler *)events[i].data.ptr;
        // dispatch delay for testing
        if (dispatch_delay && !did_delay && (events[i].events & EPOLLIN)) {
          poll(0, 0, (int)dispatch_delay);
          did_delay = true;
        }
        if (ms_record_arrival_clocks && !got_clocks
            && (events[i].events & EPOLLIN)) {
          arrival_clocks = std::clock();
          got_clocks = true;
        }
        if (handler && handler->handle_event(&events[i], arrival_clocks)) {
          ms_handler_map_ptr->decomission_handler(handler->get_address());
          removed_handlers.insert(handler);
        }
      }
    }
    if (!removed_handlers.empty())
      cleanup_and_remove_handlers(removed_handlers);
    m_reactor_ptr->handle_timeouts(timeout);
    if (ms_shutdown)
      return;
  }

  if (!ms_shutdown)
    HT_ERRORF("epoll_wait(%d) failed : %s", m_reactor_ptr->poll_fd,
              strerror(errno));

#elif defined(__APPLE__)
  struct kevent events[32];

  while ((n = kevent(m_reactor_ptr->kqd, NULL, 0, events, 32,
          timeout.get_timespec())) >= 0 || errno == EINTR) {

    if (dispatch_delay)
      did_delay = false;

    m_reactor_ptr->get_removed_handlers(removed_handlers);
    for (int i=0; i<n; i++) {
      handler = (IOHandler *)events[i].udata;
      if (removed_handlers.count(handler) == 0) {
        // dispatch delay for testing
        if (dispatch_delay && !did_delay && events[i].filter == EVFILT_READ) {
          poll(0, 0, (int)dispatch_delay);
          did_delay = true;
        }
        if (handler && handler->handle_event(&events[i], arrival_clocks)) {
          ms_handler_map_ptr->decomission_handler(handler->get_address());
          removed_handlers.insert(handler);
        }
      }
    }
    if (!removed_handlers.empty())
      cleanup_and_remove_handlers(removed_handlers);
    m_reactor_ptr->handle_timeouts(timeout);
    if (ms_shutdown)
      return;
  }

  if (!ms_shutdown)
    HT_ERRORF("kevent(%d) failed : %s", m_reactor_ptr->kqd, strerror(errno));

#endif
}



void
ReactorRunner::cleanup_and_remove_handlers(std::set<IOHandler *> &handlers) {
  foreach(IOHandler *handler, handlers) {
#if defined(__linux__)
    struct epoll_event event;
    memset(&event, 0, sizeof(struct epoll_event));
    if (epoll_ctl(m_reactor_ptr->poll_fd, EPOLL_CTL_DEL, handler->get_sd(),
        &event) < 0) {
      if (!ms_shutdown)
        HT_ERRORF("epoll_ctl(EPOLL_CTL_DEL, %d) failure, %s", handler->get_sd(),
                  strerror(errno));
      continue;
    }
#elif defined(__APPLE__)
    struct kevent devents[2];
    EV_SET(&devents[0], handler->get_sd(), EVFILT_READ, EV_DELETE, 0, 0, 0);
    EV_SET(&devents[1], handler->get_sd(), EVFILT_WRITE, EV_DELETE, 0, 0, 0);
    if (kevent(m_reactor_ptr->kqd, devents, 2, NULL, 0, NULL) == -1
        && errno != ENOENT) {
      if (!ms_shutdown)
        HT_ERRORF("kevent(%d) : %s", handler->get_sd(), strerror(errno));
      continue;
    }
#else
    ImplementMe;
#endif
    close(handler->get_sd());
    ms_handler_map_ptr->purge_handler(handler);
    m_reactor_ptr->cancel_requests(handler);
  }
}
