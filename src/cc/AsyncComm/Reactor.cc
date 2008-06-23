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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"

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
using namespace Hypertable;

const int Reactor::READ_READY   = 0x01;
const int Reactor::WRITE_READY  = 0x02;


/**
 *
 */
Reactor::Reactor() : m_mutex(), m_interrupt_in_progress(false) {
  struct sockaddr_in addr;

#if defined(__linux__)
  if ((poll_fd = epoll_create(256)) < 0) {
    perror("epoll_create");
    exit(1);
  }
#elif defined(__APPLE__)
  kqd = kqueue();
#endif

  /**
   * The following logic creates a UDP socket that is used to
   * interrupt epoll_wait so that it can reset its timeout
   * value
   */
  if ((m_interrupt_sd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    HT_ERRORF("socket() failure: %s", strerror(errno));
    exit(1);
  }

  // Set to non-blocking
  FileUtils::set_flags(m_interrupt_sd, O_NONBLOCK);

  // create address structure to bind to - any available port - any address
  memset(&addr, 0 , sizeof(sockaddr_in));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = inet_addr("127.0.0.1");
  addr.sin_port = 0;

  // bind socket
  if ((bind(m_interrupt_sd, (const sockaddr *)&addr, sizeof(sockaddr_in))) < 0) {
    HT_ERRORF("bind() failure: %s", strerror(errno));
    exit(1);
  }

#if defined(__linux__)
  struct epoll_event event;
  memset(&event, 0, sizeof(struct epoll_event));
  event.events = EPOLLERR | EPOLLHUP;
  if (epoll_ctl(poll_fd, EPOLL_CTL_ADD, m_interrupt_sd, &event) < 0) {
    HT_ERRORF("epoll_ctl(%d, EPOLL_CTL_ADD, %d, EPOLLERR|EPOLLHUP) failed : %s",
                 poll_fd, m_interrupt_sd, strerror(errno));
    exit(1);
  }
#endif

  memset(&m_next_wakeup, 0, sizeof(m_next_wakeup));
}


void Reactor::handle_timeouts(PollTimeout &next_timeout) {
  vector<ExpireTimer> expired_timers;
  EventPtr event_ptr;
  boost::xtime     now, next_req_timeout;
  ExpireTimer timer;

  {
    boost::mutex::scoped_lock lock(m_mutex);
    IOHandler       *handler;
    DispatchHandler *dh;

    boost::xtime_get(&now, boost::TIME_UTC);

    while ((dh = m_request_cache.get_next_timeout(now, handler, &next_req_timeout)) != 0) {
      handler->deliver_event(new Event(Event::ERROR, 0, ((IOHandlerData *)handler)->get_address(), Error::COMM_REQUEST_TIMEOUT), dh);
    }

    if (next_req_timeout.sec != 0) {
      next_timeout.set(now, next_req_timeout);
      memcpy(&m_next_wakeup, &next_req_timeout, sizeof(m_next_wakeup));
    }
    else {
      next_timeout.set_indefinite();
      memset(&m_next_wakeup, 0, sizeof(m_next_wakeup));
    }

    if (!m_timer_heap.empty()) {
      ExpireTimer timer;

      while (!m_timer_heap.empty()) {
        timer = m_timer_heap.top();
        if (xtime_cmp(timer.expire_time, now) > 0) {
          if (next_req_timeout.sec == 0 || xtime_cmp(timer.expire_time, next_req_timeout) < 0) {
            next_timeout.set(now, timer.expire_time);
            memcpy(&m_next_wakeup, &timer.expire_time, sizeof(m_next_wakeup));
          }
          break;
        }
        expired_timers.push_back(timer);
        m_timer_heap.pop();
      }

    }
  }

  /**
   * Deliver timer events
   */
  for (size_t i=0; i<expired_timers.size(); i++) {
    event_ptr = new Event(Event::TIMER, Error::OK);
    if (expired_timers[i].handler)
      expired_timers[i].handler->handle(event_ptr);
  }

  {
    boost::mutex::scoped_lock lock(m_mutex);

    if (!m_timer_heap.empty()) {
      timer = m_timer_heap.top();
      if (next_req_timeout.sec == 0 || xtime_cmp(timer.expire_time, next_req_timeout) < 0) {
        next_timeout.set(now, timer.expire_time);
        memcpy(&m_next_wakeup, &timer.expire_time, sizeof(m_next_wakeup));
      }
    }

    poll_loop_continue();
  }

}



/**
 *
 */
void Reactor::poll_loop_interrupt() {

  m_interrupt_in_progress = true;

#if defined(__linux__)
  struct epoll_event event;

  memset(&event, 0, sizeof(struct epoll_event));
  event.events = EPOLLOUT | EPOLLERR | EPOLLHUP;

  if (epoll_ctl(poll_fd, EPOLL_CTL_MOD, m_interrupt_sd, &event) < 0) {
    /**
    HT_ERRORF("epoll_ctl(%d, EPOLL_CTL_MOD, sd=%d) : %s",
                 poll_fd, m_interrupt_sd, strerror(errno));
    DUMP_CORE;
    **/
  }

#elif defined(__APPLE__)
  struct kevent event;

  EV_SET(&event, m_interrupt_sd, EVFILT_WRITE, EV_ADD | EV_ENABLE, 0, 0, 0);

  if (kevent(kqd, &event, 1, 0, 0, 0) == -1) {
    HT_ERRORF("kevent(sd=%d) : %s", m_interrupt_sd, strerror(errno));
    exit(1);
  }
#endif

}



/**
 *
 */
void Reactor::poll_loop_continue() {

  if (!m_interrupt_in_progress)
    return;

#if defined(__linux__)
  struct epoll_event event;

  memset(&event, 0, sizeof(struct epoll_event));
  event.events = EPOLLERR | EPOLLHUP;

  if (epoll_ctl(poll_fd, EPOLL_CTL_MOD, m_interrupt_sd, &event) < 0) {
    HT_ERRORF("epoll_ctl(EPOLL_CTL_MOD, sd=%d) : %s", m_interrupt_sd, strerror(errno));
    exit(1);
  }
#elif defined(__APPLE__)
  struct kevent devent;

  EV_SET(&devent, m_interrupt_sd, EVFILT_WRITE, EV_DELETE, 0, 0, 0);

  if (kevent(kqd, &devent, 1, 0, 0, 0) == -1 && errno != ENOENT) {
    HT_ERRORF("kevent(sd=%d) : %s", m_interrupt_sd, strerror(errno));
    exit(1);
  }
#endif
  m_interrupt_in_progress = false;
}

