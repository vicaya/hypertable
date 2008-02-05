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
using namespace Hypertable;

const int Reactor::READ_READY   = 0x01;
const int Reactor::WRITE_READY  = 0x02;


/**
 * 
 */
Reactor::Reactor() : m_mutex(), m_interrupt_in_progress(false) {
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
  if (epoll_ctl(pollFd, EPOLL_CTL_ADD, m_interrupt_sd, &event) < 0) {
    HT_ERRORF("epoll_ctl(%d, EPOLL_CTL_ADD, %d, EPOLLERR|EPOLLHUP) failed : %s",
		 pollFd, m_interrupt_sd, strerror(errno));
    exit(1);
  }
#endif

  memset(&m_next_wakeup, 0, sizeof(m_next_wakeup));
}


void Reactor::handle_timeouts(PollTimeout &nextTimeout) {
  vector<struct TimerT> expiredTimers;
  EventPtr eventPtr;
  boost::xtime     now, nextRequestTimeout;
  struct TimerT timer;

  {
    boost::mutex::scoped_lock lock(m_mutex);
    IOHandler       *handler;
    DispatchHandler *dh;

    boost::xtime_get(&now, boost::TIME_UTC);

    while ((dh = m_request_cache.get_next_timeout(now, handler, &nextRequestTimeout)) != 0) {
      handler->deliver_event( new Event(Event::ERROR, 0, ((IOHandlerData *)handler)->get_address(), Error::COMM_REQUEST_TIMEOUT), dh );
    }

    if (nextRequestTimeout.sec != 0) {
      nextTimeout.set(now, nextRequestTimeout);
      memcpy(&m_next_wakeup, &nextRequestTimeout, sizeof(m_next_wakeup));
    }
    else {
      nextTimeout.set_indefinite();
      memset(&m_next_wakeup, 0, sizeof(m_next_wakeup));
    }

    if (!m_timer_heap.empty()) {
      struct TimerT timer;

      while (!m_timer_heap.empty()) {
	timer = m_timer_heap.top();
	if (xtime_cmp(timer.expireTime, now) > 0) {
	  if (nextRequestTimeout.sec == 0 || xtime_cmp(timer.expireTime, nextRequestTimeout) < 0) {
	    nextTimeout.set(now, timer.expireTime);
	    memcpy(&m_next_wakeup, &timer.expireTime, sizeof(m_next_wakeup));
	  }
	  break;
	}
	expiredTimers.push_back(timer);
	m_timer_heap.pop();
      }

    }
  }

  /**
   * Deliver timer events
   */
  for (size_t i=0; i<expiredTimers.size(); i++) {
    eventPtr = new Event(Event::TIMER, Error::OK);
    if (expiredTimers[i].handler)
      expiredTimers[i].handler->handle(eventPtr);
  }

  {
    boost::mutex::scoped_lock lock(m_mutex);

    if (!m_timer_heap.empty()) {
      timer = m_timer_heap.top();
      if (nextRequestTimeout.sec == 0 || xtime_cmp(timer.expireTime, nextRequestTimeout) < 0) {
	nextTimeout.set(now, timer.expireTime);
	memcpy(&m_next_wakeup, &timer.expireTime, sizeof(m_next_wakeup));
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

  if (epoll_ctl(pollFd, EPOLL_CTL_MOD, m_interrupt_sd, &event) < 0) {
    /**
    HT_ERRORF("epoll_ctl(%d, EPOLL_CTL_MOD, sd=%d) : %s", 
                 pollFd, m_interrupt_sd, strerror(errno));
    DUMP_CORE;
    **/
  }

#elif defined(__APPLE__)
  struct kevent event;

  EV_SET(&event, m_interrupt_sd, EVFILT_WRITE, EV_ADD | EV_ENABLE, 0, 0, 0);

  if (kevent(kQueue, &event, 1, 0, 0, 0) == -1) {
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

  if (epoll_ctl(pollFd, EPOLL_CTL_MOD, m_interrupt_sd, &event) < 0) {
    HT_ERRORF("epoll_ctl(EPOLL_CTL_MOD, sd=%d) : %s", m_interrupt_sd, strerror(errno));
    exit(1);
  }
#elif defined(__APPLE__)
  struct kevent devent;

  EV_SET(&devent, m_interrupt_sd, EVFILT_WRITE, EV_DELETE, 0, 0, 0);

  if (kevent(kQueue, &devent, 1, 0, 0, 0) == -1 && errno != ENOENT) {
    HT_ERRORF("kevent(sd=%d) : %s", m_interrupt_sd, strerror(errno));
    exit(1);
  }
#endif
  m_interrupt_in_progress = false;
}

