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

namespace Hypertable {

  class HandlerMap;

  /**
   *
   */
  class IOHandler : public ReferenceCount {

  public:

    IOHandler(int sd, struct sockaddr_in &addr, DispatchHandlerPtr &dhp, HandlerMap &hmap) : m_addr(addr), m_sd(sd), m_dispatch_handler_ptr(dhp), m_handler_map(hmap) {
      m_reactor = ReactorFactory::get_reactor();
      m_poll_interest = 0;
      socklen_t namelen = sizeof(m_local_addr);
      getsockname(m_sd, (sockaddr *)&m_local_addr, &namelen);
    }

#if defined(__APPLE__)
    virtual bool handle_event(struct kevent *event) = 0;
#elif defined(__linux__)
    virtual bool handle_event(struct epoll_event *event) = 0;
#else
    ImplementMe;
#endif

    virtual ~IOHandler() { return; }

    void deliver_event(Event *event) {
      memcpy(&event->localAddr, &m_local_addr, sizeof(m_local_addr));
      if (!m_dispatch_handler_ptr) {
	LOG_VA_INFO("%s", event->toString().c_str());
	delete event;
      }
      else {
	EventPtr eventPtr(event);
	m_dispatch_handler_ptr->handle(eventPtr);
      }
    }

    void deliver_event(Event *event, DispatchHandler *dh) {
      memcpy(&event->localAddr, &m_local_addr, sizeof(m_local_addr));
      if (!dh) {
	if (!m_dispatch_handler_ptr) {
	  LOG_VA_INFO("%s", event->toString().c_str());
	  delete event;
	}
	else {
	  EventPtr eventPtr(event);
	  m_dispatch_handler_ptr->handle(eventPtr);
	}
      }
      else {
	EventPtr eventPtr(event);
	dh->handle(eventPtr);
      }
    }

    void start_polling() {
#if defined(__APPLE__)
      add_poll_interest(Reactor::READ_READY);
#elif defined(__linux__)
      struct epoll_event event;
      memset(&event, 0, sizeof(struct epoll_event));
      event.data.ptr = this;
      event.events = EPOLLIN | EPOLLERR | EPOLLHUP;
      if (epoll_ctl(m_reactor->pollFd, EPOLL_CTL_ADD, m_sd, &event) < 0) {
	LOG_VA_ERROR("epoll_ctl(%d, EPOLL_CTL_ADD, %d, EPOLLIN|EPOLLERR|EPOLLHUP) failed : %s",
		     m_reactor->pollFd, m_sd, strerror(errno));
	exit(1);
      }
      m_poll_interest |= Reactor::READ_READY;
#endif
    }

    void add_poll_interest(int mode);

    void remove_poll_interest(int mode);

    struct sockaddr_in &get_address() { return m_addr; }

    struct sockaddr_in &get_local_address() { return m_local_addr; }

    void get_local_address(struct sockaddr_in *addrPtr) {
      memcpy(addrPtr, &m_local_addr, sizeof(struct sockaddr_in));
    }

    int get_sd() { return m_sd; }

    Reactor *get_reactor() { return m_reactor; }

    HandlerMap &get_handler_map() { return m_handler_map; }

    void shutdown() { 
      struct TimerT timer;
      m_reactor->schedule_removal(this);
      boost::xtime_get(&timer.expireTime, boost::TIME_UTC);
      timer.expireTime.nsec += 200000000LL;
      timer.handler = 0;
      m_reactor->add_timer(timer);
    }

  protected:

    void stop_polling() {
#if defined(__APPLE__)
      remove_poll_interest(Reactor::READ_READY|Reactor::WRITE_READY);
#elif defined(__linux__)
      struct epoll_event event;  // this is necessary for < Linux 2.6.9
      if (epoll_ctl(m_reactor->pollFd, EPOLL_CTL_DEL, m_sd, &event) < 0) {
	LOG_VA_ERROR("epoll_ctl(%d, EPOLL_CTL_DEL, %d) failed : %s",
		     m_reactor->pollFd, m_sd, strerror(errno));
	exit(1);
      }
      m_poll_interest = 0;
#endif
    }

    struct sockaddr_in  m_addr;
    struct sockaddr_in  m_local_addr;
    int                 m_sd;
    DispatchHandlerPtr  m_dispatch_handler_ptr;
    HandlerMap         &m_handler_map;
    Reactor            *m_reactor;
    int                 m_poll_interest;

#if defined(__APPLE__)
    void display_event(struct kevent *event);
#elif defined(__linux__)
    void display_event(struct epoll_event *event);
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
