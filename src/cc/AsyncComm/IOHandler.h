/** -*- c++ -*-
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


#ifndef HYPERTABLE_IOHANDLER_H
#define HYPERTABLE_IOHANDLER_H

extern "C" {
#include <errno.h>
#include <time.h>
#include <sys/types.h>
#include <sys/time.h>
#if defined(__APPLE__)
#include <sys/event.h>
#endif
#include <poll.h>
#if defined(__linux__)
#include <sys/epoll.h>
#if !defined(POLLRDHUP)
#define POLLRDHUP 0x2000
#endif
#endif
}

#include "Common/Logger.h"
#include "Common/ReferenceCount.h"

#include "DispatchHandler.h"
#include "ReactorFactory.h"
#include "ExpireTimer.h"

namespace Hypertable {

  /**
   *
   */
  class IOHandler : public ReferenceCount {

  public:

    IOHandler(int sd, const sockaddr_in &addr, DispatchHandlerPtr &dhp)
      : m_free_flag(0), m_addr(addr), m_sd(sd), m_dispatch_handler_ptr(dhp) {
      ReactorFactory::get_reactor(m_reactor_ptr);
      m_poll_interest = 0;
      socklen_t namelen = sizeof(m_local_addr);
      getsockname(m_sd, (sockaddr *)&m_local_addr, &namelen);
      memset(&m_alias, 0, sizeof(m_alias));
    }

#if defined(__APPLE__)
    virtual bool handle_event(struct kevent *event, clock_t arrival_clocks) = 0;
#elif defined(__linux__)
    virtual bool handle_event(struct epoll_event *event,
                              clock_t arrival_clocks) = 0;
#else
    ImplementMe;
#endif

    virtual ~IOHandler() {
      HT_EXPECT(m_free_flag != 0xdeadbeef, Error::FAILED_EXPECTATION);
      m_free_flag = 0xdeadbeef;
      return;
    }

    void deliver_event(Event *event) {
      memcpy(&event->local_addr, &m_local_addr, sizeof(m_local_addr));
      if (!m_dispatch_handler_ptr) {
        HT_INFOF("%s", event->to_str().c_str());
        delete event;
      }
      else {
        EventPtr event_ptr(event);
        m_dispatch_handler_ptr->handle(event_ptr);
      }
    }

    void deliver_event(Event *event, DispatchHandler *dh) {
      memcpy(&event->local_addr, &m_local_addr, sizeof(m_local_addr));
      if (!dh) {
        if (!m_dispatch_handler_ptr) {
          HT_INFOF("%s", event->to_str().c_str());
          delete event;
        }
        else {
          EventPtr event_ptr(event);
          m_dispatch_handler_ptr->handle(event_ptr);
        }
      }
      else {
        EventPtr event_ptr(event);
        dh->handle(event_ptr);
      }
    }

    void start_polling() {
#if defined(__APPLE__)
      add_poll_interest(Reactor::READ_READY);
#elif defined(__linux__)
      struct epoll_event event;
      memset(&event, 0, sizeof(struct epoll_event));
      event.data.ptr = this;
      if (ReactorFactory::ms_epollet) {
        event.events = EPOLLIN | EPOLLOUT | POLLRDHUP | EPOLLET;
        if (epoll_ctl(m_reactor_ptr->poll_fd, EPOLL_CTL_ADD, m_sd, &event)
            < 0) {
          HT_ERRORF("epoll_ctl(%d, EPOLL_CTL_ADD, %d, EPOLLIN|EPOLLOUT|"
                    "POLLRDHUP|EPOLLET) failed : %s", m_reactor_ptr->poll_fd,
                    m_sd, strerror(errno));
          exit(1);
        }
      }
      else {
        event.events = EPOLLIN;
        if (epoll_ctl(m_reactor_ptr->poll_fd, EPOLL_CTL_ADD, m_sd, &event)
            < 0) {
          HT_ERRORF("epoll_ctl(%d, EPOLL_CTL_ADD, %d, EPOLLIN) failed : %s",
                    m_reactor_ptr->poll_fd, m_sd, strerror(errno));
          exit(1);
        }
      }
      m_poll_interest |= Reactor::READ_READY;
#endif
    }

    void add_poll_interest(int mode);

    void remove_poll_interest(int mode);

    struct sockaddr_in &get_address() { return m_addr; }

    struct sockaddr_in &get_local_address() { return m_local_addr; }

    void get_local_address(struct sockaddr_in *addrp) {
      memcpy(addrp, &m_local_addr, sizeof(struct sockaddr_in));
    }

    void set_alias(const sockaddr_in &alias) {
      memcpy(&m_alias, &alias, sizeof(m_alias));
    }

    void get_alias(sockaddr_in *aliasp) {
      memcpy(aliasp, &m_alias, sizeof(m_alias));
    }

    int get_sd() { return m_sd; }

    void get_reactor(ReactorPtr &reactor_ptr) { reactor_ptr = m_reactor_ptr; }

    void shutdown() {
      ExpireTimer timer;
      m_reactor_ptr->schedule_removal(this);
      boost::xtime_get(&timer.expire_time, boost::TIME_UTC);
      timer.expire_time.nsec += 200000000LL;
      timer.handler = 0;
      m_reactor_ptr->add_timer(timer);
    }

  protected:

    void stop_polling() {
#if defined(__APPLE__)
      remove_poll_interest(Reactor::READ_READY|Reactor::WRITE_READY);
#elif defined(__linux__)
      struct epoll_event event;  // this is necessary for < Linux 2.6.9
      if (epoll_ctl(m_reactor_ptr->poll_fd, EPOLL_CTL_DEL, m_sd, &event) < 0) {
        HT_ERRORF("epoll_ctl(%d, EPOLL_CTL_DEL, %d) failed : %s",
                     m_reactor_ptr->poll_fd, m_sd, strerror(errno));
        exit(1);
      }
      m_poll_interest = 0;
#endif
    }

    uint32_t            m_free_flag;
    InetAddr            m_addr;
    InetAddr            m_local_addr;
    InetAddr            m_alias;
    int                 m_sd;
    DispatchHandlerPtr  m_dispatch_handler_ptr;
    ReactorPtr          m_reactor_ptr;
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
