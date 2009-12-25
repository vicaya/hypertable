/**
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERTABLE_REACTOR_H
#define HYPERTABLE_REACTOR_H

#include <queue>
#include <set>
#include <vector>

#include <boost/thread/thread.hpp>

extern "C" {
#include <poll.h>
}

#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"

#include "PollTimeout.h"
#include "RequestCache.h"
#include "ExpireTimer.h"

namespace Hypertable {

  typedef struct {
    struct pollfd pollfd;
    IOHandler *handler;
  } PollDescriptorT;

  class Reactor : public ReferenceCount {

    friend class ReactorFactory;

  public:

    static const int READ_READY;
    static const int WRITE_READY;

    Reactor();
    ~Reactor() {
      poll_loop_interrupt();
    }

    void operator()();

    void add_request(uint32_t id, IOHandler *handler, DispatchHandler *dh,
                     boost::xtime &expire) {
      ScopedLock lock(m_mutex);
      boost::xtime now;
      m_request_cache.insert(id, handler, dh, expire);
      boost::xtime_get(&now, boost::TIME_UTC);
      if (m_next_wakeup.sec == 0 || xtime_cmp(expire, m_next_wakeup) < 0)
        poll_loop_interrupt();
    }

    DispatchHandler *remove_request(uint32_t id) {
      ScopedLock lock(m_mutex);
      return m_request_cache.remove(id);
    }

    void cancel_requests(IOHandler *handler, int32_t error=Error::COMM_BROKEN_CONNECTION) {
      ScopedLock lock(m_mutex);
      m_request_cache.purge_requests(handler, error);
    }

    void add_timer(ExpireTimer &timer) {
      ScopedLock lock(m_mutex);
      m_timer_heap.push(timer);
      poll_loop_interrupt();
    }

    void schedule_removal(IOHandler *handler) {
      ScopedLock lock(m_mutex);
      m_removed_handlers.insert(handler);
    }

    void get_removed_handlers(std::set<IOHandler *> &dst) {
      ScopedLock lock(m_mutex);
      dst = m_removed_handlers;
      m_removed_handlers.clear();
    }

    void handle_timeouts(PollTimeout &next_timeout);

#if defined(__linux__) || defined (__sun__)
    int poll_fd;
#elif defined (__APPLE__)
    int kqd;
#endif

    void add_poll_interest(int sd, short events, IOHandler *handler);
    void remove_poll_interest(int sd);
    void modify_poll_interest(int sd, short events);
    void fetch_poll_array(std::vector<struct pollfd> &fdarray,
			  std::vector<IOHandler *> &handlers);

    Mutex m_poll_array_mutex;
    std::vector<PollDescriptorT> polldata;

    void poll_loop_interrupt();
    void poll_loop_continue();

    int interrupt_sd() { return m_interrupt_sd; }

  protected:
    typedef std::priority_queue<ExpireTimer, std::vector<ExpireTimer>, LtTimer>
            TimerHeap;

    Mutex           m_mutex;
    RequestCache    m_request_cache;
    TimerHeap       m_timer_heap;
    int             m_interrupt_sd;
    bool            m_interrupt_in_progress;
    boost::xtime    m_next_wakeup;
    std::set<IOHandler *> m_removed_handlers;
  };

  typedef intrusive_ptr<Reactor> ReactorPtr;

} // namespace Hypertable

#endif // HYPERTABLE_REACTOR_H
