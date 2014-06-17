/** -*- C++ -*-
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

#ifndef HYPERTABLE_IOHANDLERDATA_H
#define HYPERTABLE_IOHANDLERDATA_H

#include <list>

extern "C" {
#include <netdb.h>
#include <string.h>
#include <time.h>
}

#include "Common/Error.h"
#include "Common/atomic.h"

#include "CommBuf.h"
#include "IOHandler.h"

namespace Hypertable {

  /**
   */
  class IOHandlerData : public IOHandler {

  public:

    IOHandlerData(int sd, const InetAddr &addr, DispatchHandlerPtr &dhp, bool connected=false)
      : IOHandler(sd, addr, dhp), m_send_queue() {
      m_connected = connected;
      reset_incoming_message_state();
    }

    void reset_incoming_message_state() {
      m_got_header = false;
      m_event = new Event(Event::MESSAGE, m_addr);
      m_message_header_ptr = m_message_header;
      m_message_header_remaining = m_event->header.fixed_length();
      m_message = 0;
      m_message_ptr = 0;
      m_message_remaining = 0;
    }

    int send_message(CommBufPtr &, uint32_t timeout_ms = 0,
                     DispatchHandler * = 0);

    int flush_send_queue();

    // define default poll() interface for everyone since it is chosen at runtime
    virtual bool handle_event(struct pollfd *event, clock_t arrival_clocks,
			      time_t arival_time=0);

#if defined(__APPLE__) || defined(__FreeBSD__)
    virtual bool handle_event(struct kevent *event, clock_t arrival_clocks,
			      time_t arival_time=0);
#elif defined(__linux__)
    virtual bool handle_event(struct epoll_event *event, clock_t arrival_clocks,
			      time_t arival_time=0);
#elif defined(__sun__)
    virtual bool handle_event(port_event_t *event, clock_t arrival_clocks,
			      time_t arival_time=0);
#else
    ImplementMe;
#endif

    bool handle_write_readiness();

  private:
    void handle_message_header(clock_t arrival_clocks, time_t arrival_time);
    void handle_message_body();
    void handle_disconnect(int error = Error::OK);

    bool                m_connected;
    Mutex               m_mutex;
    Event              *m_event;
    uint8_t             m_message_header[64];
    uint8_t            *m_message_header_ptr;
    size_t              m_message_header_remaining;
    bool                m_got_header;
    uint8_t            *m_message;
    uint8_t            *m_message_ptr;
    size_t              m_message_remaining;
    std::list<CommBufPtr> m_send_queue;
  };

  typedef intrusive_ptr<IOHandlerData> IOHandlerDataPtr;
}

#endif // HYPERTABLE_IOHANDLERDATA_H
