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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#ifndef HYPERTABLE_IOHANDLERDATA_H
#define HYPERTABLE_IOHANDLERDATA_H

#include <list>

extern "C" {
#include <netdb.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
}

#include "Common/atomic.h"

#include "CommBuf.h"
#include "IOHandler.h"
#include "RequestCache.h"

using namespace std;


namespace Hypertable {

  /**
   */
  class IOHandlerData : public IOHandler {

  public:

    IOHandlerData(int sd, struct sockaddr_in &addr, DispatchHandlerPtr &dhp) 
      : IOHandler(sd, addr, dhp), m_request_cache(), m_send_queue() {
      m_connected = false;
      reset_incoming_message_state();
      m_id = atomic_inc_return(&ms_next_connection_id);
    }

    void reset_incoming_message_state() {
      m_got_header = false;
      m_message_header_remaining = sizeof(Header::HeaderT);
      m_message = 0;
      m_message_ptr = 0;
      m_message_remaining = 0;
    }

    int send_message(CommBufPtr &cbufPtr, time_t timeout=0, DispatchHandler *dispatchHandler=0);

    int flush_send_queue();

#if defined(__APPLE__)
    virtual bool handle_event(struct kevent *event);
#elif defined(__linux__)
    virtual bool handle_event(struct epoll_event *event);
#else
    ImplementMe;
#endif

    bool handle_write_readiness();

    int connection_id() { return m_id; }

  private:

    static atomic_t ms_next_connection_id;

    bool                m_connected;
    boost::mutex        m_mutex;
    Header::HeaderT     m_message_header;
    size_t              m_message_header_remaining;
    bool                m_got_header;
    uint8_t            *m_message;
    uint8_t            *m_message_ptr;
    size_t              m_message_remaining;
    RequestCache        m_request_cache;
    list<CommBufPtr>    m_send_queue;
    int                 m_id;
  };

  typedef boost::intrusive_ptr<IOHandlerData> IOHandlerDataPtr;
}

#endif // HYPERTABLE_IOHANDLERDATA_H
