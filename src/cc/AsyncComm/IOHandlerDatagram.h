/** -*- C++ -*-
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

#ifndef HYPERTABLE_IOHANDLERDATAGRAM_H
#define HYPERTABLE_IOHANDLERDATAGRAM_H

#include <list>
#include <utility>

extern "C" {
#include <netdb.h>
#include <string.h>
#include <time.h>
}

#include "CommBuf.h"
#include "IOHandler.h"


namespace Hypertable {

  /**
   */
  class IOHandlerDatagram : public IOHandler {

  public:

    IOHandlerDatagram(int sd, const sockaddr_in &addr, DispatchHandlerPtr &dhp)
      : IOHandler(sd, addr, dhp), m_send_queue() {
      m_message = new uint8_t [65536];
    }

    virtual ~IOHandlerDatagram() { delete [] m_message; }

    int send_message(struct sockaddr_in &addr, CommBufPtr &cbp);

    int flush_send_queue();

#if defined(__APPLE__)
    virtual bool handle_event(struct kevent *event, clock_t arrival_clocks);
#elif defined(__linux__)
    virtual bool handle_event(struct epoll_event *event,
                              clock_t arrival_clocks);
#elif defined(__sun__)
    virtual bool handle_event(port_event_t *event, clock_t arrival_clocks);
#else
    ImplementMe;
#endif

    int handle_write_readiness();

  private:

    typedef std::pair<struct sockaddr_in, CommBufPtr> SendRec;

    Mutex           m_mutex;
    uint8_t        *m_message;
    std::list<SendRec>  m_send_queue;
  };

  typedef boost::intrusive_ptr<IOHandlerDatagram> IOHandlerDatagramPtr;
}

#endif // HYPERTABLE_IOHANDLERDATAGRAM_H
