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

#ifndef HYPERTABLE_IOHANDLERDATAGRAM_H
#define HYPERTABLE_IOHANDLERDATAGRAM_H

#include <list>
#include <utility>

extern "C" {
#include <netdb.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
}

#include "CommBuf.h"
#include "IOHandler.h"

using namespace std;

namespace Hypertable {

  /**
   */
  class IOHandlerDatagram : public IOHandler {

  public:

    IOHandlerDatagram(int sd, struct sockaddr_in &addr, DispatchHandlerPtr &dhp) : IOHandler(sd, addr, dhp), m_send_queue() {
      m_message = new uint8_t [ 65536 ];
    }

    virtual ~IOHandlerDatagram() { delete [] m_message; }

    int send_message(struct sockaddr_in &addr, CommBufPtr &cbufPtr);

    int flush_send_queue();

#if defined(__APPLE__)
    virtual bool handle_event(struct kevent *event);
#elif defined(__linux__)
    virtual bool handle_event(struct epoll_event *event);
#else
    ImplementMe;
#endif

    int handle_write_readiness();

  private:

    typedef std::pair<struct sockaddr_in, CommBufPtr> SendRecT;

    boost::mutex    m_mutex;
    uint8_t        *m_message;
    list<SendRecT>  m_send_queue;
  };

  typedef boost::intrusive_ptr<IOHandlerDatagram> IOHandlerDatagramPtr;
}

#endif // HYPERTABLE_IOHANDLERDATAGRAM_H
