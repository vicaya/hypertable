/** -*- c++ -*-
 * Copyright (C) 2009 Sanjit Jhala (Zvents, Inc.)
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

#ifndef HYPERSPACE_REQUESTHANDLERRENEWSESSION_H
#define HYPERSPACE_REQUESTHANDLERRENEWSESSION_H

#include "Common/Runnable.h"
#include "Common/Mutex.h"

#include "AsyncComm/ApplicationHandler.h"
#include "AsyncComm/Comm.h"

namespace Hyperspace {
  using namespace Hypertable;
  class Master;
  
  class RequestHandlerRenewSession : public ApplicationHandler {
  public:
    RequestHandlerRenewSession(Comm *comm, Master *master, 
        uint64_t session_id, uint64_t last_known_event, bool shutdown,
        EventPtr &event, struct sockaddr_in *send_addr)
      : m_comm(comm), m_master(master), m_session_id(session_id), 
        m_last_known_event(last_known_event), m_shutdown(shutdown), 
        m_event(event), m_send_addr(send_addr)  { }
    virtual ~RequestHandlerRenewSession() { }

    virtual void run();

  private:
    Comm        *m_comm;
    Master      *m_master;
    uint64_t     m_session_id;
    uint64_t     m_last_known_event;
    bool         m_shutdown;
    EventPtr     m_event; 
    struct sockaddr_in *m_send_addr;
  };
}

#endif // HYPERSPACE_REQUESTHANDLERRENEWSESSION_H
