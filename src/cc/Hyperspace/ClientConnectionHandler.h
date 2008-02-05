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

#ifndef HYPERSPACE_CLIENTCONNECTIONHANDLER_H
#define HYPERSPACE_CLIENTCONNECTIONHANDLER_H

#include <boost/thread/mutex.hpp>

#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/DispatchHandler.h"

using namespace Hypertable;

namespace Hyperspace {

  class Session;

  class ClientConnectionHandler : public DispatchHandler {
  public:

    enum { DISCONNECTED, CONNECTING, HANDSHAKING, CONNECTED };

    ClientConnectionHandler(Comm *comm, Session *session, time_t timeout);
    virtual ~ClientConnectionHandler();

    virtual void handle(Hypertable::EventPtr &eventPtr);

    void set_session_id(uint64_t id) { m_session_id = id; }

    bool disconnected() { 
      boost::mutex::scoped_lock lock(m_mutex);
      return m_state == DISCONNECTED;
    }

    int initiate_connection(struct sockaddr_in &addr) {
      boost::mutex::scoped_lock lock(m_mutex);
      DispatchHandlerPtr dhp(this);
      int error;
      m_state = CONNECTING;
      if ((error = m_comm->connect(addr, dhp)) != Error::OK) {
	std::string str;
	HT_ERRORF("Problem establishing TCP connection with Hyperspace.Master at %s - %s",
		     InetAddr::string_format(str, addr), Error::get_text(error));
	m_comm->close_socket(addr);
	m_state = DISCONNECTED;
      }
      return error;
    }

    void set_verbose_mode(bool verbose) { m_verbose = verbose; }

    void close() {
      boost::mutex::scoped_lock lock(m_mutex);
      m_comm->close_socket(m_master_addr);
      m_state = DISCONNECTED;
    }

  private:
    boost::mutex m_mutex;
    Comm *m_comm;
    Session *m_session;
    uint64_t m_session_id;
    int m_state;
    bool m_verbose;
    struct sockaddr_in m_master_addr;
    time_t m_timeout;
  };
  typedef boost::intrusive_ptr<ClientConnectionHandler> ClientConnectionHandlerPtr;
  

}

#endif // HYPERSPACE_CLIENTCONNECTIONHANDLER_H
