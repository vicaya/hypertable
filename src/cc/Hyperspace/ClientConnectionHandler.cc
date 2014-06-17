/**
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

#include "Common/Compat.h"
#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/System.h"

#include "ClientConnectionHandler.h"
#include "Master.h"
#include "Protocol.h"
#include "Session.h"

using namespace Hyperspace;
using namespace Hypertable;


ClientConnectionHandler::ClientConnectionHandler(Comm *comm, Session *session,
                                                 uint32_t timeout_ms)
  : m_comm(comm), m_session(session), m_session_id(0), m_state(DISCONNECTED),
    m_verbose(false), m_timeout_ms(timeout_ms) {
  memset(&m_master_addr, 0, sizeof(struct sockaddr_in));
  return;
}


ClientConnectionHandler::~ClientConnectionHandler() {
  /** was causing deadlock ...
  if (m_master_addr.sin_port != 0)
    m_comm->close_socket(m_master_addr);
  */
}


void ClientConnectionHandler::handle(Hypertable::EventPtr &event_ptr) {
  ScopedLock lock(m_mutex);
  int error;

  HT_DEBUGF("%s", event_ptr->to_str().c_str());

  if (event_ptr->type == Hypertable::Event::MESSAGE) {

    if (Protocol::response_code(event_ptr.get()) != Error::OK) {
      HT_ERRORF("Connection handshake error: %s",
                Protocol::string_format_message(event_ptr.get()).c_str());
      m_comm->close_socket(event_ptr->addr);
      m_state = DISCONNECTED;
      return;
    }

    m_session->state_transition(Session::STATE_SAFE);

    m_state = CONNECTED;
  }
  else if (event_ptr->type == Hypertable::Event::DISCONNECT) {

    if (m_verbose) {
      HT_WARNF("%s", event_ptr->to_str().c_str());
    }

    m_session->state_transition(Session::STATE_JEOPARDY);

    m_state = DISCONNECTED;
  }
  else if (event_ptr->type == Hypertable::Event::CONNECTION_ESTABLISHED) {

    m_state = HANDSHAKING;

    memcpy(&m_master_addr, &event_ptr->addr, sizeof(struct sockaddr_in));

    CommBufPtr
        cbp(Hyperspace::Protocol::create_handshake_request(m_session_id, System::exe_name));

    if ((error = m_comm->send_request(event_ptr->addr, m_timeout_ms, cbp, this))
        != Error::OK) {
      HT_ERRORF("Problem sending handshake request - %s",
                Error::get_text(error));
      m_comm->close_socket(event_ptr->addr);
      m_state = DISCONNECTED;
    }
  }
  else {
    HT_DEBUGF("%s", event_ptr->to_str().c_str());
    m_comm->close_socket(event_ptr->addr);
    m_state = DISCONNECTED;
  }

}
