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

#include "Common/Error.h"
#include "Common/Exception.h"
#include "Common/InetAddr.h"

#include "ClientConnectionHandler.h"
#include "Master.h"
#include "Protocol.h"
#include "Session.h"

using namespace Hyperspace;
using namespace Hypertable;


/**
 *
 */
ClientConnectionHandler::ClientConnectionHandler(Comm *comm, Session *session, time_t timeout) : m_comm(comm), m_session(session), m_session_id(0), m_state(DISCONNECTED), m_verbose(false), m_timeout(timeout) {
  memset(&m_master_addr, 0, sizeof(struct sockaddr_in));
  return;
}


/**
 *
 */
ClientConnectionHandler::~ClientConnectionHandler() {
  if (m_master_addr.sin_port != 0)
    m_comm->close_socket(m_master_addr);
}


void ClientConnectionHandler::handle(Hypertable::EventPtr &eventPtr) {
  boost::mutex::scoped_lock lock(m_mutex);
  int error;

  if (m_verbose) {
    HT_INFOF("%s", eventPtr->toString().c_str());
  }

  if (eventPtr->type == Hypertable::Event::MESSAGE) {

    if (Protocol::response_code(eventPtr.get()) != Error::OK) {
      HT_ERRORF("Connection handshake error: %s", Protocol::string_format_message(eventPtr.get()).c_str());
      m_comm->close_socket(eventPtr->addr);
      m_state = DISCONNECTED;
      return;
    }

    m_session->state_transition(Session::STATE_SAFE);

    m_state = CONNECTED;
  }
  else if (eventPtr->type == Hypertable::Event::DISCONNECT) {

    if (m_verbose) {
      HT_WARNF("%s", eventPtr->toString().c_str());
    }

    m_session->state_transition(Session::STATE_JEOPARDY);

    m_state = DISCONNECTED;
  }
  else if (eventPtr->type == Hypertable::Event::CONNECTION_ESTABLISHED) {

    m_state = HANDSHAKING;

    memcpy(&m_master_addr, &eventPtr->addr, sizeof(struct sockaddr_in));

    CommBufPtr commBufPtr( Hyperspace::Protocol::create_handshake_request(m_session_id) );

    if ((error = m_comm->send_request(eventPtr->addr, m_timeout, commBufPtr, this)) != Error::OK) {
      HT_ERRORF("Problem sending handshake request - %s", Error::get_text(error));
      m_comm->close_socket(eventPtr->addr);
      m_state = DISCONNECTED;
    }
  }
  else {
    HT_INFOF("%s", eventPtr->toString().c_str());
    m_comm->close_socket(eventPtr->addr);
    m_state = DISCONNECTED;
  }

}
