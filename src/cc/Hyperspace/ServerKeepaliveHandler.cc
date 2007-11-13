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
#include "Common/StringExt.h"
#include "Common/System.h"

#include "ServerKeepaliveHandler.h"
#include "Master.h"
#include "Protocol.h"
#include "SessionData.h"

using namespace Hypertable;
using namespace Hyperspace;


/**
 *
 */
ServerKeepaliveHandler::ServerKeepaliveHandler(Comm *comm, Master *master) : m_comm(comm), m_master(master) { 
  int error; 

  m_master->get_datagram_send_address(&m_send_addr);

  if ((error = m_comm->set_timer(1000, this)) != Error::OK) {
    LOG_VA_ERROR("Problem setting timer - %s", Error::get_text(error));
    exit(1);
  }
  
  return; 
}



/**
 *
 */
void ServerKeepaliveHandler::handle(Hypertable::EventPtr &eventPtr) {
  uint16_t command = (uint16_t)-1;
  int error;

  if (eventPtr->type == Hypertable::Event::MESSAGE) {
    uint8_t *msgPtr = eventPtr->message;
    size_t remaining = eventPtr->messageLen;

    try {

      if (!Serialization::decode_short(&msgPtr, &remaining, &command)) {
	std::string message = "Truncated Request";
	throw new ProtocolException(message);
      }

      // sanity check command code
      if (command >= Protocol::COMMAND_MAX) {
	std::string message = (std::string)"Invalid command (" + command + ")";
	throw ProtocolException(message);
      }

      switch (command) {
      case Protocol::COMMAND_KEEPALIVE:
	{
	  uint64_t sessionId;
	  SessionDataPtr sessionPtr;
	  uint64_t lastKnownEvent;

	  if (!Serialization::decode_long(&msgPtr, &remaining, &sessionId) ||
	      !Serialization::decode_long(&msgPtr, &remaining, &lastKnownEvent)) {
	    std::string message = "Truncated Request";
	    throw new ProtocolException(message);
	  }

	  if (sessionId == 0) {
	    sessionId = m_master->create_session(eventPtr->addr);
	    LOG_VA_INFO("Session handle %lld created", sessionId);
	    error = Error::OK;
	  }
	  else
	    error = m_master->renew_session_lease(sessionId);

	  if (error == Error::HYPERSPACE_EXPIRED_SESSION) {
	    LOG_VA_INFO("Session handle %lld expired", sessionId);
	    CommBufPtr cbufPtr( Protocol::create_server_keepalive_request(sessionId, Error::HYPERSPACE_EXPIRED_SESSION) );
	    if ((error = m_comm->send_datagram(eventPtr->addr, m_send_addr, cbufPtr)) != Error::OK) {
	      LOG_VA_ERROR("Comm::send_datagram returned %s", Error::get_text(error));
	    }
	    return;
	  }

	  if (!m_master->get_session(sessionId, sessionPtr)) {
	    LOG_VA_ERROR("Unable to find data for session %lld", sessionId);
	    return;
	  }

	  sessionPtr->purge_notifications(lastKnownEvent);

	  /**
	  {
	    std::string str;
	    LOG_VA_INFO("Sending Keepalive request to %s (lastKnownEvent=%lld)", InetAddr::string_format(str, eventPtr->addr), lastKnownEvent);
	  }
	  **/

	  CommBufPtr cbufPtr( Protocol::create_server_keepalive_request(sessionPtr) );
	  if ((error = m_comm->send_datagram(eventPtr->addr, m_send_addr, cbufPtr)) != Error::OK) {
	    LOG_VA_ERROR("Comm::send_datagram returned %s", Error::get_text(error));
	  }
	}
	break;
      default:
	std::string message = (string)"Command code " + command + " not implemented";
	throw ProtocolException(message);
      }
    }
    catch (ProtocolException &e) {
      std::string errMsg = e.what();
      LOG_VA_ERROR("Protocol error '%s'", e.what());
    }
  }
  else if (eventPtr->type == Hypertable::Event::TIMER) {

    m_master->remove_expired_sessions();

    if ((error = m_comm->set_timer(1000, this)) != Error::OK) {
      LOG_VA_ERROR("Problem setting timer - %s", Error::get_text(error));
      exit(1);
    }

  }
  else {
    LOG_VA_INFO("%s", eventPtr->toString().c_str());
  }
}


/**
 *
 */
void ServerKeepaliveHandler::deliver_event_notifications(uint64_t sessionId) {
  int error = 0;
  SessionDataPtr sessionPtr;

  //LOG_VA_INFO("Delivering event notifications for session %lld", sessionId);

  if (!m_master->get_session(sessionId, sessionPtr)) {
    LOG_VA_ERROR("Unable to find data for session %lld", sessionId);
    return;
  }

  /**
  {
    std::string str;
    LOG_VA_INFO("Sending Keepalive request to %s", InetAddr::string_format(str, sessionPtr->addr));
  }
  **/

  CommBufPtr cbufPtr( Protocol::create_server_keepalive_request(sessionPtr) );
  if ((error = m_comm->send_datagram(sessionPtr->addr, m_send_addr, cbufPtr)) != Error::OK) {
    LOG_VA_ERROR("Comm::send_datagram returned %s", Error::get_text(error));
  }

}
