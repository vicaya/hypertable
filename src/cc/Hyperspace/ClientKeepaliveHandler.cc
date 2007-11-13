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

extern "C" {
#include <poll.h>
}

#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/Exception.h"
#include "Common/StringExt.h"

#include "ClientKeepaliveHandler.h"
#include "Master.h"
#include "Protocol.h"
#include "Session.h"

using namespace Hypertable;
using namespace Hyperspace;

ClientKeepaliveHandler::ClientKeepaliveHandler(Comm *comm, PropertiesPtr &propsPtr, Session *session) : m_comm(comm), m_session(session), m_session_id(0), m_last_known_event(0) {
  int error;
  uint16_t masterPort;
  const char *masterHost;

  m_verbose = propsPtr->getPropertyBool("verbose", false);
  masterHost = propsPtr->getProperty("Hyperspace.Master.host", "localhost");
  masterPort = (uint16_t)propsPtr->getPropertyInt("Hyperspace.Master.port", Master::DEFAULT_MASTER_PORT);
  m_lease_interval = (uint32_t)propsPtr->getPropertyInt("Hyperspace.Lease.Interval", Master::DEFAULT_LEASE_INTERVAL);
  m_keep_alive_interval = (uint32_t)propsPtr->getPropertyInt("Hyperspace.KeepAlive.Interval", Master::DEFAULT_KEEPALIVE_INTERVAL);
  
  if (!InetAddr::initialize(&m_master_addr, masterHost, masterPort))
    exit(1);

  if (m_verbose) {
    cout << "Hyperspace.KeepAlive.Interval=" << m_keep_alive_interval << endl;
    cout << "Hyperspace.Lease.Interval=" << m_lease_interval << endl;
    cout << "Hyperspace.Master.host=" << masterHost << endl;
    cout << "Hyperspace.Master.port=" << masterPort << endl;
  }

  boost::xtime_get(&m_last_keep_alive_send_time, boost::TIME_UTC);
  boost::xtime_get(&m_jeopardy_time, boost::TIME_UTC);
  m_jeopardy_time.sec += m_lease_interval;

  InetAddr::initialize(&m_local_addr, INADDR_ANY, 0);

  DispatchHandlerPtr dhp(this);
  if ((error = m_comm->create_datagram_receive_socket(&m_local_addr, dhp)) != Error::OK) {
    std::string str;
    LOG_VA_ERROR("Unable to create datagram receive socket %s - %s", InetAddr::string_format(str, m_local_addr), Error::get_text(error));
    exit(1);
  }

  CommBufPtr commBufPtr( Hyperspace::Protocol::create_client_keepalive_request(m_session_id, m_last_known_event) );

  if ((error = m_comm->send_datagram(m_master_addr, m_local_addr, commBufPtr) != Error::OK)) {
    LOG_VA_ERROR("Unable to send datagram - %s", Error::get_text(error));
    exit(1);
  }

  if ((error = m_comm->set_timer(m_keep_alive_interval*1000, this)) != Error::OK) {
    LOG_VA_ERROR("Problem setting timer - %s", Error::get_text(error));
    exit(1);
  }

  return;
}

/**
 *
 */
ClientKeepaliveHandler::~ClientKeepaliveHandler() {
  m_comm->close_socket(m_local_addr);
}



/**
 *
 */
void ClientKeepaliveHandler::handle(Hypertable::EventPtr &eventPtr) {
  boost::mutex::scoped_lock lock(m_mutex);
  int error;
  uint16_t command = (uint16_t)-1;

  /**
  if (m_verbose) {
    LOG_VA_INFO("%s", eventPtr->toString().c_str());
  }
  **/

  if (eventPtr->type == Hypertable::Event::MESSAGE) {
    uint8_t *msgPtr = eventPtr->message;
    size_t remaining = eventPtr->messageLen;

    try {

      if (!Serialization::decode_short(&msgPtr, &remaining, &command))
	throw ProtocolException("Truncated Request");

      // sanity check command code
      if (command >= Protocol::COMMAND_MAX)
	throw ProtocolException((std::string)"Invalid command (" + command + ")");

      switch (command) {
      case Protocol::COMMAND_KEEPALIVE:
	{
	  uint64_t sessionId;
	  int state;
	  uint32_t notificationCount;
	  uint64_t handle, eventId;
	  uint32_t eventMask;
	  const char *name;

	  if (m_session->get_state() == Session::STATE_EXPIRED)
	    return;

	  // update jeopardy time
	  memcpy(&m_jeopardy_time, &m_last_keep_alive_send_time, sizeof(boost::xtime));
	  m_jeopardy_time.sec += m_lease_interval;

	  if (!Serialization::decode_long(&msgPtr, &remaining, &sessionId))
	    throw ProtocolException("Truncated Request");

	  if (!Serialization::decode_int(&msgPtr, &remaining, (uint32_t *)&error))
	    throw ProtocolException("Truncated Request");

	  if (error != Error::OK) {
	    if (error != Error::HYPERSPACE_EXPIRED_SESSION) {
	      LOG_VA_ERROR("Master session error - %s", Error::get_text(error));
	    }
	    expire_session();
	    return;
	  }

	  if (m_session_id == 0) {
	    m_session_id = sessionId;
	    if (!m_conn_handler_ptr) {
	      m_conn_handler_ptr = new ClientConnectionHandler(m_comm, m_session, m_lease_interval);
	      m_conn_handler_ptr->set_verbose_mode(m_verbose);
	      m_conn_handler_ptr->set_session_id(m_session_id);
	    }
	  }

	  if (!Serialization::decode_int(&msgPtr, &remaining, &notificationCount)) {
	    throw ProtocolException("Truncated Request");
	  }

	  for (uint32_t i=0; i<notificationCount; i++) {

	    if (!Serialization::decode_long(&msgPtr, &remaining, &handle) ||
		!Serialization::decode_long(&msgPtr, &remaining, &eventId) ||
		!Serialization::decode_int(&msgPtr, &remaining, &eventMask))
	      throw ProtocolException("Truncated Request");

	    HandleMapT::iterator iter = m_handle_map.find(handle);
	    //LOG_VA_INFO("LastKnownEvent=%lld, eventId=%lld eventMask=%d", m_last_known_event, eventId, eventMask);
	    //LOG_VA_INFO("handle=%lldm, eventId=%lld, eventMask=%d", handle, eventId, eventMask);
	    assert (iter != m_handle_map.end());
	    ClientHandleStatePtr handleStatePtr = (*iter).second;

	    if (eventMask == EVENT_MASK_ATTR_SET || eventMask == EVENT_MASK_ATTR_DEL ||
		eventMask == EVENT_MASK_CHILD_NODE_ADDED || eventMask == EVENT_MASK_CHILD_NODE_REMOVED) {
	      if (!Serialization::decode_string(&msgPtr, &remaining, &name))
		throw ProtocolException("Truncated Request");
	      if (eventId <= m_last_known_event)
		continue;
	      if (handleStatePtr->callbackPtr) {
		if (eventMask == EVENT_MASK_ATTR_SET)
		  handleStatePtr->callbackPtr->attr_set(name);
		else if (eventMask == EVENT_MASK_ATTR_DEL)
		  handleStatePtr->callbackPtr->attr_del(name);
		else if (eventMask == EVENT_MASK_CHILD_NODE_ADDED)
		  handleStatePtr->callbackPtr->child_node_added(name);
		else
		  handleStatePtr->callbackPtr->child_node_removed(name);
	      }
	    }
	    else if (eventMask == EVENT_MASK_LOCK_ACQUIRED) {
	      uint32_t mode;
	      if (!Serialization::decode_int(&msgPtr, &remaining, &mode))
		throw ProtocolException("Truncated Request");
	      if (eventId <= m_last_known_event)
		continue;
	      if (handleStatePtr->callbackPtr)
		handleStatePtr->callbackPtr->lock_acquired(mode);
	    }
	    else if (eventMask == EVENT_MASK_LOCK_RELEASED) {
	      if (eventId <= m_last_known_event)
		continue;
	      if (handleStatePtr->callbackPtr)
		handleStatePtr->callbackPtr->lock_released();
	    }
	    else if (eventMask == EVENT_MASK_LOCK_GRANTED) {
	      uint32_t mode;
	      if (!Serialization::decode_int(&msgPtr, &remaining, &mode) ||
		  !Serialization::decode_long(&msgPtr, &remaining, &handleStatePtr->lockGeneration))
		throw ProtocolException("Truncated Request");
	      if (eventId <= m_last_known_event)
		continue;
	      handleStatePtr->lockStatus = LOCK_STATUS_GRANTED;
	      handleStatePtr->sequencer->generation = handleStatePtr->lockGeneration;
	      handleStatePtr->sequencer->mode = mode;
	      handleStatePtr->cond.notify_all();
	    }

	    m_last_known_event = eventId;
	  }
	  
	  /**
	  if (m_verbose) {
	    LOG_VA_INFO("sessionId = %lld", m_session_id);
	  }
	  **/

	  if (m_conn_handler_ptr->disconnected())
	    m_conn_handler_ptr->initiate_connection(m_master_addr);
	  else
	    state = m_session->state_transition(Session::STATE_SAFE);

	  if (notificationCount > 0) {
	    CommBufPtr commBufPtr( Hyperspace::Protocol::create_client_keepalive_request(m_session_id, m_last_known_event) );
	    boost::xtime_get(&m_last_keep_alive_send_time, boost::TIME_UTC);
	    if ((error = m_comm->send_datagram(m_master_addr, m_local_addr, commBufPtr) != Error::OK)) {
	      LOG_VA_ERROR("Unable to send datagram - %s", Error::get_text(error));
	      exit(1);
	    }
	  }

	  assert(m_session_id == sessionId);
	}
	break;
      default:
	throw ProtocolException((string)"Command code " + command + " not implemented");
      }
    }
    catch (ProtocolException &e) {
      std::string errMsg = e.what();
      LOG_VA_ERROR("Protocol error '%s'", e.what());
    }
  }
  else if (eventPtr->type == Hypertable::Event::TIMER) {
    boost::xtime now;
    int state;
    
    // !!! fix - what about re-ordered packets?

    if ((state = m_session->get_state()) == Session::STATE_EXPIRED)
      return;

    boost::xtime_get(&now, boost::TIME_UTC);

    if (state == Session::STATE_SAFE) {
      if (xtime_cmp(m_jeopardy_time, now) < 0) {
	m_session->state_transition(Session::STATE_JEOPARDY);
      }
    }
    else if (m_session->expired()) {
      expire_session();
      return;
    }

    CommBufPtr commBufPtr( Hyperspace::Protocol::create_client_keepalive_request(m_session_id, m_last_known_event) );

    boost::xtime_get(&m_last_keep_alive_send_time, boost::TIME_UTC);
    
    if ((error = m_comm->send_datagram(m_master_addr, m_local_addr, commBufPtr) != Error::OK)) {
      LOG_VA_ERROR("Unable to send datagram - %s", Error::get_text(error));
      exit(1);
    }

    if ((error = m_comm->set_timer(m_keep_alive_interval*1000, this)) != Error::OK) {
      LOG_VA_ERROR("Problem setting timer - %s", Error::get_text(error));
      exit(1);
    }
  }
  else {
    LOG_VA_INFO("%s", eventPtr->toString().c_str());
  }
}



void ClientKeepaliveHandler::expire_session() {
  m_session->state_transition(Session::STATE_EXPIRED);
  if (m_conn_handler_ptr)
    m_conn_handler_ptr->close();
  poll(0,0,2000);
  m_conn_handler_ptr = 0;
  return;
}
