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
#include "Common/StringExt.h"

#include "AsyncComm/ApplicationQueue.h"

#include "Protocol.h"
#include "RequestHandlerAttrSet.h"
#include "RequestHandlerAttrGet.h"
#include "RequestHandlerAttrDel.h"
#include "RequestHandlerMkdir.h"
#include "RequestHandlerDelete.h"
#include "RequestHandlerOpen.h"
#include "RequestHandlerClose.h"
#include "RequestHandlerExists.h"
#include "RequestHandlerReaddir.h"
#include "RequestHandlerLock.h"
#include "RequestHandlerRelease.h"
#include "RequestHandlerStatus.h"
#include "ServerConnectionHandler.h"

using namespace Hypertable;
using namespace Hyperspace;


/**
 *
 */
void ServerConnectionHandler::handle(EventPtr &eventPtr) {
  uint16_t command = (uint16_t)-1;
  int error;

  HT_INFOF("%s", eventPtr->toString().c_str());

  if (eventPtr->type == Hypertable::Event::MESSAGE) {
    ApplicationHandler *requestHandler = 0;
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

      if (command != Protocol::COMMAND_HANDSHAKE && 
	  (error = m_master_ptr->renew_session_lease(m_session_id)) != Error::OK) {
	ResponseCallback cb(m_comm, eventPtr);
	HT_INFOF("Session handle %lld expired", m_session_id);
	cb.error(error, "");
	return;
      }

      switch (command) {
      case Protocol::COMMAND_HANDSHAKE:
	{
	  ResponseCallback cb(m_comm, eventPtr);
	  if (!Serialization::decode_long(&msgPtr, &remaining, &m_session_id)) {
	    std::string message = "Truncated Request";
	    throw new ProtocolException(message);
	  }
	  if (m_session_id == 0) {
	    std::string message = "Bad Session ID: 0";
	    throw new ProtocolException(message);
	  }
	  cb.response_ok();
	}
	return;
      case Protocol::COMMAND_OPEN:
	requestHandler = new RequestHandlerOpen(m_comm, m_master_ptr.get(), m_session_id, eventPtr);
	break;
      case Protocol::COMMAND_CLOSE:
	requestHandler = new RequestHandlerClose(m_comm, m_master_ptr.get(), m_session_id, eventPtr);
	break;
      case Protocol::COMMAND_MKDIR:
	requestHandler = new RequestHandlerMkdir(m_comm, m_master_ptr.get(), m_session_id, eventPtr);
	break;
      case Protocol::COMMAND_DELETE:
	requestHandler = new RequestHandlerDelete(m_comm, m_master_ptr.get(), m_session_id, eventPtr);
	break;
      case Protocol::COMMAND_ATTRSET:
	requestHandler = new RequestHandlerAttrSet(m_comm, m_master_ptr.get(), m_session_id, eventPtr);
	break;
      case Protocol::COMMAND_ATTRGET:
	requestHandler = new RequestHandlerAttrGet(m_comm, m_master_ptr.get(), m_session_id, eventPtr);
	break;
      case Protocol::COMMAND_ATTRDEL:
	requestHandler = new RequestHandlerAttrDel(m_comm, m_master_ptr.get(), m_session_id, eventPtr);
	break;
      case Protocol::COMMAND_EXISTS:
	requestHandler = new RequestHandlerExists(m_comm, m_master_ptr.get(), m_session_id, eventPtr);
	break;
      case Protocol::COMMAND_READDIR:
	requestHandler = new RequestHandlerReaddir(m_comm, m_master_ptr.get(), m_session_id, eventPtr);
	break;
      case Protocol::COMMAND_LOCK:
	requestHandler = new RequestHandlerLock(m_comm, m_master_ptr.get(), m_session_id, eventPtr);
	break;
      case Protocol::COMMAND_RELEASE:
	requestHandler = new RequestHandlerRelease(m_comm, m_master_ptr.get(), m_session_id, eventPtr);
	break;
      case Protocol::COMMAND_STATUS:
	requestHandler = new RequestHandlerStatus(m_comm, m_master_ptr.get(), m_session_id, eventPtr);
	break;
      default:
	std::string message = (string)"Command code " + command + " not implemented";
	throw ProtocolException(message);
      }
      m_app_queue_ptr->add(requestHandler);
    }
    catch (ProtocolException &e) {
      ResponseCallback cb(m_comm, eventPtr);
      std::string errMsg = e.what();
      HT_ERRORF("Protocol error '%s'", e.what());
      cb.error(Error::PROTOCOL_ERROR, errMsg);
    }
  }
  else if (eventPtr->type == Hypertable::Event::CONNECTION_ESTABLISHED) {
    HT_INFOF("%s", eventPtr->toString().c_str());    
  }
  else if (eventPtr->type == Hypertable::Event::DISCONNECT) {
    m_master_ptr->destroy_session(m_session_id);
    cout << flush;
  }
  else {
    HT_INFOF("%s", eventPtr->toString().c_str());
  }

}

