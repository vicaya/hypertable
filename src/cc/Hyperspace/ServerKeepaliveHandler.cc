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

using namespace hypertable;
using namespace Hyperspace;


/**
 *
 */
ServerKeepaliveHandler::ServerKeepaliveHandler(Comm *comm, Master *master) : mComm(comm), mMaster(master) { 
  int error; 

  mMaster->GetDatagramSendAddress(&mSendAddr);

  if ((error = mComm->SetTimer(1000, this)) != Error::OK) {
    LOG_VA_ERROR("Problem setting timer - %s", Error::GetText(error));
    exit(1);
  }
  
  return; 
}



/**
 *
 */
void ServerKeepaliveHandler::handle(hypertable::EventPtr &eventPtr) {
  uint16_t command = (uint16_t)-1;
  int error;

  if (eventPtr->type == hypertable::Event::MESSAGE) {
    uint8_t *msgPtr = eventPtr->message;
    size_t remaining = eventPtr->messageLen;

    try {

      if (!Serialization::DecodeShort(&msgPtr, &remaining, &command)) {
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

	  if (!Serialization::DecodeLong(&msgPtr, &remaining, &sessionId) ||
	      !Serialization::DecodeLong(&msgPtr, &remaining, &lastKnownEvent)) {
	    std::string message = "Truncated Request";
	    throw new ProtocolException(message);
	  }

	  if (sessionId == 0) {
	    sessionId = mMaster->CreateSession(eventPtr->addr);
	    LOG_VA_INFO("Session handle %lld created", sessionId);
	    error = Error::OK;
	  }
	  else
	    error = mMaster->RenewSessionLease(sessionId);

	  if (error == Error::HYPERSPACE_EXPIRED_SESSION) {
	    LOG_VA_INFO("Session handle %lld expired", sessionId);
	    CommBufPtr cbufPtr( Protocol::CreateServerKeepaliveRequest(sessionId, Error::HYPERSPACE_EXPIRED_SESSION) );
	    if ((error = mComm->SendDatagram(eventPtr->addr, mSendAddr, cbufPtr)) != Error::OK) {
	      LOG_VA_ERROR("Comm::SendDatagram returned %s", Error::GetText(error));
	    }
	    return;
	  }

	  if (!mMaster->GetSession(sessionId, sessionPtr)) {
	    LOG_VA_ERROR("Unable to find data for session %lld", sessionId);
	    DUMP_CORE;
	  }

	  sessionPtr->PurgeNotifications(lastKnownEvent);

	  /**
	  {
	    std::string str;
	    LOG_VA_INFO("Sending Keepalive request to %s (lastKnownEvent=%lld)", InetAddr::StringFormat(str, eventPtr->addr), lastKnownEvent);
	  }
	  **/

	  CommBufPtr cbufPtr( Protocol::CreateServerKeepaliveRequest(sessionPtr) );
	  if ((error = mComm->SendDatagram(eventPtr->addr, mSendAddr, cbufPtr)) != Error::OK) {
	    LOG_VA_ERROR("Comm::SendDatagram returned %s", Error::GetText(error));
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
  else if (eventPtr->type == hypertable::Event::TIMER) {

    mMaster->RemoveExpiredSessions();

    if ((error = mComm->SetTimer(1000, this)) != Error::OK) {
      LOG_VA_ERROR("Problem setting timer - %s", Error::GetText(error));
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
void ServerKeepaliveHandler::DeliverEventNotifications(uint64_t sessionId) {
  int error = 0;
  SessionDataPtr sessionPtr;

  //LOG_VA_INFO("Delivering event notifications for session %lld", sessionId);

  if (!mMaster->GetSession(sessionId, sessionPtr)) {
    LOG_VA_ERROR("Unable to find data for session %lld", sessionId);
    DUMP_CORE;
  }

  /**
  {
    std::string str;
    LOG_VA_INFO("Sending Keepalive request to %s", InetAddr::StringFormat(str, sessionPtr->addr));
  }
  **/

  CommBufPtr cbufPtr( Protocol::CreateServerKeepaliveRequest(sessionPtr) );
  if ((error = mComm->SendDatagram(sessionPtr->addr, mSendAddr, cbufPtr)) != Error::OK) {
    LOG_VA_ERROR("Comm::SendDatagram returned %s", Error::GetText(error));
  }

}
