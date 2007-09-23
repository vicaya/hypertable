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

#include "ServerKeepaliveHandler.h"
#include "Protocol.h"
#include "SessionData.h"

using namespace hypertable;
using namespace Hyperspace;

/**
 *
 */
void ServerKeepaliveHandler::handle(EventPtr &eventPtr) {
  uint16_t command = (uint16_t)-1;
  int error;

  if (eventPtr->type == Event::MESSAGE) {
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
	  }

	  CommBufPtr cbufPtr( Protocol::CreateServerKeepaliveRequest(sessionId, error) );
	  if ((error = mComm->SendDatagram(eventPtr->addr, eventPtr->localAddr, cbufPtr)) != Error::OK) {
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
  else {
    LOG_VA_INFO("%s", eventPtr->toString().c_str());
  }
}

