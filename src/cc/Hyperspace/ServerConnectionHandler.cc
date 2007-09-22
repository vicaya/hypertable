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
#include "RequestHandlerMkdir.h"
#include "RequestHandlerOpen.h"
#include "ServerConnectionHandler.h"

using namespace hypertable;
using namespace Hyperspace;


/**
 *
 */
void ServerConnectionHandler::handle(EventPtr &eventPtr) {
  uint16_t command = (uint16_t)-1;
  int error;

  LOG_VA_INFO("%s", eventPtr->toString().c_str());

  if (eventPtr->type == Event::MESSAGE) {
    ApplicationHandler *requestHandler = 0;
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
      case Protocol::COMMAND_HANDSHAKE:
	{
	  ResponseCallback cb(mComm, eventPtr);
	  if (!Serialization::DecodeLong(&msgPtr, &remaining, &mSessionId)) {
	    std::string message = "Truncated Request";
	    throw new ProtocolException(message);
	  }
	  if (mSessionId == 0) {
	    std::string message = "Bad Session ID: 0";
	    throw new ProtocolException(message);
	  }
	  cb.response_ok();
	}
	return;
      case Protocol::COMMAND_OPEN:
	requestHandler = new RequestHandlerOpen(mComm, mMaster, mSessionId, eventPtr);
	break;
      case Protocol::COMMAND_MKDIR:
	requestHandler = new RequestHandlerMkdir(mComm, mMaster, mSessionId, eventPtr);
	break;
      default:
	std::string message = (string)"Command code " + command + " not implemented";
	throw ProtocolException(message);
      }
      ApplicationHandlerPtr appHandlerPtr(requestHandler);
      mAppQueue->Add(appHandlerPtr);
    }
    catch (ProtocolException &e) {
      ResponseCallback cb(mComm, eventPtr);
      std::string errMsg = e.what();
      LOG_VA_ERROR("Protocol error '%s'", e.what());
      cb.error(Error::PROTOCOL_ERROR, errMsg);
    }
  }
  else if (eventPtr->type == Event::CONNECTION_ESTABLISHED) {
    LOG_VA_INFO("%s", eventPtr->toString().c_str());    
  }
  else if (eventPtr->type == Event::DISCONNECT) {
    // do something here!!!
    LOG_VA_INFO("%s : Closing all open handles", eventPtr->toString().c_str());
  }
  else {
    LOG_VA_INFO("%s", eventPtr->toString().c_str());
  }

}

