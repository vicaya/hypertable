/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#include "Common/Error.h"
#include "Common/Exception.h"
#include "Common/StringExt.h"

#include "ConnectionHandler.h"
#include "MasterProtocol.h"
#include "RequestHandlerCreateTable.h"
#include "RequestHandlerGetSchema.h"

using namespace hypertable;

/**
 *
 */
void ConnectionHandler::handle(Event &event) {
  short command = -1;

  if (event.type == Event::MESSAGE) {
    Runnable *requestHandler = 0;

    //event.Display()

    try {
      if (event.messageLen < sizeof(int16_t)) {
	std::string message = "Truncated Request";
	throw new ProtocolException(message);
      }
      memcpy(&command, event.message, sizeof(int16_t));

      // sanity check command code
      if (command < 0 || command >= MasterProtocol::COMMAND_MAX) {
	std::string message = (std::string)"Invalid command (" + command + ")";
	throw ProtocolException(message);
      }

      switch (command) {
      case MasterProtocol::COMMAND_CREATE_TABLE:
	requestHandler = new RequestHandlerCreateTable(mComm, mMaster, event);
	break;
      case MasterProtocol::COMMAND_GET_SCHEMA:
	requestHandler = new RequestHandlerGetSchema(mComm, mMaster, event);
	break;
      default:
	std::string message = (string)"Command code " + command + " not implemented";
	throw ProtocolException(message);
      }
      mWorkQueue->AddRequest( requestHandler );
    }
    catch (ProtocolException &e) {
      ResponseCallback cb(mComm, event);
      std::string errMsg = e.what();
      LOG_VA_ERROR("Protocol error '%s'", e.what());
      cb.error(Error::PROTOCOL_ERROR, errMsg);
    }
  }
  else if (event.type == Event::DISCONNECT) {
    // do something here!!!
    LOG_VA_INFO("%s : Closing all open handles", event.toString().c_str());
  }
  else {
    LOG_VA_INFO("%s", event.toString().c_str());
  }

}

