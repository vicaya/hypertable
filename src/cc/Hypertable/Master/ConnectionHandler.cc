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

#include "AsyncComm/MessageBuilderSimple.h"

#include "Global.h"
#include "ConnectionHandler.h"

using namespace hypertable;


/**
 *
 */
void ConnectionHandler::handle(Event &event) {
  short command = -1;

  if (event.type == Event::MESSAGE) {

    //event.Display()

    try {
      if (event.messageLen < sizeof(int16_t)) {
	std::string message = "Truncated Request";
	throw new ProtocolException(message);
      }
      memcpy(&command, event.message, sizeof(int16_t));
      Global::workQueue->AddRequest( mRequestFactory.newInstance(event, command) );
    }
    catch (ProtocolException &e) {
      MessageBuilderSimple mbuilder;

      LOG_VA_ERROR("Protocol error '%s'", e.what());

      // Build error message
      CommBuf *cbuf = Global::protocol->CreateErrorMessage(command, Error::PROTOCOL_ERROR, e.what(), mbuilder.HeaderLength());

      // Encapsulate with Comm message response header
      mbuilder.LoadFromMessage(event.header);
      mbuilder.Encapsulate(cbuf);

      Global::comm->SendResponse(event.addr, cbuf);
    }
  }
  else if (event.type == Event::DISCONNECT) {
    LOG_VA_INFO("%s : Closing all open handles", event.toString().c_str());
  }
  else {
    LOG_VA_INFO("%s", event.toString().c_str());
  }

}

