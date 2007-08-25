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
#include "ConnectionHandler.h"
#include "RequestHandlerClose.h"
#include "RequestHandlerCreate.h"
#include "RequestHandlerOpen.h"
#include "RequestHandlerRead.h"
#include "RequestHandlerAppend.h"
#include "RequestHandlerSeek.h"
#include "RequestHandlerRemove.h"
#include "RequestHandlerLength.h"
#include "RequestHandlerPread.h"
#include "RequestHandlerMkdirs.h"
#include "RequestHandlerFlush.h"
#include "RequestHandlerStatus.h"

using namespace hypertable;
using namespace hypertable::DfsBroker;

/**
 *
 */
void ConnectionHandler::handle(EventPtr &eventPtr) {
  short command = -1;

  if (eventPtr->type == Event::MESSAGE) {
    ApplicationHandler *requestHandler = 0;

    //eventPtr->Display()

    try {
      if (eventPtr->messageLen < sizeof(int16_t)) {
	std::string message = "Truncated Request";
	throw new ProtocolException(message);
      }
      memcpy(&command, eventPtr->message, sizeof(int16_t));

      // sanity check command code
      if (command < 0 || command >= Protocol::COMMAND_MAX) {
	std::string message = (std::string)"Invalid command (" + command + ")";
	throw ProtocolException(message);
      }

      switch (command) {
      case Protocol::COMMAND_OPEN:
	requestHandler = new RequestHandlerOpen(mComm, mBroker, eventPtr);
	break;
      case Protocol::COMMAND_CREATE:
	requestHandler = new RequestHandlerCreate(mComm, mBroker, eventPtr);
	break;
      case Protocol::COMMAND_CLOSE:
	requestHandler = new RequestHandlerClose(mComm, mBroker, eventPtr);
	break;
      case Protocol::COMMAND_READ:
	requestHandler = new RequestHandlerRead(mComm, mBroker, eventPtr);
	break;
      case Protocol::COMMAND_APPEND:
	requestHandler = new RequestHandlerAppend(mComm, mBroker, eventPtr);
	break;
      case Protocol::COMMAND_SEEK:
	requestHandler = new RequestHandlerSeek(mComm, mBroker, eventPtr);
	break;
      case Protocol::COMMAND_REMOVE:
	requestHandler = new RequestHandlerRemove(mComm, mBroker, eventPtr);
	break;
      case Protocol::COMMAND_LENGTH:
	requestHandler = new RequestHandlerLength(mComm, mBroker, eventPtr);
	break;
      case Protocol::COMMAND_PREAD:
	requestHandler = new RequestHandlerPread(mComm, mBroker, eventPtr);
	break;
      case Protocol::COMMAND_MKDIRS:
	requestHandler = new RequestHandlerMkdirs(mComm, mBroker, eventPtr);
	break;
      case Protocol::COMMAND_FLUSH:
	requestHandler = new RequestHandlerFlush(mComm, mBroker, eventPtr);
	break;
      case Protocol::COMMAND_STATUS:
	requestHandler = new RequestHandlerStatus(mComm, mBroker, eventPtr);
	break;
      case Protocol::COMMAND_SHUTDOWN:
	{
	  uint16_t flags = 0;
	  ResponseCallback cb(mComm, eventPtr);
	  if (eventPtr->messageLen >= 4)
	    memcpy(&flags, &eventPtr->message[2], 2);
	  mBroker->Shutdown(&cb, flags);
	}
	break;
      default:
	std::string message = (string)"Command code " + command + " not implemented";
	throw ProtocolException(message);
      }
      ApplicationHandlerPtr  appHandlerPtr( requestHandler );
      mAppQueue->Add( appHandlerPtr );
    }
    catch (ProtocolException &e) {
      ResponseCallback cb(mComm, eventPtr);
      std::string errMsg = e.what();
      LOG_VA_ERROR("Protocol error '%s'", e.what());
      cb.error(Error::PROTOCOL_ERROR, errMsg);
    }
  }
  else if (eventPtr->type == Event::DISCONNECT) {
    // do something here!!!
    LOG_VA_INFO("%s : Closing all open handles", eventPtr->toString().c_str());
  }
  else {
    LOG_VA_INFO("%s", eventPtr->toString().c_str());
  }

}

