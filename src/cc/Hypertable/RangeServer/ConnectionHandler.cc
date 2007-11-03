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
#include <iostream>

#include "Common/Error.h"
#include "Common/Exception.h"
#include "Common/StringExt.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/ResponseCallback.h"

#include "Hypertable/Lib/MasterClient.h"
#include "Hypertable/Lib/RangeServerProtocol.h"

#include "RequestHandlerCompact.h"
#include "RequestHandlerLoadRange.h"
#include "RequestHandlerUpdate.h"
#include "RequestHandlerCreateScanner.h"
#include "RequestHandlerFetchScanblock.h"
#include "RequestHandlerStatus.h"

#include "ConnectionHandler.h"
#include "EventHandlerMasterConnection.h"
#include "RangeServer.h"


using namespace hypertable;

/**
 *
 */
ConnectionHandler::ConnectionHandler(Comm *comm, ApplicationQueuePtr &appQueuePtr, RangeServerPtr rangeServerPtr, MasterClientPtr &masterClientPtr) : mComm(comm), mAppQueuePtr(appQueuePtr), mRangeServerPtr(rangeServerPtr), mMasterClientPtr(masterClientPtr) {
  return;
}

/**
 *
 */
ConnectionHandler::ConnectionHandler(Comm *comm, ApplicationQueuePtr &appQueuePtr, RangeServerPtr rangeServerPtr) : mComm(comm), mAppQueuePtr(appQueuePtr), mRangeServerPtr(rangeServerPtr) {
  return;
}


/**
 *
 */
void ConnectionHandler::handle(EventPtr &eventPtr) {
  short command = -1;

  if (eventPtr->type == Event::MESSAGE) {
    ApplicationHandler *requestHandler = 0;

    //eventPtr->Display();

    try {
      if (eventPtr->messageLen < sizeof(int16_t)) {
	std::string message = "Truncated Request";
	throw new ProtocolException(message);
      }
      memcpy(&command, eventPtr->message, sizeof(int16_t));

      // sanity check command code
      if (command < 0 || command >= RangeServerProtocol::COMMAND_MAX) {
	std::string message = (std::string)"Invalid command (" + command + ")";
	throw ProtocolException(message);
      }

      switch (command) {
      case RangeServerProtocol::COMMAND_COMPACT:
	requestHandler = new RequestHandlerCompact(mComm, mRangeServerPtr.get(), eventPtr);
	break;
      case RangeServerProtocol::COMMAND_LOAD_RANGE:
	requestHandler = new RequestHandlerLoadRange(mComm, mRangeServerPtr.get(), eventPtr);
	break;
      case RangeServerProtocol::COMMAND_UPDATE:
	requestHandler = new RequestHandlerUpdate(mComm, mRangeServerPtr.get(), eventPtr);
	break;
      case RangeServerProtocol::COMMAND_CREATE_SCANNER:
	requestHandler = new RequestHandlerCreateScanner(mComm, mRangeServerPtr.get(), eventPtr);
	break;
      case RangeServerProtocol::COMMAND_FETCH_SCANBLOCK:
	requestHandler = new RequestHandlerFetchScanblock(mComm, mRangeServerPtr.get(), eventPtr);
	break;
      case RangeServerProtocol::COMMAND_STATUS:
	requestHandler = new RequestHandlerStatus(mComm, mRangeServerPtr.get(), eventPtr);
	break;
      default:
	std::string message = (string)"Command code " + command + " not implemented";
	throw ProtocolException(message);
      }
      mAppQueuePtr->Add( requestHandler );
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

    /**
     * If this is the connection to the Master, then we need to register ourselves
     * with the master
     */
    if (mMasterClientPtr)
      mAppQueuePtr->Add( new EventHandlerMasterConnection(mMasterClientPtr, mRangeServerPtr->ServerIdStr(), eventPtr) );
  }
  else {
    LOG_VA_INFO("%s", eventPtr->toString().c_str());
  }

}

