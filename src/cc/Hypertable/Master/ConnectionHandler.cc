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

#include "Hypertable/Lib/MasterProtocol.h"

#include "ConnectionHandler.h"
#include "RequestHandlerCreateTable.h"
#include "RequestHandlerDropTable.h"
#include "RequestHandlerGetSchema.h"
#include "RequestHandlerStatus.h"
#include "RequestHandlerRegisterServer.h"
#include "RequestHandlerReportSplit.h"

using namespace Hypertable;

/**
 *
 */
void ConnectionHandler::handle(EventPtr &eventPtr) {
  short command = -1;

  if (eventPtr->type == Event::MESSAGE) {
    ApplicationHandler *requestHandler = 0;

    //eventPtr->display()

    try {
      if (eventPtr->messageLen < sizeof(int16_t)) {
	std::string message = "Truncated Request";
	throw new ProtocolException(message);
      }
      memcpy(&command, eventPtr->message, sizeof(int16_t));

      // sanity check command code
      if (command < 0 || command >= MasterProtocol::COMMAND_MAX) {
	std::string message = (std::string)"Invalid command (" + command + ")";
	throw ProtocolException(message);
      }

      switch (command) {
      case MasterProtocol::COMMAND_CREATE_TABLE:
	requestHandler = new RequestHandlerCreateTable(m_comm, m_master_ptr.get(), eventPtr);
	break;
      case MasterProtocol::COMMAND_DROP_TABLE:
	requestHandler = new RequestHandlerDropTable(m_comm, m_master_ptr.get(), eventPtr);
	break;
      case MasterProtocol::COMMAND_GET_SCHEMA:
	requestHandler = new RequestHandlerGetSchema(m_comm, m_master_ptr.get(), eventPtr);
	break;
      case MasterProtocol::COMMAND_STATUS:
	requestHandler = new RequestHandlerStatus(m_comm, m_master_ptr.get(), eventPtr);
	break;
      case MasterProtocol::COMMAND_REGISTER_SERVER:
	requestHandler = new RequestHandlerRegisterServer(m_comm, m_master_ptr.get(), eventPtr);
	break;
      case MasterProtocol::COMMAND_REPORT_SPLIT:
	requestHandler = new RequestHandlerReportSplit(m_comm, m_master_ptr.get(), eventPtr);
	break;
      default:
	std::string message = (string)"Command code " + command + " not implemented";
	throw ProtocolException(message);
      }
      m_app_queue_ptr->add( requestHandler );
    }
    catch (ProtocolException &e) {
      ResponseCallback cb(m_comm, eventPtr);
      std::string errMsg = e.what();
      HT_ERRORF("Protocol error '%s'", e.what());
      cb.error(Error::PROTOCOL_ERROR, errMsg);
    }
  }
  else {
    HT_INFOF("%s", eventPtr->toString().c_str());
  }

}

