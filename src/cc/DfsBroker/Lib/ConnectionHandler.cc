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
#include "RequestHandlerRmdir.h"
#include "RequestHandlerReaddir.h"

using namespace Hypertable;
using namespace Hypertable::DfsBroker;

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
      if (command < 0 || command >= Protocol::COMMAND_MAX) {
	std::string message = (std::string)"Invalid command (" + command + ")";
	throw ProtocolException(message);
      }

      switch (command) {
      case Protocol::COMMAND_OPEN:
	requestHandler = new RequestHandlerOpen(m_comm, m_broker_ptr.get(), eventPtr);
	break;
      case Protocol::COMMAND_CREATE:
	requestHandler = new RequestHandlerCreate(m_comm, m_broker_ptr.get(), eventPtr);
	break;
      case Protocol::COMMAND_CLOSE:
	requestHandler = new RequestHandlerClose(m_comm, m_broker_ptr.get(), eventPtr);
	break;
      case Protocol::COMMAND_READ:
	requestHandler = new RequestHandlerRead(m_comm, m_broker_ptr.get(), eventPtr);
	break;
      case Protocol::COMMAND_APPEND:
	requestHandler = new RequestHandlerAppend(m_comm, m_broker_ptr.get(), eventPtr);
	break;
      case Protocol::COMMAND_SEEK:
	requestHandler = new RequestHandlerSeek(m_comm, m_broker_ptr.get(), eventPtr);
	break;
      case Protocol::COMMAND_REMOVE:
	requestHandler = new RequestHandlerRemove(m_comm, m_broker_ptr.get(), eventPtr);
	break;
      case Protocol::COMMAND_LENGTH:
	requestHandler = new RequestHandlerLength(m_comm, m_broker_ptr.get(), eventPtr);
	break;
      case Protocol::COMMAND_PREAD:
	requestHandler = new RequestHandlerPread(m_comm, m_broker_ptr.get(), eventPtr);
	break;
      case Protocol::COMMAND_MKDIRS:
	requestHandler = new RequestHandlerMkdirs(m_comm, m_broker_ptr.get(), eventPtr);
	break;
      case Protocol::COMMAND_FLUSH:
	requestHandler = new RequestHandlerFlush(m_comm, m_broker_ptr.get(), eventPtr);
	break;
      case Protocol::COMMAND_RMDIR:
	requestHandler = new RequestHandlerRmdir(m_comm, m_broker_ptr.get(), eventPtr);
	break;
      case Protocol::COMMAND_READDIR:
	requestHandler = new RequestHandlerReaddir(m_comm, m_broker_ptr.get(), eventPtr);
	break;
      case Protocol::COMMAND_STATUS:
	requestHandler = new RequestHandlerStatus(m_comm, m_broker_ptr.get(), eventPtr);
	break;
      case Protocol::COMMAND_SHUTDOWN:
	{
	  uint16_t flags = 0;
	  ResponseCallback cb(m_comm, eventPtr);
	  if (eventPtr->messageLen >= 4)
	    memcpy(&flags, &eventPtr->message[2], 2);
	  if ((flags & Protocol::SHUTDOWN_FLAG_IMMEDIATE) != 0)
	    m_app_queue_ptr->shutdown();
	  m_broker_ptr->shutdown(&cb);
	  exit(0);
	}
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
  else if (eventPtr->type == Event::DISCONNECT) {
    HT_INFOF("%s : Closing all open handles from %s:%d", eventPtr->toString().c_str(),
		inet_ntoa(eventPtr->addr.sin_addr), ntohs(eventPtr->addr.sin_port));
    OpenFileMap &ofMap = m_broker_ptr->get_open_file_map();
    ofMap.remove_all(eventPtr->addr);
  }
  else {
    HT_INFOF("%s", eventPtr->toString().c_str());
  }

}

