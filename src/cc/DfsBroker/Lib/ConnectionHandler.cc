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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
#include "Common/Error.h"
#include "Common/String.h"
#include "Common/StringExt.h"
#include "Common/Serialization.h"

#include "AsyncComm/ApplicationQueue.h"

#include "Protocol.h"
#include "ConnectionHandler.h"
#include "RequestHandlerClose.h"
#include "RequestHandlerCreate.h"
#include "RequestHandlerDebug.h"
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
#include "RequestHandlerExists.h"
#include "RequestHandlerRename.h"

using namespace Hypertable;
using namespace DfsBroker;
using namespace Serialization;

/**
 *
 */
void ConnectionHandler::handle(EventPtr &event) {

  if (event->type == Event::MESSAGE) {
    ApplicationHandler *handler = 0;
    const uint8_t *decode_ptr = event->payload;
    size_t decode_remain = event->payload_len;

    //event->display()

    try {
      // sanity check command code
      if (event->header.command < 0
          || event->header.command >= Protocol::COMMAND_MAX)
        HT_THROWF(Error::PROTOCOL_ERROR, "Invalid command (%llx)",
                  (Llu)event->header.command);

      switch (event->header.command) {
      case Protocol::COMMAND_OPEN:
        handler = new RequestHandlerOpen(m_comm, m_broker_ptr.get(), event);
        break;
      case Protocol::COMMAND_CREATE:
        handler = new RequestHandlerCreate(m_comm, m_broker_ptr.get(), event);
        break;
      case Protocol::COMMAND_CLOSE:
        handler = new RequestHandlerClose(m_comm, m_broker_ptr.get(), event);
        break;
      case Protocol::COMMAND_READ:
        handler = new RequestHandlerRead(m_comm, m_broker_ptr.get(), event);
        break;
      case Protocol::COMMAND_APPEND:
        handler = new RequestHandlerAppend(m_comm, m_broker_ptr.get(), event);
        break;
      case Protocol::COMMAND_SEEK:
        handler = new RequestHandlerSeek(m_comm, m_broker_ptr.get(), event);
        break;
      case Protocol::COMMAND_REMOVE:
        handler = new RequestHandlerRemove(m_comm, m_broker_ptr.get(), event);
        break;
      case Protocol::COMMAND_LENGTH:
        handler = new RequestHandlerLength(m_comm, m_broker_ptr.get(), event);
        break;
      case Protocol::COMMAND_PREAD:
        handler = new RequestHandlerPread(m_comm, m_broker_ptr.get(), event);
        break;
      case Protocol::COMMAND_MKDIRS:
        handler = new RequestHandlerMkdirs(m_comm, m_broker_ptr.get(), event);
        break;
      case Protocol::COMMAND_FLUSH:
        handler = new RequestHandlerFlush(m_comm, m_broker_ptr.get(), event);
        break;
      case Protocol::COMMAND_RMDIR:
        handler = new RequestHandlerRmdir(m_comm, m_broker_ptr.get(), event);
        break;
      case Protocol::COMMAND_READDIR:
        handler = new RequestHandlerReaddir(m_comm, m_broker_ptr.get(), event);
        break;
      case Protocol::COMMAND_EXISTS:
        handler = new RequestHandlerExists(m_comm, m_broker_ptr.get(), event);
        break;
      case Protocol::COMMAND_RENAME:
        handler = new RequestHandlerRename(m_comm, m_broker_ptr.get(), event);
        break;
      case Protocol::COMMAND_DEBUG:
        handler = new RequestHandlerDebug(m_comm, m_broker_ptr.get(), event);
        break;
      case Protocol::COMMAND_STATUS:
        handler = new RequestHandlerStatus(m_comm, m_broker_ptr.get(), event);
        break;
      case Protocol::COMMAND_SHUTDOWN: {
          uint16_t flags = 0;
          ResponseCallback cb(m_comm, event);
          if (event->payload_len >= 2)
            flags = Serialization::decode_i16(&decode_ptr, &decode_remain);
          if ((flags & Protocol::SHUTDOWN_FLAG_IMMEDIATE) != 0)
            m_app_queue_ptr->shutdown();
          m_broker_ptr->shutdown(&cb);
          exit(0);
        }
        break;
      default:
        HT_THROWF(Error::PROTOCOL_ERROR, "Unimplemented command (%llx)",
                  (Llu)event->header.command);
      }
      m_app_queue_ptr->add(handler);
    }
    catch (Exception &e) {
      ResponseCallback cb(m_comm, event);
      HT_ERROR_OUT << e << HT_END;
      String errmsg = format("%s - %s", e.what(), Error::get_text(e.code()));
      cb.error(Error::PROTOCOL_ERROR, errmsg);
    }
  }
  else if (event->type == Event::DISCONNECT) {
    HT_DEBUGF("%s : Closing all open handles from %s", event->to_str().c_str(),
              event->addr.format().c_str());
    OpenFileMap &ofmap = m_broker_ptr->get_open_file_map();
    ofmap.remove_all(event->addr);
  }
  else {
    HT_DEBUGF("%s", event->to_str().c_str());
  }

}

