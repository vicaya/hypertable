/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
#include "Common/StringExt.h"

#include "AsyncComm/ApplicationQueue.h"

#include "Protocol.h"
#include "RequestHandlerAttrSet.h"
#include "RequestHandlerAttrGet.h"
#include "RequestHandlerAttrDel.h"
#include "RequestHandlerAttrExists.h"
#include "RequestHandlerAttrList.h"
#include "RequestHandlerMkdir.h"
#include "RequestHandlerDelete.h"
#include "RequestHandlerOpen.h"
#include "RequestHandlerClose.h"
#include "RequestHandlerExists.h"
#include "RequestHandlerReaddir.h"
#include "RequestHandlerLock.h"
#include "RequestHandlerRelease.h"
#include "RequestHandlerStatus.h"
#include "RequestHandlerHandshake.h"
#include "ServerConnectionHandler.h"

using namespace std;
using namespace Hypertable;
using namespace Hyperspace;
using namespace Serialization;


/**
 *
 */
void ServerConnectionHandler::handle(EventPtr &event) {

  //HT_INFOF("%s", event->to_str().c_str());

  if (event->type == Hypertable::Event::MESSAGE) {
    ApplicationHandler *handler = 0;

    try {

      // sanity check command code
      if (event->header.command >= Protocol::COMMAND_MAX)
        HT_THROWF(Error::PROTOCOL_ERROR, "Invalid command (%llu)",
                  (Llu)event->header.command);

      switch (event->header.command) {
         case Protocol::COMMAND_HANDSHAKE:
           {
             const uint8_t *decode_ptr = event->payload;
             size_t decode_remain = event->payload_len;

             m_session_id = decode_i64(&decode_ptr, &decode_remain);
             if (m_session_id == 0)
             HT_THROW(Error::PROTOCOL_ERROR, "Bad session id: 0");
             handler = new RequestHandlerHandshake(m_comm, m_master_ptr.get(),
                                                   m_session_id, event);

          }
          break;
      case Protocol::COMMAND_OPEN:
        handler = new RequestHandlerOpen(m_comm, m_master_ptr.get(),
                                         m_session_id, event);
        break;
      case Protocol::COMMAND_CLOSE:
        handler = new RequestHandlerClose(m_comm, m_master_ptr.get(),
                                          m_session_id, event);
        break;
      case Protocol::COMMAND_MKDIR:
        handler = new RequestHandlerMkdir(m_comm, m_master_ptr.get(),
                                          m_session_id, event);
        break;
      case Protocol::COMMAND_DELETE:
        handler = new RequestHandlerDelete(m_comm, m_master_ptr.get(),
                                           m_session_id, event);
        break;
      case Protocol::COMMAND_ATTRSET:
        handler = new RequestHandlerAttrSet(m_comm, m_master_ptr.get(),
                                            m_session_id, event);
        break;
      case Protocol::COMMAND_ATTRGET:
        handler = new RequestHandlerAttrGet(m_comm, m_master_ptr.get(),
                                            m_session_id, event);
        break;
      case Protocol::COMMAND_ATTREXISTS:
        handler = new RequestHandlerAttrExists(m_comm, m_master_ptr.get(),
                                               m_session_id, event);
        break;
      case Protocol::COMMAND_ATTRLIST:
        handler = new RequestHandlerAttrList(m_comm, m_master_ptr.get(),
                                             m_session_id, event);
        break;
      case Protocol::COMMAND_ATTRDEL:
        handler = new RequestHandlerAttrDel(m_comm, m_master_ptr.get(),
                                            m_session_id, event);
        break;
      case Protocol::COMMAND_EXISTS:
        handler = new RequestHandlerExists(m_comm, m_master_ptr.get(),
                                           m_session_id, event);
        break;
      case Protocol::COMMAND_READDIR:
        handler = new RequestHandlerReaddir(m_comm, m_master_ptr.get(),
                                            m_session_id, event);
        break;
      case Protocol::COMMAND_LOCK:
        handler = new RequestHandlerLock(m_comm, m_master_ptr.get(),
                                         m_session_id, event);
        break;
      case Protocol::COMMAND_RELEASE:
        handler = new RequestHandlerRelease(m_comm, m_master_ptr.get(),
                                            m_session_id, event);
        break;
      case Protocol::COMMAND_STATUS:
        handler = new RequestHandlerStatus(m_comm, m_master_ptr.get(),
                                           m_session_id, event);
        break;
      default:
        HT_THROWF(Error::PROTOCOL_ERROR, "Unimplemented command (%llu)",
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
  else if (event->type == Hypertable::Event::CONNECTION_ESTABLISHED) {
    HT_INFOF("%s", event->to_str().c_str());
  }
  else if (event->type == Hypertable::Event::DISCONNECT) {
    m_master_ptr->destroy_session(m_session_id);
    cout << flush;
  }
  else {
    HT_INFOF("%s", event->to_str().c_str());
  }

}
