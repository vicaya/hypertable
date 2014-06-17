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
#include "Common/Config.h"
#include "Common/Error.h"
#include "Common/StringExt.h"

#include "AsyncComm/ApplicationQueue.h"

#include "Protocol.h"
#include "RequestHandlerAttrSet.h"
#include "RequestHandlerAttrGet.h"
#include "RequestHandlerAttrIncr.h"
#include "RequestHandlerAttrDel.h"
#include "RequestHandlerAttrExists.h"
#include "RequestHandlerAttrList.h"
#include "RequestHandlerMkdir.h"
#include "RequestHandlerDelete.h"
#include "RequestHandlerOpen.h"
#include "RequestHandlerClose.h"
#include "RequestHandlerExists.h"
#include "RequestHandlerReaddir.h"
#include "RequestHandlerReaddirAttr.h"
#include "RequestHandlerReadpathAttr.h"
#include "RequestHandlerLock.h"
#include "RequestHandlerRelease.h"
#include "RequestHandlerStatus.h"
#include "RequestHandlerHandshake.h"
#include "RequestHandlerDoMaintenance.h"
#include "RequestHandlerDestroySession.h"
#include "ServerConnectionHandler.h"

using namespace std;
using namespace Hypertable;
using namespace Hyperspace;
using namespace Serialization;
using namespace Error;

ServerConnectionHandler::ServerConnectionHandler(Comm *comm, ApplicationQueuePtr &app_queue,
    MasterPtr &master) : m_comm(comm), m_app_queue_ptr(app_queue), m_master_ptr(master),
    m_session_id(0) {
  m_maintenance_interval = Config::properties->get_i32("Hyperspace.Maintenance.Interval");
}



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

      // if this is not the current replication master then try to return
      // addr of current master
      if (!m_master_ptr->is_master())
        HT_THROW(Error::HYPERSPACE_NOT_MASTER_LOCATION, (String) "Current master=" +
            m_master_ptr->get_current_master());

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
      case Protocol::COMMAND_ATTRINCR:
        handler = new RequestHandlerAttrIncr(m_comm, m_master_ptr.get(),
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
      case Protocol::COMMAND_READDIRATTR:
        handler = new RequestHandlerReaddirAttr(m_comm, m_master_ptr.get(),
                                                m_session_id, event);
        break;
      case Protocol::COMMAND_READPATHATTR:
        handler = new RequestHandlerReadpathAttr(m_comm, m_master_ptr.get(),
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
    m_app_queue_ptr->add( new RequestHandlerDestroySession(m_comm, m_master_ptr.get(), m_session_id) );
    cout << flush;
  }
  else if (event->type == Hypertable::Event::TIMER) {
    int error;
    try {
      m_app_queue_ptr->add(new Hyperspace::RequestHandlerDoMaintenance(m_comm,
          m_master_ptr.get(), event) );
    }
    catch (Exception &e) {
      HT_ERROR_OUT << e << HT_END;
    }

    if ((error = m_comm->set_timer(m_maintenance_interval, this)) != Error::OK)
       HT_FATALF("Problem setting timer - %s", Error::get_text(error));

  }

  else {
    HT_INFOF("%s", event->to_str().c_str());
  }

}
