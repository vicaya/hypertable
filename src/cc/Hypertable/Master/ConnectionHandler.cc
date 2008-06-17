/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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
#include "Common/Serialization.h"

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
using namespace Serialization;
using namespace Error;

/**
 *
 */
void ConnectionHandler::handle(EventPtr &event) {
  short command = -1;

  if (event->type == Event::MESSAGE) {
    ApplicationHandler *hp = 0;
    const uint8_t *msg = event->message;
    size_t remain = event->message_len;

    //event->display()

    try {
      command = decode_i16(&msg, &remain);

      // sanity check command code
      if (command < 0 || command >= MasterProtocol::COMMAND_MAX)
        HT_THROWF(PROTOCOL_ERROR, "Invalid command (%d)", command);

      switch (command) {
      case MasterProtocol::COMMAND_CREATE_TABLE:
        hp = new RequestHandlerCreateTable(m_comm, m_master_ptr.get(), event);
        break;
      case MasterProtocol::COMMAND_DROP_TABLE:
        hp = new RequestHandlerDropTable(m_comm, m_master_ptr.get(), event);
        break;
      case MasterProtocol::COMMAND_GET_SCHEMA:
        hp = new RequestHandlerGetSchema(m_comm, m_master_ptr.get(), event);
        break;
      case MasterProtocol::COMMAND_STATUS:
        hp = new RequestHandlerStatus(m_comm, m_master_ptr.get(), event);
        break;
      case MasterProtocol::COMMAND_REGISTER_SERVER:
        hp = new RequestHandlerRegisterServer(m_comm, m_master_ptr.get(), event);
        break;
      case MasterProtocol::COMMAND_REPORT_SPLIT:
        hp = new RequestHandlerReportSplit(m_comm, m_master_ptr.get(), event);
        break;
      default:
        HT_THROWF(PROTOCOL_ERROR, "Unimplemented command (%d)", command);
      }
      m_app_queue_ptr->add(hp);
    }
    catch (Exception &e) {
      ResponseCallback cb(m_comm, event);
      HT_ERROR_OUT << e << HT_END;
      std::string errmsg = format("%s - %s", e.what(), get_text(e.code()));
      cb.error(Error::PROTOCOL_ERROR, errmsg);
    }
  }
  else {
    HT_INFOF("%s", event->to_str().c_str());
  }

}
