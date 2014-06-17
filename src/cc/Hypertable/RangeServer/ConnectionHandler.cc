/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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
#include <iostream>

extern "C" {
#include <poll.h>
}

#include "Common/Error.h"
#include "Common/StringExt.h"
#include "Common/Serialization.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/ResponseCallback.h"

#include "Hypertable/Lib/RangeServerProtocol.h"

#include "RequestHandlerAcknowledgeLoad.h"
#include "RequestHandlerCompact.h"
#include "RequestHandlerDestroyScanner.h"
#include "RequestHandlerDump.h"
#include "RequestHandlerGetStatistics.h"
#include "RequestHandlerLoadRange.h"
#include "RequestHandlerUpdateSchema.h"
#include "RequestHandlerUpdate.h"
#include "RequestHandlerCreateScanner.h"
#include "RequestHandlerFetchScanblock.h"
#include "RequestHandlerDropTable.h"
#include "RequestHandlerStatus.h"
#include "RequestHandlerReplayBegin.h"
#include "RequestHandlerReplayLoadRange.h"
#include "RequestHandlerReplayUpdate.h"
#include "RequestHandlerReplayCommit.h"
#include "RequestHandlerDropRange.h"
#include "RequestHandlerClose.h"
#include "RequestHandlerCommitLogSync.h"
#include "RequestHandlerWaitForMaintenance.h"

#include "ConnectionHandler.h"
#include "RangeServer.h"


using namespace Hypertable;
using namespace Serialization;
using namespace Error;

/**
 *
 */
ConnectionHandler::ConnectionHandler(Comm *comm, ApplicationQueuePtr &app_queue,
                                     RangeServerPtr range_server)
  : m_comm(comm), m_app_queue_ptr(app_queue), m_range_server_ptr(range_server),
    m_shutdown(false) {
}


/**
 *
 */
void ConnectionHandler::handle(EventPtr &event) {

  if (m_shutdown &&
      event->header.command != RangeServerProtocol::COMMAND_SHUTDOWN &&
      event->header.command != RangeServerProtocol::COMMAND_STATUS) {
    ResponseCallback cb(m_comm, event);
    cb.error(RANGESERVER_SHUTTING_DOWN, "");
    return;
  }

  if (event->type == Event::MESSAGE) {
    ApplicationHandler *handler = 0;

    //event->display();

    try {

      // sanity check command code
      if (event->header.command < 0
          || event->header.command >= RangeServerProtocol::COMMAND_MAX)
        HT_THROWF(PROTOCOL_ERROR, "Invalid command (%llu)",
                  (Llu)event->header.command);

      switch (event->header.command) {
      case RangeServerProtocol::COMMAND_ACKNOWLEDGE_LOAD:
        handler = new RequestHandlerAcknowledgeLoad(m_comm, m_range_server_ptr.get(),
                                                    event);
        break;
      case RangeServerProtocol::COMMAND_COMPACT:
        handler = new RequestHandlerCompact(m_comm, m_range_server_ptr.get(),
                                            event);
        break;
      case RangeServerProtocol::COMMAND_LOAD_RANGE:
        handler = new RequestHandlerLoadRange(m_comm, m_range_server_ptr.get(),
                                              event);
        break;
      case RangeServerProtocol::COMMAND_UPDATE:
        handler = new RequestHandlerUpdate(m_comm, m_range_server_ptr.get(),
                                           event);
        break;
      case RangeServerProtocol::COMMAND_CREATE_SCANNER:
        handler = new RequestHandlerCreateScanner(m_comm,
            m_range_server_ptr.get(), event);
        break;
      case RangeServerProtocol::COMMAND_DESTROY_SCANNER:
        handler = new RequestHandlerDestroyScanner(m_comm,
            m_range_server_ptr.get(), event);
        break;
      case RangeServerProtocol::COMMAND_FETCH_SCANBLOCK:
        handler = new RequestHandlerFetchScanblock(m_comm,
            m_range_server_ptr.get(), event);
        break;
      case RangeServerProtocol::COMMAND_DROP_TABLE:
        handler = new RequestHandlerDropTable(m_comm, m_range_server_ptr.get(),
                                              event);
        break;
      case RangeServerProtocol::COMMAND_REPLAY_BEGIN:
        handler = new RequestHandlerReplayBegin(m_comm,
            m_range_server_ptr.get(), event);
        break;
      case RangeServerProtocol::COMMAND_REPLAY_LOAD_RANGE:
        handler = new RequestHandlerReplayLoadRange(m_comm,
            m_range_server_ptr.get(), event);
        break;
      case RangeServerProtocol::COMMAND_REPLAY_UPDATE:
        handler = new RequestHandlerReplayUpdate(m_comm,
            m_range_server_ptr.get(), event);
        break;
      case RangeServerProtocol::COMMAND_REPLAY_COMMIT:
        handler = new RequestHandlerReplayCommit(m_comm,
            m_range_server_ptr.get(), event);
        break;
      case RangeServerProtocol::COMMAND_DROP_RANGE:
        handler = new RequestHandlerDropRange(m_comm, m_range_server_ptr.get(),
                                              event);
        break;
      case RangeServerProtocol::COMMAND_STATUS:
        handler = new RequestHandlerStatus(m_comm, m_range_server_ptr.get(),
                                           event);
        break;
      case RangeServerProtocol::COMMAND_CLOSE:
        m_shutdown = true;
        handler = new RequestHandlerClose(m_comm, m_range_server_ptr.get(),
                                             event);
        break;
      case RangeServerProtocol::COMMAND_WAIT_FOR_MAINTENANCE:
        handler = new RequestHandlerWaitForMaintenance(m_comm, m_range_server_ptr.get(), event);
        break;
      case RangeServerProtocol::COMMAND_SHUTDOWN:
        HT_INFO("Received shutdown command");
        m_shutdown = true;
        m_range_server_ptr->shutdown();
        m_range_server_ptr = 0;
        m_app_queue_ptr->shutdown();
        return;
      case RangeServerProtocol::COMMAND_DUMP:
        handler = new RequestHandlerDump(m_comm, m_range_server_ptr.get(),
                                         event);
        break;
      case RangeServerProtocol::COMMAND_GET_STATISTICS:
        handler = new RequestHandlerGetStatistics(m_comm,
            m_range_server_ptr.get(), event);
        break;
      case RangeServerProtocol::COMMAND_UPDATE_SCHEMA:
        handler = new RequestHandlerUpdateSchema(m_comm,
            m_range_server_ptr.get(), event);
        break;
      case RangeServerProtocol::COMMAND_COMMIT_LOG_SYNC:
        handler = new RequestHandlerCommitLogSync(m_comm, m_range_server_ptr.get(), event);
        break;
      default:
        HT_THROWF(PROTOCOL_ERROR, "Unimplemented command (%llu)",
                  (Llu)event->header.command);
      }
      m_app_queue_ptr->add(handler);
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

