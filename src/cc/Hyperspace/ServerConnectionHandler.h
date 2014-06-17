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

#ifndef HYPERSPACE_SERVERCONNECTIONHANDLER_H
#define HYPERSPACE_SERVERCONNECTIONHANDLER_H

#include "Common/Compat.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/DispatchHandler.h"

#include "Master.h"

namespace Hyperspace {

  /**
   *
   */
  class ServerConnectionHandler : public DispatchHandler {
  public:
    ServerConnectionHandler(Comm *comm, ApplicationQueuePtr &app_queue, MasterPtr &master);

    virtual void handle(EventPtr &event_ptr);

  private:
    Comm                *m_comm;
    ApplicationQueuePtr  m_app_queue_ptr;
    MasterPtr            m_master_ptr;
    uint64_t             m_session_id;
    uint32_t             m_maintenance_interval;
  };

}

#endif // HYPERSPACE_SERVERCONNECTIONHANDLER_H
