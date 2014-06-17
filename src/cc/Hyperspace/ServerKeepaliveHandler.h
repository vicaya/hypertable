/**
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERSPACE_SERVERKEEPALIVEHANDLER_H
#define HYPERSPACE_SERVERKEEPALIVEHANDLER_H

#include <boost/shared_ptr.hpp>

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/DispatchHandler.h"

#include "Event.h"

namespace Hyperspace {

  class Master;

  /**
   */
  class ServerKeepaliveHandler : public DispatchHandler {
  public:
    ServerKeepaliveHandler(Comm *comm, Master *master,
                           ApplicationQueuePtr &app_queue_ptr);
    virtual void handle(Hypertable::EventPtr &event_ptr);
    void deliver_event_notifications(uint64_t session_id);

  private:
    Comm              *m_comm;
    Master            *m_master;
    struct sockaddr_in m_send_addr;
    ApplicationQueuePtr m_app_queue_ptr;
  };
  typedef boost::shared_ptr<ServerKeepaliveHandler> ServerKeepaliveHandlerPtr;
}

#endif // HYPERSPACE_SERVERKEEPALIVEHANDLER_H

