/**
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
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

#ifndef HYPERTABLE_REQUESTHANDLERRENAME_H
#define HYPERTABLE_REQUESTHANDLERRENAME_H

#include "Common/Runnable.h"

#include "AsyncComm/ApplicationHandler.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/Event.h"

#include "Broker.h"

namespace Hypertable { namespace DfsBroker {

    class RequestHandlerRename : public ApplicationHandler {
    public:
      RequestHandlerRename(Comm *comm, Broker *broker, EventPtr &event) :
          ApplicationHandler(event), m_comm(comm), m_broker(broker) {}

      virtual void run();

    private:
      Comm   *m_comm;
      Broker *m_broker;
    };

}} // namespace Hypertable::DfsBroker

#endif // HYPERTABLE_REQUESTHANDLERRENAME_H
