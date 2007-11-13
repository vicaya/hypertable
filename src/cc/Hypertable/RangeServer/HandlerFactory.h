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

#ifndef HYPERTABLE_HANDLERFACTORY_H
#define HYPERTABLE_HANDLERFACTORY_H

#include "AsyncComm/ApplicationQueue.h"

#include "ConnectionHandler.h"
#include "RangeServer.h"


namespace Hypertable {

  class Comm;

  /**
   *
   */
  class HandlerFactory : public ConnectionHandlerFactory {
  public:
    HandlerFactory(Comm *comm, ApplicationQueuePtr &appQueuePtr, RangeServerPtr rangeServerPtr) : m_comm(comm), m_app_queue_ptr(appQueuePtr), m_range_server_ptr(rangeServerPtr) { return; }
    virtual void get_instance(DispatchHandlerPtr &dhp) {
      dhp = new ConnectionHandler(m_comm, m_app_queue_ptr, m_range_server_ptr);
    }
  private:
    Comm                *m_comm;
    ApplicationQueuePtr  m_app_queue_ptr;
    RangeServerPtr       m_range_server_ptr;
  };

}

#endif // HYPERTABLE_HANDLERFACTORY_H
