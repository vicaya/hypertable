/** -*- c++ -*-
 * Copyright (C) 2010 Doug Judd (Hypertable, Inc.)
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

#ifndef HYPERTABLE_REQUESTHANDLERGROUPCOMMIT_H
#define HYPERTABLE_REQUESTHANDLERGROUPCOMMIT_H

#include "Common/Runnable.h"

#include "AsyncComm/ApplicationHandler.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/Event.h"


namespace Hypertable {

  class RangeServer;

  class RequestHandlerGroupCommit : public ApplicationHandler {
  public:
    RequestHandlerGroupCommit(Comm *comm, RangeServer *rs)
      : ApplicationHandler(true), m_comm(comm), m_range_server(rs) { m_urgent=true; }

    virtual void run();

  private:
    Comm        *m_comm;
    RangeServer *m_range_server;
  };

}

#endif // HYPERTABLE_REQUESTHANDLERGROUPCOMMIT_H
