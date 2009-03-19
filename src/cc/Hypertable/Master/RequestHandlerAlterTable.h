/** -*- c++ -*-
 * Copyright (C) 2009 Sanjit Jhala(Zvents, Inc.)
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

#ifndef HYPERTABLE_REQUESTHANDLERALTERTABLE_H
#define HYPERTABLE_REQUESTHANDLERALTERTABLE_H

#include "Common/Runnable.h"

#include "AsyncComm/ApplicationHandler.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/Event.h"


namespace Hypertable {

  class Master;

  class RequestHandlerAlterTable : public ApplicationHandler {
  public:
    RequestHandlerAlterTable(Comm *comm, Master *master,
        EventPtr &event_ptr)
      : ApplicationHandler(event_ptr), m_comm(comm), m_master(master) { }

    virtual void run();

  private:
    Comm     *m_comm;
    Master   *m_master;
  };

}

#endif // HYPERTABLE_REQUESTHANDLERALTERTABLE_H
