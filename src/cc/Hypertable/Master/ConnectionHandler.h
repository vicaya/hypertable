/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
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

#ifndef HYPERTABLE_CONNECTIONHANDLER_H
#define HYPERTABLE_CONNECTIONHANDLER_H

#include "AsyncComm/DispatchHandler.h"

#include "Context.h"

namespace Hypertable {

  /**
   */
  class ConnectionHandler2 : public DispatchHandler {
  public:
    ConnectionHandler2(ContextPtr &context);
    virtual void handle(EventPtr &event);

  private:
    ContextPtr  m_context;
    //int64_t m_last_maintenance;
  };

}

#endif // HYPERTABLE_CONNECTIONHANDLER_H

