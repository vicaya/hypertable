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

#ifndef HYPERTABLE_TABLEMUTATORDISPATCHHANDLER_H
#define HYPERTABLE_TABLEMUTATORDISPATCHHANDLER_H

#include "AsyncComm/DispatchHandler.h"
#include "AsyncComm/Event.h"

#include "TableMutatorScatterBuffer.h"
#include "TableMutatorSendBuffer.h"

namespace Hypertable {

  /**
   * This class is a DispatchHandler class that can be used
   *
   */
  class TableMutatorDispatchHandler : public DispatchHandler {

  public:
    /**
     * Constructor.  Initializes state.
     */
    TableMutatorDispatchHandler(TableMutatorSendBuffer *send_buffer, bool refresh_schema);

    /**
     * Dispatch method.  This gets called by the AsyncComm layer
     * when an event occurs in response to a previously sent
     * request that was supplied with this dispatch handler.
     *
     * @param event_ptr shared pointer to event object
     */
    virtual void handle(EventPtr &event_ptr);

  private:
    TableMutatorSendBuffer *m_send_buffer;
    bool                    m_refresh_schema;
  };
}


#endif // HYPERTABLE_TABLEMUTATORDISPATCHHANDLER_H
