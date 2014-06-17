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

#ifndef HYPERTABLE_TABLESCANNERDISPATCHHANDLER_H
#define HYPERTABLE_TABLESCANNERDISPATCHHANDLER_H

#include "Common/Compat.h"
#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/DispatchHandler.h"
#include "AsyncComm/Event.h"

namespace Hypertable {

  class TableScannerAsync;
  /**
   *
   */
  class TableScannerDispatchHandler : public DispatchHandler {

  public:
    /**
     * Constructor.  Initializes state.
     */
    TableScannerDispatchHandler(ApplicationQueuePtr &app_queue, TableScannerAsync *scanner,
                                int m_interval_scanner, bool is_create);

    /**
     * Dispatch method.  This gets called by the AsyncComm layer
     * when an event occurs in response to a previously sent
     * request that was supplied with this dispatch handler.
     *
     * @param event_ptr shared pointer to event object
     */
    virtual void handle(EventPtr &event_ptr);

  private:
    ApplicationQueuePtr     m_app_queue;
    TableScannerAsync      *m_scanner;
    int                     m_interval_scanner;
    bool                    m_is_create;
  };
}


#endif // HYPERTABLE_TABLESCANNERDISPATCHHANDLER_H
