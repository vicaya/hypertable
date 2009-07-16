/** -*- C++ -*-
 * Copyright (C) 2009  Luke Lu (llu@hypertable.org)
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#ifndef HYPERTABLE_TABLEMUTATOR_FLUSH_HANDLER_H
#define HYPERTABLE_TABLEMUTATOR_FLUSH_HANDLER_H

#include "AsyncComm/ApplicationHandler.h"

#include "TableMutatorIntervalHandler.h"

namespace Hypertable {

struct TableMutatorFlushHandler: ApplicationHandler {
  TableMutatorFlushHandler(TableMutatorIntervalHandler *interval_handler,
                           EventPtr &event)
    : ApplicationHandler(event), interval_handler(interval_handler) { }

  virtual void run();

  TableMutatorIntervalHandlerPtr interval_handler;
};


} // namespace Hypertable

#endif /* HYPERTABLE_TABLEMUTATOR_FLUSH_HANDLER_H */
