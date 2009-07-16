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

#ifndef HYPERTABLE_TABLEMUTATOR_INTERVAL_HANDLER_H
#define HYPERTABLE_TABLEMUTATOR_INTERVAL_HANDLER_H

#include "Common/Mutex.h"

#include "AsyncComm/DispatchHandler.h"

namespace Hypertable {

class Comm;
class TableMutatorShared;

struct TableMutatorIntervalHandler : DispatchHandler {
  TableMutatorIntervalHandler(Comm *comm, ApplicationQueue *app_queue,
                              TableMutatorShared *mutator)
    : active(true), comm(comm), app_queue(app_queue), mutator(mutator) {
    self_register();
  }

  virtual void handle(EventPtr &);

  void flush();

  void self_register();

  void stop() {
    ScopedLock lock(mutex);
    active = false;
  }

  bool stopped() {
    ScopedLock lock(mutex);
    return !active;
  }

  Mutex               mutex;
  bool                active;
  Comm               *comm;
  ApplicationQueue   *app_queue;
  TableMutatorShared *mutator;
};

typedef intrusive_ptr<TableMutatorIntervalHandler>
        TableMutatorIntervalHandlerPtr;

} // namespace Hypertable

#endif /* HYPERTABLE_TABLEMUTATOR_INTERVAL_HANDLER_H */
