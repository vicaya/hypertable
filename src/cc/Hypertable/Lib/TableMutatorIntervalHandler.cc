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

#include "Common/Compat.h"

#include "AsyncComm/Comm.h"

#include "TableMutatorShared.h"
#include "TableMutatorIntervalHandler.h"
#include "TableMutatorFlushHandler.h"

using namespace Hypertable;


void TableMutatorIntervalHandler::handle(EventPtr &event) {

  if (active) {
    app_queue->add(new TableMutatorFlushHandler(this, event));
    self_register();
  }
}


void TableMutatorIntervalHandler::self_register() {
  HT_ASSERT(comm->set_timer(mutator->flush_interval(), this) == Error::OK);
}

void TableMutatorIntervalHandler::flush() {
  ScopedLock lock(mutex);

  if (active)
    mutator->interval_flush();
}
