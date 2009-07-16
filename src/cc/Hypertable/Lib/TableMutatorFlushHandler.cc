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
#include "TableMutatorFlushHandler.h"

using namespace Hypertable;


void TableMutatorFlushHandler::run() {
  try {
    interval_handler->flush();
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
  }
}
