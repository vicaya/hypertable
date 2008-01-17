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

#include <cassert>

#include "AsyncComm/ApplicationQueue.h"

#include "Global.h"

using namespace Hypertable;

namespace Hypertable {

  SessionPtr             Global::hyperspace_ptr = 0;
  Filesystem            *Global::dfs = 0;
  Filesystem            *Global::logDfs = 0;
  MaintenanceQueue      *Global::maintenance_queue = 0;
  RangeServerProtocol   *Global::protocol = 0;
  bool                   Global::verbose = false;
  CommitLog             *Global::log = 0;
  std::string            Global::logDir = "";
  uint64_t               Global::rangeMaxBytes = 0;
  int32_t                Global::localityGroupMaxFiles = 0;
  int32_t                Global::localityGroupMergeFiles = 0;
  int32_t                Global::localityGroupMaxMemory = 0;
  ScannerMap             Global::scannerMap;
  FileBlockCache        *Global::blockCache = 0;
  TablePtr               Global::metadata_table_ptr = 0;
  uint64_t               Global::range_metadata_max_bytes = 0;
  MemoryTracker          Global::memory_tracker;

}
