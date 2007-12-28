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
#ifndef HYPERTABLE_RANGESERVER_GLOBAL_H
#define HYPERTABLE_RANGESERVER_GLOBAL_H

#include <string>

#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include "Common/Properties.h"
#include "AsyncComm/Comm.h"
#include "Hyperspace/Session.h"
#include "Hypertable/Lib/CommitLog.h"
#include "Hypertable/Lib/RangeServerClient.h"
#include "Hypertable/Lib/RangeServerProtocol.h"
#include "Hypertable/Lib/Schema.h"
#include "Hypertable/Lib/Filesystem.h"
#include "Hypertable/Lib/Table.h"
#include "Hypertable/Lib/Types.h"

#include "FileBlockCache.h"
#include "MemoryTracker.h"
#include "ScannerMap.h"
#include "TableInfo.h"

namespace Hypertable {

  class ApplicationQueue;

  class Global {
  public:
    static Hypertable::Filesystem *dfs;
    static Hypertable::Filesystem *logDfs;
    static boost::thread *maintenanceThreadPtr;
    static Hypertable::RangeServerProtocol *protocol;
    static bool           verbose;
    static CommitLog     *log;
    static std::string    logDir;
    static uint64_t       rangeMaxBytes;
    static int32_t        localityGroupMaxFiles;
    static int32_t        localityGroupMergeFiles;
    static int32_t        localityGroupMaxMemory;
    static ScannerMap     scannerMap;
    static Hypertable::FileBlockCache *blockCache;
    static TablePtr       metadata_table_ptr;
    static uint64_t       range_metadata_max_bytes;
    static Hypertable::MemoryTracker memory_tracker;
  };
}


#endif // HYPERTABLE_RANGESERVER_GLOBAL_H

