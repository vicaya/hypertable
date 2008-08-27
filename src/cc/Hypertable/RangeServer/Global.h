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

#ifndef HYPERTABLE_RANGESERVER_GLOBAL_H
#define HYPERTABLE_RANGESERVER_GLOBAL_H

#include <string>

#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include "Common/CrashTest.h"
#include "Common/Properties.h"
#include "AsyncComm/Comm.h"
#include "Hyperspace/Session.h"
#include "Hypertable/Lib/CommitLog.h"
#include "Hypertable/Lib/RangeServerClient.h"
#include "Hypertable/Lib/RangeServerMetaLog.h"
#include "Hypertable/Lib/RangeServerProtocol.h"
#include "Hypertable/Lib/Schema.h"
#include "Hypertable/Lib/Filesystem.h"
#include "Hypertable/Lib/Table.h"
#include "Hypertable/Lib/Types.h"

#include "FileBlockCache.h"
#include "MaintenanceQueue.h"
#include "MemoryTracker.h"
#include "ScannerMap.h"
#include "TableInfo.h"

namespace Hypertable {

  class ApplicationQueue;

  class Global {
  public:
    static Hyperspace::SessionPtr hyperspace_ptr;
    static Hypertable::Filesystem *dfs;
    static Hypertable::Filesystem *log_dfs;
    static Hypertable::MaintenanceQueue *maintenance_queue;
    static Hypertable::RangeServerProtocol *protocol;
    static bool           verbose;
    static CommitLog     *user_log;
    static CommitLog     *metadata_log;
    static CommitLog     *root_log;
    static Hypertable::RangeServerMetaLog *range_log;
    static std::string    log_dir;
    static uint64_t       range_max_bytes;
    static int32_t        access_group_max_files;
    static int32_t        access_group_merge_files;
    static int32_t        access_group_max_mem;
    static ScannerMap     scanner_map;
    static Hypertable::FileBlockCache *block_cache;
    static TablePtr       metadata_table_ptr;
    static uint64_t       range_metadata_max_bytes;
    static Hypertable::MemoryTracker memory_tracker;
    static uint64_t       log_prune_threshold_min;
    static uint64_t       log_prune_threshold_max;
    static Hypertable::CrashTest *crash_test;
  };
}


#endif // HYPERTABLE_RANGESERVER_GLOBAL_H

