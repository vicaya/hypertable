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

#include "Common/Properties.h"
#include "Common/Filesystem.h"

#include "AsyncComm/Comm.h"
#include "Hyperspace/Session.h"
#include "Hypertable/Lib/CommitLog.h"
#include "Hypertable/Lib/MetaLogWriter.h"
#include "Hypertable/Lib/RangeServerClient.h"
#include "Hypertable/Lib/RangeServerProtocol.h"
#include "Hypertable/Lib/Schema.h"
#include "Hypertable/Lib/Client.h"
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
    static Hyperspace::SessionPtr hyperspace;
    static Hypertable::FilesystemPtr dfs;
    static Hypertable::FilesystemPtr log_dfs;
    static Hypertable::MaintenanceQueuePtr maintenance_queue;
    static Hypertable::RangeServerProtocol *protocol;
    static bool           verbose;
    static CommitLog     *user_log;
    static CommitLog     *system_log;
    static CommitLog     *metadata_log;
    static CommitLog     *root_log;
    static MetaLog::WriterPtr rsml_writer;
    static std::string    log_dir;
    static int64_t        range_split_size;
    static int64_t        range_maximum_size;
    static int32_t        access_group_garbage_compaction_threshold;
    static int32_t        access_group_max_files;
    static int32_t        access_group_merge_files;
    static int32_t        access_group_max_mem;
    static int32_t        cell_cache_scanner_cache_size;
    static ScannerMap     scanner_map;
    static Hypertable::FileBlockCache *block_cache;
    static TablePtr       metadata_table;
    static TablePtr       rs_metrics_table;
    static int64_t        range_metadata_split_size;
    static Hypertable::MemoryTracker *memory_tracker;
    static int64_t        log_prune_threshold_min;
    static int64_t        log_prune_threshold_max;
    static int64_t        memory_limit;
    static uint64_t       access_counter;
    static bool           enable_shadow_cache;
    static std::string    toplevel_dir;
    static int32_t        metrics_interval;
  };

} // namespace Hypertable

#endif // HYPERTABLE_RANGESERVER_GLOBAL_H
