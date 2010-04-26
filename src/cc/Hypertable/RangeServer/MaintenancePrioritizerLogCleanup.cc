/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
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
#include "Common/Compat.h"
#include "Common/Math.h"
#include "Common/StringExt.h"

#include <iostream>

#include "Global.h"
#include "MaintenanceFlag.h"
#include "MaintenancePrioritizerLogCleanup.h"

using namespace Hypertable;
using namespace std;

void
MaintenancePrioritizerLogCleanup::prioritize(RangeStatsVector &range_data,
                                             MemoryState &memory_state,
                                             String &trace_str) {
  RangeStatsVector range_data_root;
  RangeStatsVector range_data_metadata;
  RangeStatsVector range_data_user;
  int32_t priority = 1;
  int collector_id = RSStats::STATS_COLLECTOR_MAINTENANCE;

  for (size_t i=0; i<range_data.size(); i++) {
    if (range_data[i]->range->is_root())
      range_data_root.push_back(range_data[i]);
    else if (range_data[i]->table_id == 0)
      range_data_metadata.push_back(range_data[i]);
    else
      range_data_user.push_back(range_data[i]);
  }


  /**
   * Assign priority for ROOT range
   */
  if (!range_data_root.empty())
    assign_priorities(range_data_root, Global::root_log,
		      Global::log_prune_threshold_min,
                      memory_state, priority, trace_str);


  /**
   * Assign priority for METADATA ranges
   */
  if (!range_data_metadata.empty())
    assign_priorities(range_data_metadata, Global::metadata_log,
                      Global::log_prune_threshold_min,
                      memory_state, priority, trace_str);


  /**
   * Assign priority for USER ranges
   */
  int64_t prune_threshold = (int64_t)(m_server_stats->get_update_mbps(collector_id) * (double)Global::log_prune_threshold_max);

  if (prune_threshold < Global::log_prune_threshold_min)
    prune_threshold = Global::log_prune_threshold_min;
  else if (prune_threshold > Global::log_prune_threshold_max)
    prune_threshold = Global::log_prune_threshold_max;

  trace_str += String("STATS user log prune threshold\t") + prune_threshold + "\n";

  if (!range_data_user.empty())
    assign_priorities(range_data_user, Global::user_log, prune_threshold,
                      memory_state, priority, trace_str);

  /**
   *  If there is no update activity, or there is little update activity and
   *  scan activity, then increase the block cache size
   */
  if (m_server_stats->get_update_bytes(collector_id) == 0 ||
      (m_server_stats->get_update_bytes(collector_id) < 1000000 &&
       m_server_stats->get_scan_count(collector_id) > 20)) {
    if (memory_state.balance < memory_state.limit) {
      int64_t available = memory_state.limit - memory_state.balance;
      int64_t block_cache_available = Global::block_cache->available();
      if (block_cache_available < available) {
        HT_INFOF("Increasing block cache limit by %lld",
                 (Lld)available - block_cache_available);
        Global::block_cache->increase_limit(available - block_cache_available);
      }
    }
  }

}


/**
 * 1. schedule in-progress splits
 * 2. schedule splits
 * 3. schedule compactions for log cleanup purposes
 */
void
MaintenancePrioritizerLogCleanup::assign_priorities(RangeStatsVector &range_data,
              CommitLog *log, int64_t prune_threshold, MemoryState &memory_state,
              int32_t &priority, String &trace_str) {

  /**
   * 1. Schedule in-progress splits
   */
  schedule_inprogress_splits(range_data, memory_state, priority, trace_str);

  /**
   * 2. Schedule splits
   */
  schedule_splits(range_data, memory_state, priority, trace_str);

  /**
   * 3. Schedule compactions for log cleaning purposes
   */
  schedule_necessary_compactions(range_data, log, prune_threshold,
                                 memory_state, priority, trace_str);

}
