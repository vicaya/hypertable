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
#include "Common/Config.h"
#include "Common/Math.h"
#include "Common/ScopeGuard.h"
#include "Common/StringExt.h"

#include <iostream>

#include "Global.h"
#include "MaintenanceFlag.h"
#include "MaintenancePrioritizerLowMemory.h"

using namespace Hypertable;
using namespace Hypertable::Config;
using namespace std;

void
MaintenancePrioritizerLowMemory::prioritize(RangeStatsVector &range_data,
                                            MemoryState &memory_state,
                                            String &trace_str) {
  RangeStatsVector range_data_root;
  RangeStatsVector range_data_metadata;
  RangeStatsVector range_data_user;
  int32_t priority = 1;

  for (size_t i=0; i<range_data.size(); i++) {
    if (range_data[i]->range->is_root())
      range_data_root.push_back(range_data[i]);
    else if (range_data[i]->table_id == 0)
      range_data_metadata.push_back(range_data[i]);
    else
      range_data_user.push_back(range_data[i]);
  }

  m_cellstore_minimum_size = get_i64("Hypertable.RangeServer.CellStore.TargetSize.Minimum");

  /**
   * Assign priority for ROOT range
   */
  if (!range_data_root.empty())
    assign_priorities_all(range_data_root, Global::root_log,
                          Global::log_prune_threshold_min,
                          memory_state, priority, trace_str);


  /**
   * Assign priority for METADATA ranges
   */
  if (!range_data_metadata.empty())
    assign_priorities_all(range_data_metadata, Global::metadata_log,
                          Global::log_prune_threshold_min, memory_state,
                          priority, trace_str);


  /**
   * Assign priority for USER ranges
   */
  int64_t prune_threshold = (int64_t)(m_server_stats->get_update_mbps() * (double)Global::log_prune_threshold_max);

  if (prune_threshold < Global::log_prune_threshold_min)
    prune_threshold = Global::log_prune_threshold_min;
  else if (prune_threshold > Global::log_prune_threshold_max)
    prune_threshold = Global::log_prune_threshold_max;

  trace_str += String("STATS user log prune threshold\t") + prune_threshold + "\n";

  if (!range_data_user.empty()) {
    assign_priorities_all(range_data_user, Global::user_log, prune_threshold,
                          memory_state, priority, trace_str);
    assign_priorities_user(range_data_user, memory_state, priority, trace_str);
  }


}


/**
 * Memory freeing algorithm:
 *
 * 1. schedule in-progress splits
 * 2. schedule needed splits
 * 3. schedule needed compactions
 */
void
MaintenancePrioritizerLowMemory::assign_priorities_all(RangeStatsVector &range_data,
            CommitLog *log, int64_t prune_threshold, MemoryState &memory_state,
	    int32_t &priority, String &trace_str) {

  if (!schedule_inprogress_splits(range_data, memory_state, priority, trace_str))
    return;

  if (!schedule_splits(range_data, memory_state, priority, trace_str))
    return;

  if (!schedule_necessary_compactions(range_data, log, prune_threshold,
                                      memory_state, priority, trace_str))
    return;

}


/**
 * Memory freeing algorithm:
 *
 * 1. purge shadow caches
 * 
 * if (READ heavy)
 *   2. compact remaining cell caches
 *   3. shrink block cache
 *   4. purge cell store indexes
 * else if (WRITE heavy)
 *   2. shrink block cache
 *   3. purge cell store indexes
 *   4. compact remaining cell caches
 * else
 *   2. shrink block cache
 *   3. compact remaining cell caches
 *   4. purge cell store indexes
 *
 */

void
MaintenancePrioritizerLowMemory::assign_priorities_user(RangeStatsVector &range_data,
                  MemoryState &memory_state, int32_t &priority, String &trace_str) {
  uint64_t update_bytes = m_server_stats->get_update_bytes();
  uint32_t scan_count = m_server_stats->get_scan_count();

  if (!purge_shadow_caches(range_data, memory_state, priority, trace_str))
    return;

  if (update_bytes < 500000 && scan_count > 10) {

    // READ heavy

    if (!compact_cellcaches(range_data, memory_state, priority, trace_str))
      return;

    Global::block_cache->cap_memory_use();
    memory_state.decrement_needed( Global::block_cache->decrease_limit(memory_state.needed) );
    if (!memory_state.need_more())
      return;

    if (!purge_cellstore_indexes(range_data, memory_state, priority, trace_str))
      return;

  }
  else if (m_server_stats->get_update_mbps() > 0.5 && scan_count < 5) {

    // WRITE heavy 

    Global::block_cache->cap_memory_use();
    memory_state.decrement_needed( Global::block_cache->decrease_limit(memory_state.needed) );
    if (!memory_state.need_more())
      return;

    if (!purge_cellstore_indexes(range_data, memory_state, priority, trace_str))
      return;

    if (!compact_cellcaches(range_data, memory_state, priority, trace_str))
      return;

  }
  else {

    Global::block_cache->cap_memory_use();
    memory_state.decrement_needed( Global::block_cache->decrease_limit(memory_state.needed) );
    if (!memory_state.need_more())
      return;

    if (!compact_cellcaches(range_data, memory_state, priority, trace_str))
      return;

    if (!purge_cellstore_indexes(range_data, memory_state, priority, trace_str))
      return;

  }
  
}
