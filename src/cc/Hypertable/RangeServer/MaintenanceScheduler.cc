/**
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
#include "Common/SystemInfo.h"

#include <algorithm>
#include <iostream>

#include "Global.h"
#include "MaintenanceFlag.h"
#include "MaintenancePrioritizerLogCleanup.h"
#include "MaintenanceScheduler.h"
#include "MaintenanceTaskCompaction.h"
#include "MaintenanceTaskMemoryPurge.h"
#include "MaintenanceTaskSplit.h"

using namespace Hypertable;
using namespace std;
using namespace Hypertable::Config;

namespace {
  struct RangeStatsAscending {
    bool operator()(const Range::MaintenanceData *x, const Range::MaintenanceData *y) const {
      return x->priority < y->priority;
    }
  };
}


MaintenanceScheduler::MaintenanceScheduler(MaintenanceQueuePtr &queue, RSStatsPtr &server_stats,
                                           RangeStatsGathererPtr &gatherer)
  : m_initialized(false), m_scheduling_needed(false), m_queue(queue),
    m_server_stats(server_stats), m_stats_gatherer(gatherer),
    m_prioritizer_log_cleanup(server_stats),
    m_prioritizer_low_memory(server_stats) {
  m_prioritizer = &m_prioritizer_log_cleanup;
  m_maintenance_interval = get_i32("Hypertable.RangeServer.Maintenance.Interval");
  m_query_cache_memory = get_i64("Hypertable.RangeServer.QueryCache.MaxMemory");
  // Setup to immediately schedule maintenance
  boost::xtime_get(&m_last_maintenance, TIME_UTC);
  m_last_maintenance.sec -= m_maintenance_interval / 1000;
}



void MaintenanceScheduler::schedule() {
  RangeStatsVector range_data;
  RangeStatsVector range_data_prioritized;
  AccessGroup::MaintenanceData *ag_data;
  String output;
  String ag_name;
  String trace_str = "STAT ***** MaintenanceScheduler::schedule() *****\n";
  MaintenancePrioritizer::MemoryState memory_state;

  memory_state.balance = Global::memory_tracker->balance();

  if (low_memory_mode()) {
    if (Global::maintenance_queue->pending() < Global::maintenance_queue->workers())
      m_scheduling_needed = true;
    int64_t excess = (memory_state.balance > Global::memory_limit) ? memory_state.balance - Global::memory_limit : 0;
    memory_state.needed = ((Global::memory_limit * 10) / 100) + excess;
  }

  boost::xtime now;
  boost::xtime_get(&now, TIME_UTC);
  int64_t millis_since_last_maintenance =
    xtime_diff_millis(m_last_maintenance, now);

  if (!m_scheduling_needed &&
      millis_since_last_maintenance < m_maintenance_interval)
    return;

  Global::maintenance_queue->clear();

  m_stats_gatherer->fetch(range_data);

  if (range_data.empty()) {
    m_scheduling_needed = false;
    return;
  }

  HT_ASSERT(m_prioritizer);

  int64_t block_index_memory = 0;
  int64_t bloom_filter_memory = 0;
  int64_t cell_cache_memory = 0;
  int64_t shadow_cache_memory = 0;

  /**
   * Purge commit log fragments
   */
  {
    int64_t revision_user = Global::user_log ? Global::user_log->get_latest_revision() : TIMESTAMP_MIN;
    int64_t revision_metadata = Global::metadata_log ? Global::metadata_log->get_latest_revision() : TIMESTAMP_MIN;
    int64_t revision_root = Global::root_log ? Global::root_log->get_latest_revision() : TIMESTAMP_MIN;
    AccessGroup::CellStoreMaintenanceData *cs_data;

    for (size_t i=0; i<range_data.size(); i++) {
      for (ag_data = range_data[i]->agdata; ag_data; ag_data = ag_data->next) {

	// compute memory stats
	cell_cache_memory += ag_data->mem_allocated;
	for (cs_data = ag_data->csdata; cs_data; cs_data = cs_data->next) {
	  shadow_cache_memory += cs_data->shadow_cache_size;
	  block_index_memory += cs_data->index_stats.block_index_memory;
	  bloom_filter_memory += cs_data->index_stats.bloom_filter_memory;
	}

        if (ag_data->earliest_cached_revision != TIMESTAMP_MAX) {
          if (range_data[i]->range->is_root()) {
            revision_root = ag_data->earliest_cached_revision;
          }
          else if (range_data[i]->is_metadata) {
            if (ag_data->earliest_cached_revision < revision_metadata)
              revision_metadata = ag_data->earliest_cached_revision;
          }
          else {
            if (ag_data->earliest_cached_revision < revision_user)
              revision_user = ag_data->earliest_cached_revision;
          }
        }
      }
    }

    trace_str += String("STAT revision_root\t") + revision_root + "\n";
    trace_str += String("STAT revision_metadata\t") + revision_metadata + "\n";
    trace_str += String("STAT revision_user\t") + revision_user + "\n";

    if (Global::root_log)
      Global::root_log->purge(revision_root);

    if (Global::metadata_log)
      Global::metadata_log->purge(revision_metadata);

    if (Global::user_log)
      Global::user_log->purge(revision_user);
  }

  {
    int64_t block_cache_memory = Global::block_cache->memory_used();
    int64_t total_memory = block_cache_memory + block_index_memory + bloom_filter_memory + cell_cache_memory + shadow_cache_memory + m_query_cache_memory;
    double block_cache_pct = ((double)block_cache_memory / (double)total_memory) * 100.0;
    double block_index_pct = ((double)block_index_memory / (double)total_memory) * 100.0;
    double bloom_filter_pct = ((double)bloom_filter_memory / (double)total_memory) * 100.0;
    double cell_cache_pct = ((double)cell_cache_memory / (double)total_memory) * 100.0;
    double shadow_cache_pct = ((double)shadow_cache_memory / (double)total_memory) * 100.0;
    double query_cache_pct = ((double)m_query_cache_memory / (double)total_memory) * 100.0;

    HT_INFOF("Memory Statistics (MB): VM=%.2f, RSS=%.2f, tracked=%.2f, computed=%.2f limit=%.2f",
	     System::proc_stat().vm_size, System::proc_stat().vm_resident,
	     (double)memory_state.balance/1000000.0, (double)total_memory/1000000.0,
             (double)Global::memory_limit/1000000.0);
    HT_INFOF("Memory Allocation: BlockCache=%.2f%% BlockIndex=%.2f%% "
	     "BloomFilter=%.2f%% CellCache=%.2f%% ShadowCache=%.2f%% "
	     "QueryCache=%.2f%%",
	     block_cache_pct, block_index_pct, bloom_filter_pct,
	     cell_cache_pct, shadow_cache_pct, query_cache_pct);
  }

  m_prioritizer->prioritize(range_data, memory_state, trace_str);

  boost::xtime schedule_time;
  boost::xtime_get(&schedule_time, boost::TIME_UTC);

  // if this is the first time around, just enqueue work that
  // was in progress
  if (!m_initialized) {
    for (size_t i=0; i<range_data.size(); i++) {
      if (range_data[i]->state == RangeState::SPLIT_LOG_INSTALLED ||
          range_data[i]->state == RangeState::SPLIT_SHRUNK) {
        RangePtr range(range_data[i]->range);
        Global::maintenance_queue->add(new MaintenanceTaskSplit(schedule_time, range));
      }
    }
    m_initialized = true;
  }
  else {

    // Sort the ranges based on priority
    range_data_prioritized.reserve( range_data.size() );
    for (size_t i=0; i<range_data.size(); i++) {
      if (range_data[i]->priority > 0)
	range_data_prioritized.push_back(range_data[i]);
    }
    struct RangeStatsAscending ordering;
    sort(range_data_prioritized.begin(), range_data_prioritized.end(), ordering);

    for (size_t i=0; i<range_data_prioritized.size(); i++) {
      if (range_data_prioritized[i]->maintenance_flags & MaintenanceFlag::SPLIT) {
        RangePtr range(range_data_prioritized[i]->range);
        Global::maintenance_queue->add(new MaintenanceTaskSplit(schedule_time, range));
      }
      else if (range_data_prioritized[i]->maintenance_flags & MaintenanceFlag::COMPACT) {
	MaintenanceTaskCompaction *task;
        RangePtr range(range_data_prioritized[i]->range);
	task = new MaintenanceTaskCompaction(schedule_time, range);
	for (AccessGroup::MaintenanceData *ag_data=range_data_prioritized[i]->agdata; ag_data; ag_data=ag_data->next) {
	  if (ag_data->maintenance_flags & MaintenanceFlag::COMPACT)
	    task->add_subtask(ag_data->ag, ag_data->maintenance_flags);
	}
        Global::maintenance_queue->add(task);
      }
      else if (range_data_prioritized[i]->maintenance_flags & MaintenanceFlag::MEMORY_PURGE) {
	MaintenanceTaskMemoryPurge *task;
        RangePtr range(range_data_prioritized[i]->range);
	task = new MaintenanceTaskMemoryPurge(schedule_time, range);
	for (AccessGroup::MaintenanceData *ag_data=range_data_prioritized[i]->agdata; ag_data; ag_data=ag_data->next) {
	  if (ag_data->maintenance_flags & MaintenanceFlag::MEMORY_PURGE) {
	    task->add_subtask(ag_data->ag, ag_data->maintenance_flags);
	    for (AccessGroup::CellStoreMaintenanceData *cs_data=ag_data->csdata; cs_data; cs_data=cs_data->next) {
	      if (cs_data->maintenance_flags & MaintenanceFlag::MEMORY_PURGE)
		task->add_subtask(cs_data->cs, cs_data->maintenance_flags);
	    }
	  }
	}
        Global::maintenance_queue->add(task);
      }
    }
  }

  cout << flush << trace_str << flush;

  m_scheduling_needed = false;

  m_stats_gatherer->clear();
}
