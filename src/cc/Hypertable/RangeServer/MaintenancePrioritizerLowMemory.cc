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
#include "MaintenancePrioritizerLowMemory.h"

using namespace Hypertable;
using namespace std;

void
MaintenancePrioritizerLowMemory::prioritize(RangeStatsVector &range_data,
                                            int64_t memory_needed,
                                            String &trace_str) {
  RangeStatsVector range_data_root;
  RangeStatsVector range_data_metadata;
  RangeStatsVector range_data_user;

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
                      memory_needed, trace_str);


  /**
   * Assign priority for METADATA ranges
   */
  if (!range_data_metadata.empty())
    assign_priorities(range_data_metadata, Global::metadata_log,
                      Global::log_prune_threshold_min,
                      memory_needed, trace_str);


  /**
   * Assign priority for USER ranges
   */
  int64_t prune_threshold = (int64_t)(m_stats.mbps() * (double)Global::log_prune_threshold_max);

  if (prune_threshold < Global::log_prune_threshold_min)
    prune_threshold = Global::log_prune_threshold_min;
  else if (prune_threshold > Global::log_prune_threshold_max)
    prune_threshold = Global::log_prune_threshold_max;

  trace_str += String("STATS user log prune threshold\t") + prune_threshold + "\n";

  if (!range_data_user.empty())
    assign_priorities(range_data_user, Global::user_log, prune_threshold,
                      memory_needed, trace_str);

}

namespace {

  class StatsRec {
  public:
    StatsRec(AccessGroup::MaintenanceData *agdata_,
             Range::MaintenanceData *rangedata_) :
      agdata(agdata_), rangedata(rangedata_) { }
    AccessGroup::MaintenanceData *agdata;
    Range::MaintenanceData *rangedata;
  };

  struct StatsRecOrderingDescending {
    bool operator()(const StatsRec &x, const StatsRec &y) const {
      return x.agdata->mem_used > y.agdata->mem_used;
    }
  };

}



void
MaintenancePrioritizerLowMemory::assign_priorities(RangeStatsVector &range_data,
              CommitLog *log, int64_t prune_threshold,
              int64_t &memory_needed, String &trace_str) {
  AccessGroup::MaintenanceData *ag_data;
  int64_t disk_total, mem_total;
  std::vector<StatsRec> ag_range_stats;
  int64_t memory_freed = 0;

  for (size_t i=0; i<range_data.size(); i++) {

    if (range_data[i]->busy)
      continue;

    if (range_data[i]->state == RangeState::SPLIT_LOG_INSTALLED ||
        range_data[i]->state == RangeState::SPLIT_SHRUNK) {
      HT_INFOF("Adding maintenance for range %s because mid-split(%d)",
               range_data[i]->range->get_name().c_str(), range_data[i]->state);
      range_data[i]->needs_split = true;
      range_data[i]->priority = 3000;
      range_data[i]->purgeable_index_memory = 0;
      for (ag_data = range_data[i]->agdata; ag_data; ag_data = ag_data->next) {
        if (range_data[i]->state == RangeState::SPLIT_LOG_INSTALLED)
          memory_freed += ag_data->mem_used;
        range_data[i]->purgeable_index_memory +=
          ag_data->ag->purgeable_index_memory(m_stats.starting_access_counter());
      }
      memory_freed += range_data[i]->purgeable_index_memory;
      continue;
    }

    mem_total = 0;
    disk_total = 0;
    range_data[i]->purgeable_index_memory = 0;

    for (ag_data = range_data[i]->agdata; ag_data; ag_data = ag_data->next) {
      disk_total += ag_data->disk_used;
      range_data[i]->purgeable_index_memory +=
        ag_data->ag->purgeable_index_memory(m_stats.starting_access_counter());
      ag_range_stats.push_back(StatsRec(ag_data, range_data[i]));
    }
    memory_freed += range_data[i]->purgeable_index_memory;

    if (!range_data[i]->range->is_root()) {
      if (range_data[i]->table_id == 0 && Global::range_metadata_split_size != 0) {
        if (disk_total >= Global::range_metadata_split_size) {
          HT_INFOF("Adding maintenance for range %s because dist_total %d exceeds %d",
                   range_data[i]->range->get_name().c_str(), (int)disk_total, (int)Global::range_metadata_split_size);
          mem_total = range_data[i]->compactable_memory();
          memory_freed += mem_total;
          range_data[i]->priority = 2000 + Math::log2(mem_total);
          range_data[i]->needs_split = true;
        }
      }
      else {
        if (disk_total >= Global::range_split_size) {
          HT_INFOF("Adding maintenance for range %s because dist_total %d exceeds %d",
                   range_data[i]->range->get_name().c_str(), (int)disk_total, (int)Global::range_split_size);
          mem_total = range_data[i]->compactable_memory();
          memory_freed += mem_total;
          range_data[i]->priority = 1000 + Math::log2(mem_total);
          range_data[i]->needs_split = true;
        }
      }
    }
  }

  if (memory_freed < memory_needed) {

    struct StatsRecOrderingDescending order_desc;

    sort(ag_range_stats.begin(), ag_range_stats.end(), order_desc);

    for (size_t i=0; i<ag_range_stats.size(); i++) {
      if (memory_freed >= memory_needed ||
          ag_range_stats[i].agdata->mem_used == 0)
        break;
      if (!ag_range_stats[i].rangedata->needs_split) {
        ag_range_stats[i].rangedata->compact_memory +=
          ag_range_stats[i].agdata->mem_used;
        memory_freed += ag_range_stats[i].agdata->mem_used;
        ag_range_stats[i].agdata->ag->set_compaction_bit();
        ag_range_stats[i].rangedata->needs_compaction = true;
      }
    }

  }

  for (size_t i=0; i<range_data.size(); i++) {
    if (range_data[i]->needs_compaction)
      range_data[i]->priority = Math::log2( (uint32_t)(range_data[i]->compact_memory & 0xFFFFFFFFLL) );
  }

  if (memory_freed > memory_needed)
    memory_needed = 0;
  else
    memory_needed -= memory_freed;

}
