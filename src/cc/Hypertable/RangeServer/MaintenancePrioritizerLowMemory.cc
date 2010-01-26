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
                                            int64_t memory_needed,
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
    assign_priorities(range_data_root, Global::root_log,
                      Global::log_prune_threshold_min,
                      memory_needed, priority, trace_str);


  /**
   * Assign priority for METADATA ranges
   */
  if (!range_data_metadata.empty())
    assign_priorities(range_data_metadata, Global::metadata_log,
                      Global::log_prune_threshold_min, memory_needed,
		      priority, trace_str);


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
                      memory_needed, priority, trace_str);

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

  struct ShadowCacheSortOrdering {
    bool operator()(const AccessGroup::CellStoreMaintenanceData *x,
		    const AccessGroup::CellStoreMaintenanceData *y) const {
      if (x->shadow_cache_size > 0 && y->shadow_cache_size > 0) {
	if (x->shadow_cache_hits > 0 || y->shadow_cache_hits > 0)
	  return x->shadow_cache_hits < y->shadow_cache_hits;
	return x->shadow_cache_ecr < y->shadow_cache_ecr;
      }
      return x->shadow_cache_size > y->shadow_cache_size;
    }
  };

  struct CellStoreIndexSortOrdering {
    bool operator()(const AccessGroup::CellStoreMaintenanceData *x,
		    const AccessGroup::CellStoreMaintenanceData *y) const {
      int64_t x_mem = x->index_stats.bloom_filter_memory + x->index_stats.block_index_memory;
      int64_t y_mem = y->index_stats.bloom_filter_memory + y->index_stats.block_index_memory;

      if (y_mem == 0 || x_mem == 0)
	return x_mem > y_mem;

      uint64_t x_atime = std::max(x->index_stats.bloom_filter_access_counter,
				  x->index_stats.block_index_access_counter);
      uint64_t y_atime = std::max(y->index_stats.bloom_filter_access_counter,
				  y->index_stats.block_index_access_counter);

      return x_atime < y_atime;
    }
  };

  struct CellCacheCompactionSortOrdering {
    bool operator()(const AccessGroup::MaintenanceData *x,
		    const AccessGroup::MaintenanceData *y) const {
      if (x->mem_used && y->mem_used)
	return x->earliest_cached_revision < y->earliest_cached_revision;
      return x->mem_used;
    }
  };

  void adjust_memory_needed(int64_t *neededp, int64_t *freedp) {
    if (*freedp > *neededp)
      *neededp = 0;
    else
      *neededp -= *freedp;
  }

}


/**
 * Memory freeing algorithm:
 *
 * 1. schedule in-progress splits
 * 2. schedule splits
 * 3. purge shadow caches
 * 4. compact LARGE cell caches
 * 5. purge cellstore indexes 
 * 6. compact remaining cell caches (remember to propertly account for in-memory caches)
 */

void
MaintenancePrioritizerLowMemory::assign_priorities(RangeStatsVector &range_data,
            CommitLog *log, int64_t prune_threshold, int64_t &memory_needed,
	    int32_t &priority, String &trace_str) {
  Range::MaintenanceData *range_maintenance_data;
  AccessGroup::MaintenanceData *ag_data;
  AccessGroup::CellStoreMaintenanceData *cs_data;
  int64_t disk_total, mem_total;
  std::vector<AccessGroup::MaintenanceData *> md;
  std::vector<AccessGroup::CellStoreMaintenanceData *> csmd;
  int64_t memory_freed = 0;

  HT_ON_SCOPE_EXIT(&adjust_memory_needed, &memory_needed, &memory_freed);

  /**
   * 1. Schedule in-progress splits
   */
  for (size_t i=0; i<range_data.size(); i++) {

    if (range_data[i]->busy)
      continue;

    if (range_data[i]->state == RangeState::SPLIT_LOG_INSTALLED ||
        range_data[i]->state == RangeState::SPLIT_SHRUNK) {
      HT_INFOF("Adding maintenance for range %s because mid-split(%d)",
               range_data[i]->range->get_name().c_str(), range_data[i]->state);
      range_data[i]->maintenance_flags |= MaintenanceFlag::SPLIT;
      range_data[i]->priority = priority++;
      if (range_data[i]->state == RangeState::SPLIT_LOG_INSTALLED) {
	for (ag_data = range_data[i]->agdata; ag_data; ag_data = ag_data->next) {
	  memory_freed += ag_data->mem_used / 2;
	  for (cs_data=ag_data->csdata; cs_data; cs_data=cs_data->next)
	    memory_freed += 
	      cs_data->index_stats.bloom_filter_memory +
	      cs_data->index_stats.block_index_memory +
	      cs_data->shadow_cache_size;
	}
      }
      continue;
    }
  }

  if (memory_freed >= memory_needed)
    return;

  /**
   * 2. Schedule splits
   */
  for (size_t i=0; i<range_data.size(); i++) {

    if (range_data[i]->busy || range_data[i]->priority)
      continue;

    mem_total = 0;
    disk_total = 0;

    // compute disk and memory totals
    for (ag_data = range_data[i]->agdata; ag_data; ag_data = ag_data->next) {
      disk_total += ag_data->disk_used;
      mem_total += ag_data->mem_used / 2;
      for (cs_data=ag_data->csdata; cs_data; cs_data=cs_data->next)
	mem_total +=
	  cs_data->index_stats.bloom_filter_memory +
	  cs_data->index_stats.block_index_memory +
	  cs_data->shadow_cache_size;
    }

    if (!range_data[i]->range->is_root() &&
	range_data[i]->range->get_error() != Error::RANGESERVER_ROW_OVERFLOW) {
      if (range_data[i]->table_id == 0 && Global::range_metadata_split_size != 0) {
        if (disk_total >= Global::range_metadata_split_size) {
          HT_INFOF("Adding maintenance for range %s because dist_total %d exceeds %d",
                   range_data[i]->range->get_name().c_str(), (int)disk_total, (int)Global::range_metadata_split_size);
          memory_freed += mem_total;
          range_data[i]->priority = priority++;
	  range_data[i]->maintenance_flags |= MaintenanceFlag::SPLIT;
        }
      }
      else {
        if (disk_total >= Global::range_split_size) {
          HT_INFOF("Adding maintenance for range %s because dist_total %d exceeds %d",
                   range_data[i]->range->get_name().c_str(), (int)disk_total, (int)Global::range_split_size);
          memory_freed += mem_total;
          range_data[i]->priority = priority++;
	  range_data[i]->maintenance_flags |= MaintenanceFlag::SPLIT;
        }
      }
    }
  }

  if (memory_freed >= memory_needed)
    return;

  /**
   *  3. purge shadow cache
   */

  csmd.clear();
  for (size_t i=0; i<range_data.size(); i++) {
    if (range_data[i]->busy || range_data[i]->priority)
      continue;
    for (ag_data = range_data[i]->agdata; ag_data; ag_data = ag_data->next) {
      ag_data->user_data = (void *)range_data[i];
      for (cs_data=ag_data->csdata; cs_data; cs_data=cs_data->next) {
	if (cs_data->shadow_cache_size > 0) {
 	  cs_data->user_data = (void *)ag_data;
	  csmd.push_back(cs_data);
	}
      }
    }
  }

  {
    struct ShadowCacheSortOrdering ordering;
    sort(csmd.begin(), csmd.end(), ordering);
  }
  
  for (size_t i=0; i<csmd.size(); i++) {
    ag_data = (AccessGroup::MaintenanceData *)(csmd[i]->user_data);
    ag_data->maintenance_flags |= MaintenanceFlag::MEMORY_PURGE;
    range_maintenance_data = (Range::MaintenanceData *)(ag_data->user_data);
    range_maintenance_data->maintenance_flags |= MaintenanceFlag::MEMORY_PURGE;
    if (range_maintenance_data->priority == 0)
      range_maintenance_data->priority = priority++;
    csmd[i]->maintenance_flags |= MaintenanceFlag::MEMORY_PURGE_SHADOW_CACHE;
    memory_freed += csmd[i]->shadow_cache_size;
    if (memory_freed >= memory_needed)
      return;
  }

  /**
   *  4. Compact LARGE cell caches
   */

  for (size_t i=0; i<range_data.size(); i++) {

    if (range_data[i]->busy ||
	range_data[i]->maintenance_flags & MaintenanceFlag::SPLIT)
      continue;

    for (ag_data = range_data[i]->agdata; ag_data; ag_data = ag_data->next) {
      if (!ag_data->in_memory &&
	  (((int64_t)(ag_data->compression_ratio * (float)ag_data->mem_used) > m_cellstore_minimum_size) ||
	   ag_data->mem_used > Global::access_group_max_mem)) {
	if (range_data[i]->priority == 0)
	  range_data[i]->priority = priority++;
	ag_data->maintenance_flags |= MaintenanceFlag::COMPACT_MINOR|MaintenanceFlag::MEMORY_PURGE_SHADOW_CACHE;
	range_data[i]->maintenance_flags |= MaintenanceFlag::COMPACT|MaintenanceFlag::MEMORY_PURGE;
	memory_freed += ag_data->mem_used;
      }
    }
  }

  if (memory_freed >= memory_needed)
    return;

  /**
   *  5. purge cellstore indexes 
   */

  csmd.clear();
  for (size_t i=0; i<range_data.size(); i++) {
    if (range_data[i]->busy ||
	range_data[i]->maintenance_flags & MaintenanceFlag::SPLIT)
      continue;
    for (ag_data = range_data[i]->agdata; ag_data; ag_data = ag_data->next) {
      ag_data->user_data = (void *)range_data[i];
      for (cs_data=ag_data->csdata; cs_data; cs_data=cs_data->next) {
	if (cs_data->index_stats.bloom_filter_memory > 0 ||
	    cs_data->index_stats.block_index_memory > 0) {
 	  cs_data->user_data = (void *)ag_data;
	  csmd.push_back(cs_data);
	}
      }
    }
  }

  {
    CellStoreIndexSortOrdering ordering;
    sort(csmd.begin(), csmd.end(), ordering);
  }

  int64_t memory_used = 0;
  for (size_t i=0; i<csmd.size(); i++) {
    ag_data = (AccessGroup::MaintenanceData *)(csmd[i]->user_data);
    ag_data->maintenance_flags |= MaintenanceFlag::MEMORY_PURGE;
    range_maintenance_data = (Range::MaintenanceData *)(ag_data->user_data);
    range_maintenance_data->maintenance_flags |= MaintenanceFlag::MEMORY_PURGE;
    memory_used = csmd[i]->index_stats.block_index_memory + csmd[i]->index_stats.bloom_filter_memory;
    if (range_maintenance_data->priority == 0)
      range_maintenance_data->priority = priority++;
    csmd[i]->maintenance_flags |= MaintenanceFlag::MEMORY_PURGE_CELLSTORE;
    memory_freed += memory_used;
    if (memory_freed >= memory_needed)
      return;
  }

  /**
   *  6. compact remaining cell caches
   */

  md.clear();

  for (size_t i=0; i<range_data.size(); i++) {

    if (range_data[i]->busy ||
	range_data[i]->maintenance_flags & (MaintenanceFlag::SPLIT|MaintenanceFlag::COMPACT))
      continue;

    for (ag_data = range_data[i]->agdata; ag_data; ag_data = ag_data->next) {
      if (!ag_data->in_memory && ag_data->mem_used > 0) {
	ag_data->user_data = range_data[i];
	md.push_back(ag_data);
      }
    }

  }

  {
    struct CellCacheCompactionSortOrdering ordering;
    sort(md.begin(), md.end(), ordering);
  }

  for (size_t i=0; i<md.size(); i++) {
    if (((Range::MaintenanceData *)md[i]->user_data)->priority == 0)
      ((Range::MaintenanceData *)md[i]->user_data)->priority = priority++;
    md[i]->maintenance_flags |= MaintenanceFlag::COMPACT_MINOR;
    ((Range::MaintenanceData *)md[i]->user_data)->maintenance_flags |= MaintenanceFlag::COMPACT;
    memory_freed += md[i]->mem_used;
    if (memory_freed >= memory_needed)
      break;
  }

}
