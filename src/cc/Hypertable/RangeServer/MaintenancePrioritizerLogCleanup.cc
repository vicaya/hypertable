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
#include "Common/StringExt.h"

#include <iostream>

#include "Global.h"
#include "MaintenancePrioritizerLogCleanup.h"

using namespace Hypertable;
using namespace std;

void
MaintenancePrioritizerLogCleanup::prioritize(RangeStatsVector &range_data,
                                             Stats &stats, String &trace_str) {
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
                      Global::log_prune_threshold_min, trace_str);


  /**
   * Assign priority for METADATA ranges
   */
  if (!range_data_metadata.empty())
    assign_priorities(range_data_metadata, Global::metadata_log,
                      Global::log_prune_threshold_min, trace_str);


  /**
   * Assign priority for USER ranges
   */
  int64_t prune_threshold = (int64_t)(stats.mbps() * (double)Global::log_prune_threshold_max);

  if (prune_threshold < Global::log_prune_threshold_min)
    prune_threshold = Global::log_prune_threshold_min;
  else if (prune_threshold > Global::log_prune_threshold_max)
    prune_threshold = Global::log_prune_threshold_max;

  trace_str += String("STATS user log prune threshold\t") + prune_threshold + "\n";

  if (!range_data_user.empty())
    assign_priorities(range_data_user, Global::user_log, prune_threshold, trace_str);

}



void
MaintenancePrioritizerLogCleanup::assign_priorities(RangeStatsVector &stats,
              CommitLog *log, int64_t prune_threshold, String &trace_str) {
  CommitLog::CumulativeSizeMap cumulative_size_map;
  CommitLog::CumulativeSizeMap::iterator iter;
  AccessGroup::MaintenanceData *ag_data;
  int state, disk_total;

  log->load_cumulative_size_map(cumulative_size_map);
  
  for (size_t i=0; i<stats.size(); i++) {

    state = stats[i]->range->get_state();

    if (stats[i]->busy)
      continue;
    
    if (state == RangeState::SPLIT_LOG_INSTALLED ||
        state == RangeState::SPLIT_SHRUNK) {
      HT_INFOF("Adding maintenance for range %s because mid-split(%d)", stats[i]->range->get_name().c_str(), state);
      stats[i]->split_needed = true;
      stats[i]->priority = 3;
      continue;
    }

    disk_total = 0;

    for (ag_data = stats[i]->agdata; ag_data; ag_data = ag_data->next) {

      if (ag_data->mem_used > Global::access_group_max_mem) {
        stats[i]->priority = 1;
        ag_data->ag->set_compaction_bit();
      }

      disk_total += ag_data->disk_used;

      if (ag_data->earliest_cached_revision == TIMESTAMP_NULL ||
          ag_data->earliest_cached_revision == TIMESTAMP_MAX)
        continue;

      iter = cumulative_size_map.lower_bound(ag_data->earliest_cached_revision);

      if (iter == cumulative_size_map.end()) {
        String errstr;
        for (iter = cumulative_size_map.begin(); iter != cumulative_size_map.end(); iter++) {
          errstr += String("PERROR frag-") + (*iter).second.fragno +
            "\trevision\t" + (*iter).first + "\n";
          errstr += String("PERROR frag-") + (*iter).second.fragno +
            "\tdistance\t" + (*iter).second.distance + "\n";
          errstr += String("PERROR frag-") + (*iter).second.fragno +
            "\tsize\t" + (*iter).second.cumulative_size + "\n";
        }
        errstr += String("PERROR revision ") +
          ag_data->earliest_cached_revision + " not found in map\n";
        cout << flush << errstr << flush;
        trace_str += String("STAT *** This should never happen, ecr = ") +
          ag_data->earliest_cached_revision + " ***\n";
        continue;
      }

      if ((*iter).second.cumulative_size > prune_threshold) {
        trace_str += String("STAT ") + ag_data->ag->get_full_name()+" cumulative_size "
          + (*iter).second.cumulative_size + " > prune_threshold " + prune_threshold + "\n";

        if (ag_data->mem_used > 0)
          ag_data->ag->set_compaction_bit();

        stats[i]->priority = 1;
        
      }
      else
        trace_str += String("STAT ") + ag_data->ag->get_full_name()+" cumulative_size "
          + (*iter).second.cumulative_size + " <= prune_threshold " + prune_threshold + "\n";
    }

    if (!stats[i]->range->is_root() && disk_total >= Global::range_max_bytes) {
      HT_INFOF("Adding maintenance for range %s because dist_total %d exceeds %d",
               stats[i]->range->get_name().c_str(), (int)disk_total, (int)Global::range_max_bytes);
      stats[i]->priority = 2;
      stats[i]->split_needed = true;
    }
  }

}
