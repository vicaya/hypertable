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

#include <iostream>

#include "Global.h"
#include "MaintenancePrioritizerLogCleanup.h"
#include "MaintenanceScheduler.h"
#include "MaintenanceTaskCompaction.h"

using namespace Hypertable;
using namespace std;

MaintenanceScheduler::MaintenanceScheduler(MaintenanceQueuePtr &queue,
                                           RangeStatsGathererPtr &gatherer) 
  : m_initialized(false), m_queue(queue), m_stats_gatherer(gatherer) {
  m_prioritizer = new MaintenancePrioritizerLogCleanup();
}



void MaintenanceScheduler::schedule() {
  RangeStatsVector range_data;
  AccessGroup::MaintenanceData *ag_data;
  String output;
  String ag_name;
  String trace_str = "STAT ***** MaintenanceScheduler::schedule() *****\n";

  m_stats.stop();

  Global::maintenance_queue->clear();

  m_stats_gatherer->fetch(range_data);

  if (range_data.empty()) {
    m_stats.start();
    return;
  }

  HT_ASSERT(m_prioritizer);

  /**
   * Purge commit log fragments
   */
  {
    int64_t revision;
    int64_t revision_root     = TIMESTAMP_MAX;
    int64_t revision_metadata = TIMESTAMP_MAX;
    int64_t revision_user     = TIMESTAMP_MAX;

    for (size_t i=0; i<range_data.size(); i++) {
      for (ag_data = range_data[i]->agdata; ag_data; ag_data = ag_data->next) {
        if ((revision = ag_data->ag->get_earliest_cached_revision()) != TIMESTAMP_NULL) {
          if (range_data[i]->range->is_root()) {
            revision_root = revision;
          }
          else if (range_data[i]->table_id == 0) {
            if (revision < revision_metadata)
              revision_metadata = revision;
          }
          else {
            if (revision < revision_user)
              revision_user = revision;
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

  m_prioritizer->prioritize(range_data, m_stats, trace_str);

  boost::xtime schedule_time;
  boost::xtime_get(&schedule_time, boost::TIME_UTC);

  if (!m_initialized) {
    // if this is the first time around, just enqueue work that
    // was in progress
    for (size_t i=0; i<range_data.size(); i++) {
      if (range_data[i]->priority == 3) {
        RangePtr range(range_data[i]->range);
        Global::maintenance_queue->add(new MaintenanceTaskSplit(schedule_time, range));
      }
    }    
    m_initialized = true;
  }
  else {

    for (size_t i=0; i<range_data.size(); i++) {
      if (range_data[i]->priority == 1) {
        RangePtr range(range_data[i]->range);
        Global::maintenance_queue->add(new MaintenanceTaskCompaction(schedule_time, range, false));
      }
      else if (range_data[i]->priority == 2 || range_data[i]->priority == 3) {
        RangePtr range(range_data[i]->range);
        Global::maintenance_queue->add(new MaintenanceTaskSplit(schedule_time, range));
      }
    }

  }

  cout << flush << trace_str << flush;

  m_stats.start();
}

