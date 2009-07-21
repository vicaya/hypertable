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

#include <algorithm>
#include <iostream>

#include "Global.h"
#include "MaintenancePrioritizerLogCleanup.h"
#include "MaintenanceScheduler.h"
#include "MaintenanceTaskCompaction.h"
#include "MaintenanceTaskIndexPurge.h"
#include "MaintenanceTaskSplit.h"

using namespace Hypertable;
using namespace std;
using namespace Hypertable::Config;

namespace {
  struct RangeStatsDescending {
    bool operator()(const Range::MaintenanceData *x, const Range::MaintenanceData *y) const {
      return x->priority > y->priority;
    }
  };
}


MaintenanceScheduler::MaintenanceScheduler(MaintenanceQueuePtr &queue,
                                           RangeStatsGathererPtr &gatherer)
  : m_initialized(false), m_scheduling_needed(false), m_queue(queue),
    m_stats_gatherer(gatherer), m_prioritizer_log_cleanup(m_stats),
    m_prioritizer_low_memory(m_stats) {
  m_prioritizer = &m_prioritizer_log_cleanup;
  m_maintenance_interval = get_i32("Hypertable.RangeServer.Maintenance.Interval");
}



void MaintenanceScheduler::schedule() {
  RangeStatsVector range_data;
  AccessGroup::MaintenanceData *ag_data;
  String output;
  String ag_name;
  String trace_str = "STAT ***** MaintenanceScheduler::schedule() *****\n";
  int64_t memory_needed = 0;

  if (low_memory_mode()) {
    if (Global::maintenance_queue->pending() < Global::maintenance_queue->workers())
      m_scheduling_needed = true;
    memory_needed = (Global::memory_limit * 10) / 100;  // 10% of the memory limit
  }

  m_stats.stop();

  if (!m_scheduling_needed &&
      m_stats.duration_millis() < m_maintenance_interval)
    return;

  Global::maintenance_queue->clear();

  m_stats_gatherer->fetch(range_data);

  if (range_data.empty()) {
    m_scheduling_needed = false;
    m_stats.start();
    return;
  }

  HT_ASSERT(m_prioritizer);

  /**
   * Purge commit log fragments
   */
  {
    int64_t revision_user;
    int64_t revision_metadata;
    int64_t revision_root;

    (Global::user_log !=0) ?
        revision_user = Global::user_log->get_latest_revision() : TIMESTAMP_MIN;
    (Global::metadata_log !=0) ?
        revision_metadata = Global::metadata_log->get_latest_revision() : TIMESTAMP_MIN;
    (Global::root_log !=0) ?
        revision_root = Global::root_log->get_latest_revision() : TIMESTAMP_MIN;

    for (size_t i=0; i<range_data.size(); i++) {
      for (ag_data = range_data[i]->agdata; ag_data; ag_data = ag_data->next) {
        if (ag_data->earliest_cached_revision != TIMESTAMP_NULL) {
          if (range_data[i]->range->is_root()) {
            revision_root = ag_data->earliest_cached_revision;
          }
          else if (range_data[i]->table_id == 0) {
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

  m_prioritizer->prioritize(range_data, memory_needed, trace_str);

  boost::xtime schedule_time;
  boost::xtime_get(&schedule_time, boost::TIME_UTC);

  for (size_t i=0; i<range_data.size(); i++) {
    if (range_data[i]->purgeable_index_memory > 0) {
      RangePtr range(range_data[i]->range);
      Global::maintenance_queue->add(new MaintenanceTaskIndexPurge(schedule_time, range, m_stats.starting_scanner_generation()));
    }
  }

  struct RangeStatsDescending range_sort_desc;

  sort(range_data.begin(), range_data.end(), range_sort_desc);

  if (!m_initialized) {
    // if this is the first time around, just enqueue work that
    // was in progress
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

    for (size_t i=0; i<range_data.size() && range_data[i]->priority > 0; i++) {
      if (range_data[i]->needs_split) {
        RangePtr range(range_data[i]->range);
        Global::maintenance_queue->add(new MaintenanceTaskSplit(schedule_time, range));
      }
      else if (range_data[i]->needs_compaction) {
        RangePtr range(range_data[i]->range);
        Global::maintenance_queue->add(new MaintenanceTaskCompaction(schedule_time, range, false));
      }
    }

  }

  cout << flush << trace_str << flush;

  m_scheduling_needed = false;

  m_stats_gatherer->clear();

  m_stats.start();
}

