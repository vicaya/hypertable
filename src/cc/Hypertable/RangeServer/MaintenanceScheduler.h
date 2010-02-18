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

#ifndef HYPERTABLE_MAINTENANCESCHEDULER_H
#define HYPERTABLE_MAINTENANCESCHEDULER_H

#include "Common/ReferenceCount.h"

#include "MaintenancePrioritizerLogCleanup.h"
#include "MaintenancePrioritizerLowMemory.h"
#include "RangeServerStats.h"

namespace Hypertable {

  class MaintenanceScheduler : public ReferenceCount {
  public:
    MaintenanceScheduler(MaintenanceQueuePtr &queue,
			 RangeServerStatsPtr &server_stats,
                         RangeStatsGathererPtr &gatherer);

    void schedule();

    void set_low_memory_mode(bool on) {
      if (on) {
        if (m_prioritizer != &m_prioritizer_low_memory)
          m_prioritizer = &m_prioritizer_low_memory;
      }
      else {
        if (m_prioritizer != &m_prioritizer_log_cleanup)
          m_prioritizer = &m_prioritizer_log_cleanup;
      }
    }

    void need_scheduling() {
      m_scheduling_needed = true;
    }

  private:

    bool low_memory_mode() {
      return m_prioritizer == &m_prioritizer_low_memory;
    }

    Mutex m_mutex;
    bool m_initialized;
    bool m_scheduling_needed;
    ApplicationQueuePtr m_app_queue;
    MaintenanceQueuePtr m_queue;
    RangeServerStatsPtr m_server_stats;
    RangeStatsGathererPtr m_stats_gatherer;
    MaintenancePrioritizer *m_prioritizer;
    MaintenancePrioritizerLogCleanup m_prioritizer_log_cleanup;
    MaintenancePrioritizerLowMemory  m_prioritizer_low_memory;
    int32_t m_maintenance_interval;
    boost::xtime m_last_maintenance;
    int64_t m_query_cache_memory;
  };

  typedef intrusive_ptr<MaintenanceScheduler> MaintenanceSchedulerPtr;


}

#endif // HYPERTABLE_MAINTENANCESCHEDULER_H


