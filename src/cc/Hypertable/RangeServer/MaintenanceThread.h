/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This file is part of Hypertable.
 * 
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
#ifndef HYPERTABLE_MAINTENANCETHREAD_H
#define HYPERTABLE_MAINTENANCETHREAD_H

#include <queue>
#include <string>

#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/xtime.hpp>

#include "AccessGroup.h"
#include "Range.h"

namespace hypertable {

  class MaintenanceThread {
  public:

    enum WorkType { MAINTENANCE, SPLIT, MERGE, COMPACTION_MAJOR, COMPACTION_MINOR };

    typedef struct {
      WorkType      type;
      boost::xtime  startTime;
      Range        *range;
    } WorkInfoT;

    struct ltWorkInfo {
      bool operator()(const WorkInfoT &wi1, const WorkInfoT &wi2) const {
	return xtime_cmp(wi1.startTime, wi1.startTime) >= 0;
      }
    };

    static std::priority_queue<WorkInfoT, std::vector<WorkInfoT>, ltWorkInfo> ms_input_queue;

    static boost::mutex     ms_mutex;
    static boost::condition ms_cond;

    static void schedule_maintenance(Range *rangen);
    static void schedule_compaction(Range *range, WorkType wtype);

    void operator()();

  };

}

#endif // HYPERTABLE_MAINTENANCETHREAD_H

