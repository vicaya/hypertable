/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * This program is distributed in the hope that it will be useful,
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

#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>

#include <queue>
#include <string>

#include "AccessGroup.h"
#include "Range.h"

namespace hypertable {

  class MaintenanceThread {
  public:

    enum WorkType { MAINTENANCE, SPLIT, MERGE, COMPACTION_MAJOR, COMPACTION_MINOR };

    typedef struct {
      WorkType   type;
      Range     *range;
      uint64_t   timestamp;
      std::string localityGroupName;
    } CompactionInfoT;

    static std::queue<CompactionInfoT> msInputQueue;

    static boost::mutex     msMutex;
    static boost::condition msCond;

    static void ScheduleMaintenance(Range *range, uint64_t timestamp);
    static void ScheduleCompaction(Range *range, WorkType wtype, uint64_t timestamp, const char *lgName);

    void operator()();

  };

}

#endif // HYPERTABLE_MAINTENANCETHREAD_H

