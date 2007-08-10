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

#include <cassert>

#include "Common/Logger.h"

#include "MaintenanceThread.h"

using namespace hypertable;

std::queue<MaintenanceThread::CompactionInfoT> MaintenanceThread::msInputQueue;

boost::mutex      MaintenanceThread::msMutex;
boost::condition  MaintenanceThread::msCond;


void MaintenanceThread::ScheduleMaintenance(Range *range) {
  boost::mutex::scoped_lock lock(msMutex);
  CompactionInfoT workInfo;
  workInfo.type = MAINTENANCE;
  workInfo.range = range;
  msInputQueue.push(workInfo);
  msCond.notify_one();
}


void MaintenanceThread::ScheduleCompaction(Range *range, WorkType wtype) {
  boost::mutex::scoped_lock lock(msMutex);
  CompactionInfoT workInfo;
  workInfo.type = wtype;
  workInfo.range = range;
  msInputQueue.push(workInfo);
  msCond.notify_one();
}


/**
 *
 */
void MaintenanceThread::operator()() {
  CompactionInfoT workInfo;
  AccessGroup *lg;

  while (true) {

    // protect msInputQueue with the mutex
    {
      boost::mutex::scoped_lock lock(msMutex);

      while (msInputQueue.empty())
	msCond.wait(lock);

      workInfo  = msInputQueue.front();
    }

    if (workInfo.type == COMPACTION_MAJOR || workInfo.type == COMPACTION_MINOR) {
      bool majorCompaction = (workInfo.type == COMPACTION_MAJOR) ? true : false;
      workInfo.range->DoCompaction(majorCompaction);
    }
    else if (workInfo.type == MAINTENANCE) {
      workInfo.range->DoMaintenance();
    }
    else {
      assert(!"WorkType not implemented");
    }

    // protect msInputQueue with the mutex
    {
      boost::mutex::scoped_lock lock(msMutex);
      msInputQueue.pop();
    }
  }
  
}
