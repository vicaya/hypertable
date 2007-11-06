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

std::priority_queue<MaintenanceThread::WorkInfoT, std::vector<MaintenanceThread::WorkInfoT>, MaintenanceThread::ltWorkInfo> MaintenanceThread::ms_input_queue;

boost::mutex      MaintenanceThread::ms_mutex;
boost::condition  MaintenanceThread::ms_cond;


void MaintenanceThread::schedule_maintenance(Range *range) {
  boost::mutex::scoped_lock lock(ms_mutex);
  WorkInfoT workInfo;
  workInfo.type = MAINTENANCE;
  workInfo.range = range;
  boost::xtime_get(&workInfo.startTime, boost::TIME_UTC);
  ms_input_queue.push(workInfo);
  ms_cond.notify_one();
}


void MaintenanceThread::schedule_compaction(Range *range, WorkType wtype) {
  boost::mutex::scoped_lock lock(ms_mutex);
  WorkInfoT workInfo;
  workInfo.type = wtype;
  workInfo.range = range;
  boost::xtime_get(&workInfo.startTime, boost::TIME_UTC);
  ms_input_queue.push(workInfo);
  ms_cond.notify_one();
}


/**
 *
 */
void MaintenanceThread::operator()() {
  WorkInfoT workInfo;
  boost::xtime now, nextWork;

  while (true) {

    // while the queue is empty or the top of the queue is not ready, wait
    {
      boost::mutex::scoped_lock lock(ms_mutex);

      boost::xtime_get(&now, boost::TIME_UTC);

      while (ms_input_queue.empty() || xtime_cmp((ms_input_queue.top()).startTime, now) > 0) {
	if (ms_input_queue.empty())
	  ms_cond.wait(lock);
	else {
	  nextWork = (ms_input_queue.top()).startTime;
	  ms_cond.timed_wait(lock, nextWork);
	}
	boost::xtime_get(&now, boost::TIME_UTC);
      }

      workInfo = ms_input_queue.top();
      ms_input_queue.pop();
    }

    if (workInfo.type == COMPACTION_MAJOR || workInfo.type == COMPACTION_MINOR) {
      bool majorCompaction = (workInfo.type == COMPACTION_MAJOR) ? true : false;
      workInfo.range->do_compaction(majorCompaction);
    }
    else if (workInfo.type == MAINTENANCE) {
      workInfo.range->do_maintenance();
    }
    else {
      assert(!"WorkType not implemented");
    }

  }
  
}
