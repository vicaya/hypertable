/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cassert>

#include "Common/Logger.h"

#include "MaintenanceThread.h"

using namespace hypertable;

std::queue<MaintenanceThread::CompactionInfoT> MaintenanceThread::msInputQueue;

boost::mutex      MaintenanceThread::msMutex;
boost::condition  MaintenanceThread::msCond;


void MaintenanceThread::ScheduleMaintenance(Range *range, uint64_t timestamp) {
  boost::mutex::scoped_lock lock(msMutex);
  CompactionInfoT workInfo;
  workInfo.type = MAINTENANCE;
  workInfo.range = range;
  workInfo.timestamp = timestamp;
  msInputQueue.push(workInfo);
  msCond.notify_one();
}


void MaintenanceThread::ScheduleCompaction(Range *range, WorkType wtype, uint64_t timestamp, std::string &lgName) {
  boost::mutex::scoped_lock lock(msMutex);
  CompactionInfoT workInfo;
  workInfo.type = wtype;
  workInfo.range = range;
  workInfo.timestamp = timestamp;
  workInfo.localityGroupName = lgName;
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

      if (workInfo.localityGroupName != "") {
	lg = workInfo.range->GetAccessGroup(workInfo.localityGroupName);
	lg->RunCompaction(workInfo.timestamp, majorCompaction);
      }
      else {
	vector<AccessGroup *> localityGroups = workInfo.range->AccessGroupVector();
	for (size_t i=0; i< localityGroups.size(); i++)
	  localityGroups[i]->RunCompaction(workInfo.timestamp, majorCompaction);
      }
    }
    else if (workInfo.type == MAINTENANCE) {
      workInfo.range->DoMaintenance(workInfo.timestamp);
    }
    else {
      assert(!"WorkType not implemented");
    }

    // protect msInputQueue with the mutex
    {
      boost::mutex::scoped_lock lock(msMutex);
      msInputQueue.pop();
      //workInfo.localityGroup->UnmarkBusy();
    }
  }
  
}
