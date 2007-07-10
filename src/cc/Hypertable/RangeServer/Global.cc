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

#include "Global.h"

using namespace hypertable;

Comm                  *Global::comm = 0;
Properties            *Global::props = 0;
WorkQueue             *Global::workQueue;
HyperspaceClient      *Global::hyperspace = 0;
HdfsClient            *Global::hdfsClient = 0;
boost::thread         *Global::maintenanceThreadPtr = 0;
RangeServerProtocol   *Global::protocol = 0;
Global::TableInfoMapT  Global::tableInfoMap;
bool                   Global::verbose = false;
boost::mutex           Global::mutex;
Metadata              *Global::metadata = 0;
CommitLog             *Global::log = 0;
std::string            Global::logDirRoot = "";
std::string            Global::logDir = "";
uint64_t               Global::rangeMaxBytes = 0;
int32_t                Global::localityGroupMaxFiles = 0;
int32_t                Global::localityGroupMergeFiles = 0;
int32_t                Global::localityGroupMaxMemory = 0;
ScannerMap             Global::scannerMap;
FileBlockCache        *Global::blockCache = 0;
SplitLogMap            Global::splitLogMap;


/**
 *
 */
bool Global::GetTableInfo(string &name, TableInfoPtr &info) {
  boost::mutex::scoped_lock lock(mutex);
  TableInfoMapT::iterator iter = tableInfoMap.find(name);
  if (iter == tableInfoMap.end())
    return false;
  info = (*iter).second;
  return true;
}



/**
 *
 */
void Global::SetTableInfo(string &name, TableInfoPtr &info) {
  boost::mutex::scoped_lock lock(mutex);  
  TableInfoMapT::iterator iter = tableInfoMap.find(name);
  if (iter != tableInfoMap.end())
    tableInfoMap.erase(iter);
  tableInfoMap[name] = info;
}

