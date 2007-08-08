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

#include <cassert>

#include "AsyncComm/ApplicationQueue.h"

#include "Global.h"

using namespace hypertable;

ApplicationQueue      *Global::appQueue;
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

