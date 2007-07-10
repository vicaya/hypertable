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
#ifndef HYPERTABLE_RANGESERVER_GLOBAL_H
#define HYPERTABLE_RANGESERVER_GLOBAL_H

#include <string>

#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include "Common/Properties.h"
#include "Common/WorkQueue.h"

#include "AsyncComm/Comm.h"

#include "Hyperspace/HyperspaceClient.h"

#include "Hypertable/Test/Metadata.h"
#include "Hypertable/Schema.h"
#include "Hypertable/HdfsClient/HdfsClient.h"
#include "CommitLog.h"
#include "FileBlockCache.h"
#include "RangeServerProtocol.h"
#include "ScannerMap.h"
#include "SplitLogMap.h"
#include "TableInfo.h"

namespace hypertable {

  class Global {
  public:

    typedef __gnu_cxx::hash_map<string, TableInfoPtr> TableInfoMapT;

    static hypertable::Comm *comm;
    static hypertable::Properties *props;
    static hypertable::WorkQueue  *workQueue;
    static hypertable::HyperspaceClient *hyperspace;
    static hypertable::HdfsClient  *hdfsClient;
    static boost::thread *maintenanceThreadPtr;
    static hypertable::RangeServerProtocol *protocol;
    static bool           verbose;
    static boost::mutex   mutex;
    static Metadata      *metadata;
    static TableInfoMapT  tableInfoMap;
    static CommitLog     *log;
    static std::string    logDirRoot;
    static std::string    logDir;
    static uint64_t       rangeMaxBytes;
    static int32_t        localityGroupMaxFiles;
    static int32_t        localityGroupMergeFiles;
    static int32_t        localityGroupMaxMemory;
    static ScannerMap     scannerMap;
    static hypertable::FileBlockCache *blockCache;
    static SplitLogMap    splitLogMap;

    static bool GetTableInfo(string &name, TableInfoPtr &info);
    static void SetTableInfo(string &name, TableInfoPtr &info);

  };
}


#endif // HYPERTABLE_RANGESERVER_GLOBAL_H

