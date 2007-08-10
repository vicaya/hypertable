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
#ifndef HYPERTABLE_RANGESERVER_GLOBAL_H
#define HYPERTABLE_RANGESERVER_GLOBAL_H

#include <string>

#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include "Common/Properties.h"

#include "AsyncComm/Comm.h"

#include "Hyperspace/HyperspaceClient.h"

#include "Hypertable/Test/Metadata.h"
#include "Hypertable/Schema.h"
#include "HdfsClient/HdfsClient.h"
#include "CommitLog.h"
#include "FileBlockCache.h"
#include "RangeServerProtocol.h"
#include "ScannerMap.h"
#include "TableInfo.h"

namespace hypertable {

  class ApplicationQueue;

  class Global {
  public:

    typedef __gnu_cxx::hash_map<string, TableInfoPtr> TableInfoMapT;

    static hypertable::ApplicationQueue *appQueue;
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

    static bool GetTableInfo(string &name, TableInfoPtr &info);
    static void SetTableInfo(string &name, TableInfoPtr &info);

  };
}


#endif // HYPERTABLE_RANGESERVER_GLOBAL_H

