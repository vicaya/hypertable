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

#ifndef HYPERTABLE_RANGESERVER_H
#define HYPERTABLE_RANGESERVER_H

#include "Common/Properties.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/ResponseCallback.h"

#include "Hyperspace/HyperspaceClient.h"

#include "Hypertable/Lib/Types.h"

#include "ResponseCallbackCreateScanner.h"
#include "ResponseCallbackFetchScanblock.h"
#include "ResponseCallbackUpdate.h"
#include "TableInfo.h"

using namespace hypertable;

namespace hypertable {

  class RangeServer {
  public:
    RangeServer(ConnectionManager *connManager, PropertiesPtr &propsPtr);

    void Compact(ResponseCallback *cb, RangeSpecificationT *rangeSpec, uint8_t compactionType);
    void CreateScanner(ResponseCallbackCreateScanner *cb, RangeSpecificationT *rangeSpec, ScanSpecificationT *spec);
    void FetchScanblock(ResponseCallbackFetchScanblock *cb, uint32_t scannerId);
    void LoadRange(ResponseCallback *cb, RangeSpecificationT *rangeSpec);
    void Update(ResponseCallbackUpdate *cb, RangeSpecificationT *rangeSpec, BufferT &buffer);

  private:
    int DirectoryInitialize(Properties *props);

    bool GetTableInfo(std::string &name, TableInfoPtr &info);
    bool GetTableInfo(const char *name, TableInfoPtr &info) {
      std::string nameStr = name;
      return GetTableInfo(nameStr, info);
    }

    void SetTableInfo(std::string &name, TableInfoPtr &info);
    void SetTableInfo(const char *name, TableInfoPtr &info) {
      std::string nameStr = name;
      SetTableInfo(nameStr, info);
    }

    typedef __gnu_cxx::hash_map<string, TableInfoPtr> TableInfoMapT;

    boost::mutex   mMutex;
    bool mVerbose;
    HyperspaceClient *mHyperspace;
    TableInfoMapT mTableInfoMap;
  };

}

#endif // HYPERTABLE_RANGESERVER_H
