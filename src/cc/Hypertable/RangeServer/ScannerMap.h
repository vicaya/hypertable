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
#ifndef HYPERTABLE_SCANNERMAP_H
#define HYPERTABLE_SCANNERMAP_H

#include <boost/thread/mutex.hpp>

#include <ext/hash_map>

#include "Common/atomic.h"

#include "CellListScanner.h"
#include "Range.h"

namespace hypertable {

  class ScannerMap {

  public:
    ScannerMap() : mMutex() { return; }
    uint32_t Put(CellListScannerPtr &scannerPtr, RangePtr &rangePtr);
    bool Get(uint32_t id, CellListScannerPtr &scannerPtr, RangePtr &rangePtr);
    bool Remove(uint32_t id);

  private:
    static atomic_t msNextId;

    boost::mutex   mMutex;
    typedef struct {
      CellListScannerPtr scannerPtr;
      RangePtr rangePtr;
    } ScanInfoT;
    typedef __gnu_cxx::hash_map<uint32_t, ScanInfoT> CellListScannerMapT;

    CellListScannerMapT mScannerMap;

  };

}


#endif // HYPERTABLE_SCANNERMAP_H
