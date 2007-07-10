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
#ifndef HYPERTABLE_CELLLISTSCANNER_H
#define HYPERTABLE_CELLLISTSCANNER_H

#include <boost/shared_array.hpp>
#include <boost/shared_ptr.hpp>

#include "Common/DynamicBuffer.h"

#include "Key.h"

using namespace hypertable;

namespace hypertable {

  class CellListScanner {
  public:
    CellListScanner();
    virtual ~CellListScanner() { return; }
    virtual void RestrictRange(KeyT *start, KeyT *end);
    virtual void RestrictColumns(const uint8_t *families, size_t count);
    virtual void Forward() = 0;
    virtual bool Get(KeyT **keyp, ByteString32T **valuep) = 0;
    virtual void Reset() = 0;

  protected:
    bool  mReset;
    KeyT *mRangeStart;
    KeyT *mRangeEnd;
    KeyPtr mRangeStartPtr;
    KeyPtr mRangeEndPtr;
    boost::shared_array<bool> mFamilyMask;
  };

  typedef boost::shared_ptr<CellListScanner> CellListScannerPtr;

}

#endif // HYPERTABLE_CELLLISTSCANNER_H

