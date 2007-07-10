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
#ifndef HYPERTABLE_CELLCACHESCANNER_H
#define HYPERTABLE_CELLCACHESCANNER_H

#include "CellCache.h"
#include "CellListScanner.h"

using namespace hypertable;

namespace hypertable {

  class CellCacheScanner : public CellListScanner {
  public:
    CellCacheScanner(CellCache *memtable);
    virtual ~CellCacheScanner() { return; }
    virtual void Forward();
    virtual bool Get(KeyT **keyp, ByteString32T **valuep);
    virtual void Reset();
    using CellListScanner::RestrictRange;
    using CellListScanner::RestrictColumns;

  private:
    CellCache::CellMapT::iterator      mStartIter;
    CellCache::CellMapT::iterator      mEndIter;
    CellCache::CellMapT::iterator      mCurIter;
    CellCache                         *mTable;
  };
}

#endif // HYPERTABLE_CELLCACHESCANNER_H

