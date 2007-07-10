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
#ifndef HYPERTABLE_CELLCACHE_H
#define HYPERTABLE_CELLCACHE_H

#include <map>

#include "Key.h"
#include "CellListScanner.h"
#include "CellList.h"

namespace hypertable {

  class CellCache : public CellList {

  public:
    CellCache() : CellList(), mMemoryUsed(0) {
      mCellMap = new CellMapT;
    }
    virtual ~CellCache() { delete mCellMap; }
    virtual int Add(const KeyT *key, const ByteString32T *value);
    virtual CellListScanner *CreateScanner(bool suppressDeleted=false);

    void Display();
    uint64_t MemoryUsed() { return mMemoryUsed; }
    void Purge(uint64_t timestamp);

    friend class CellCacheScanner;

  protected:

    typedef std::map<KeyPtr, ByteString32Ptr, ltKeyPtr> CellMapT;

    CellMapT  *mCellMap;
    uint64_t   mMemoryUsed;
  };

}

#endif // HYPERTABLE_CELLCACHE_H

