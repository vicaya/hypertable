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

