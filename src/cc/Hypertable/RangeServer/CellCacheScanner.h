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

