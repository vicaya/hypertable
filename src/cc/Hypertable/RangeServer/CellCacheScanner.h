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
#ifndef HYPERTABLE_CELLCACHESCANNER_H
#define HYPERTABLE_CELLCACHESCANNER_H

#include "CellCache.h"
#include "CellListScanner.h"
#include "ScanContext.h"

using namespace hypertable;

namespace hypertable {

  /**
   * CellCacheScanner is a class that provides a scanning interface to
   * a CellCache.
   */
  class CellCacheScanner : public CellListScanner {
  public:
    CellCacheScanner(CellCachePtr &cellCachePtr, ScanContextPtr &scanContextPtr);
    virtual ~CellCacheScanner() { return; }
    virtual void Forward();
    virtual bool Get(ByteString32T **keyp, ByteString32T **valuep);

  private:
    CellCache::CellMapT::iterator  mStartIter;
    CellCache::CellMapT::iterator  mEndIter;
    CellCache::CellMapT::iterator  mCurIter;
    CellCachePtr                   mCellCachePtr;
    boost::mutex                  &mCellCacheMutex;
    const ByteString32T           *mCurKey;
    const ByteString32T           *mCurValue;
    bool                           mEos;

  };
}

#endif // HYPERTABLE_CELLCACHESCANNER_H

