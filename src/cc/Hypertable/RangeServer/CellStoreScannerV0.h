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
#ifndef HYPERTABLE_CELLSTORESCANNERVERSION1_H
#define HYPERTABLE_CELLSTORESCANNERVERSION1_H

#include "Common/DynamicBuffer.h"

#include "CellStoreV0.h"
#include "CellListScanner.h"

namespace hypertable {

  class CellStore;
  class BlockInflater;

  class CellStoreScannerV0 : public CellListScanner {
  public:
    CellStoreScannerV0(CellStorePtr &cellStorePtr, ScanContextPtr &scanContextPtr);
    virtual ~CellStoreScannerV0();
    virtual void Lock() { return; }
    virtual void Unlock() { return; }
    virtual void Forward();
    virtual bool Get(ByteString32T **keyp, ByteString32T **valuep);
    virtual void Reset();
    using CellListScanner::RestrictRange;
    using CellListScanner::RestrictColumns;

  private:

    typedef struct {
      uint32_t offset;
      uint32_t zlength;
      uint8_t *base;
      uint8_t *ptr;
      uint8_t *end;
    } BlockInfoT;

    bool FetchNextBlock();

    bool Initialize();

    CellStorePtr            mCellStorePtr;
    CellStoreV0            *mCellStoreV0;
    CellStoreV0::IndexMapT  mIndex;

    CellStoreV0::IndexMapT::iterator mIter;

    BlockInfoT            mBlock;
    ByteString32T        *mCurKey;
    ByteString32T        *mCurValue;
    BlockInflater        *mBlockInflater;
    bool                  mCheckForRangeEnd;
    int                   mFileId;
    ByteString32T        *mStartKey;
    ByteString32T        *mEndKey;
  };

}

#endif // HYPERTABLE_CELLSTORESCANNERVERSION1_H

