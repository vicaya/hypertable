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
    CellStoreScannerV0(CellStoreV0 *store);
    virtual ~CellStoreScannerV0();
    virtual void Lock() { return; }
    virtual void Unlock() { return; }
    virtual void Forward();
    virtual bool Get(KeyT **keyp, ByteString32T **valuep);
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

    CellStoreV0            *mStore;
    HdfsClient             *mClient;
    int32_t                 mFd;
    HdfsProtocol           *mProtocol;
    std::string             mFilename;
    CellStoreV0::IndexMapT  mIndex;
    uint32_t                mDataEndOffset;

    CellStoreV0::IndexMapT::iterator mIter;

    BlockInfoT            mBlock;
    KeyT                 *mCurKey;
    ByteString32T        *mCurValue;
    BlockInflater        *mBlockInflater;
    bool                  mCheckForRangeEnd;
    int                   mFileId;
    KeyT                 *mStartKey;
    KeyT                 *mEndKey;
  };

}

#endif // HYPERTABLE_CELLSTORESCANNERVERSION1_H

