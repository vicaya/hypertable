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

