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

#ifndef HYPERTABLE_CELLSTOREVERSION1_H
#define HYPERTABLE_CELLSTOREVERSION1_H

#include <map>
#include <string>
#include <vector>

#include "AsyncComm/CallbackHandlerSynchronizer.h"
#include "Common/DynamicBuffer.h"
#include "Hypertable/HdfsClient/HdfsClient.h"

#include "BlockDeflater.h"
#include "CellStore.h"
#include "Constants.h"
#include "Key.h"

using namespace hypertable;

/**
 * Forward declarations
 */
namespace hypertable {
  class Client;
  class Protocol;
}

namespace hypertable {

  class CellStoreV0 : public CellStore {

  public:

    CellStoreV0(HdfsClient *client);
    virtual ~CellStoreV0();

    int Create(const char *fname, size_t blockSize=Constants::DEFAULT_BLOCKSIZE);
    virtual int Add(const KeyT *key, const ByteString32T *value);
    int Finalize(uint64_t timestamp);

    virtual int Open(const char *fname, const KeyT *startKey, const KeyT *endKey);
    virtual int LoadIndex();
    virtual CellListScanner *CreateScanner(bool suppressDeleted=false);
    virtual uint64_t GetLogCutoffTime();
    virtual uint64_t DiskUsage() { return mDiskUsage; }
    virtual KeyT *GetSplitKey();
    virtual std::string &GetFilename() { return mFilename; }
    virtual uint16_t GetFlags();
    virtual int Close();

    friend class CellStoreScannerV0;

  protected:

    void AddIndexEntry(const KeyT *key, uint32_t offset);
    void RecordSplitKey(const uint8_t *keyBytes);

    typedef struct {
      uint32_t  fixIndexOffset;
      uint32_t  varIndexOffset;
      uint32_t  splitKeyOffset;
      uint32_t  indexEntries;
      uint64_t  timestamp;
      uint16_t  flags;
      uint16_t  compressionType;
      uint16_t  version;
    } __attribute__((packed)) StoreTrailerT;

    typedef map<KeyT *,uint32_t, ltKey> IndexMapT;

    HdfsClient            *mClient;
    HdfsProtocol          *mProtocol;
    std::string            mFilename;
    int32_t                mFd;
    IndexMapT              mIndex;
    StoreTrailerT          mTrailer;
    BlockDeflater         *mBlockDeflater;
    DynamicBuffer          mBuffer;
    DynamicBuffer          mFixIndexBuffer;
    DynamicBuffer          mVarIndexBuffer;
    size_t                 mBlockSize;
    CallbackHandlerSynchronizer  *mHandler;
    uint32_t               mOutstandingId;
    uint32_t               mOffset;
    bool                   mGotFirstIndex;
    uint64_t               mFileLength;
    uint32_t               mDiskUsage;
    KeyPtr                 mSplitKey;
    int                    mFileId;
    KeyPtr                 mStartKeyPtr;
    KeyPtr                 mEndKeyPtr;
  };

}

#endif // HYPERTABLE_CELLSTOREVERSION1_H
