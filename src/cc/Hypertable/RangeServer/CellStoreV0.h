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

#ifndef HYPERTABLE_CELLSTOREVERSION1_H
#define HYPERTABLE_CELLSTOREVERSION1_H

#include <map>
#include <string>
#include <vector>

#include "AsyncComm/DispatchHandlerSynchronizer.h"
#include "Common/DynamicBuffer.h"
#include "HdfsClient/HdfsClient.h"

#include "BlockDeflater.h"
#include "CellStore.h"

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

    virtual int Create(const char *fname, size_t blockSize=Constants::DEFAULT_BLOCKSIZE);
    virtual int Add(const ByteString32T *key, const ByteString32T *value);
    virtual int Finalize(uint64_t timestamp);

    virtual int Open(const char *fname, const ByteString32T *startKey, const ByteString32T *endKey);
    virtual int LoadIndex();
    virtual uint64_t GetLogCutoffTime();
    virtual uint64_t DiskUsage() { return mDiskUsage; }
    virtual ByteString32T *GetSplitKey();
    virtual std::string &GetFilename() { return mFilename; }
    virtual uint16_t GetFlags();

    /**
     * Creates a CellStoreScannerV0 object that contains an shared pointer (intrusive_ptr)
     * to this CellStore;
     */
    virtual CellListScanner *CreateScanner(ScanContextPtr &scanContextPtr);

    friend class CellStoreScannerV0;

  protected:

    void AddIndexEntry(const ByteString32T *key, uint32_t offset);
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

    typedef map<ByteString32T *, uint32_t, ltByteString32> IndexMapT;

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
    DispatchHandlerSynchronizer  mSyncHandler;
    uint32_t               mOutstandingId;
    uint32_t               mOffset;
    ByteString32T         *mLastKey;
    uint64_t               mFileLength;
    uint32_t               mDiskUsage;
    ByteString32Ptr        mSplitKey;
    int                    mFileId;
    ByteString32Ptr        mStartKeyPtr;
    ByteString32Ptr        mEndKeyPtr;
  };

}

#endif // HYPERTABLE_CELLSTOREVERSION1_H
