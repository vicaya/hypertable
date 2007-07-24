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
#include <cassert>

#include <boost/scoped_ptr.hpp>

extern "C" {
#include <string.h>
}

#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/System.h"

#include "Hypertable/HdfsClient/HdfsClient.h"

#include "BlockDeflaterZlib.h"
#include "BlockInflaterZlib.h"
#include "CellStoreScannerV0.h"
#include "CellStoreV0.h"
#include "Constants.h"
#include "FileBlockCache.h"

using namespace hypertable;

CellStoreV0::CellStoreV0(HdfsClient *client) : mClient(client), mFilename(), mFd(-1), mIndex(),
  mBuffer(0), mFixIndexBuffer(0), mVarIndexBuffer(0), mBlockSize(Constants::DEFAULT_BLOCKSIZE),
  mHandler(0), mOutstandingId(0), mOffset(0), mGotFirstIndex(false), mFileLength(0),
  mDiskUsage(0), mSplitKey(0), mFileId(0), mStartKeyPtr(0), mEndKeyPtr(0) {
  mProtocol = mClient->GetProtocolObject();
  mBlockDeflater = new BlockDeflaterZlib();
  mFileId = FileBlockCache::GetNextFileId();
}



CellStoreV0::~CellStoreV0() {
  delete mBlockDeflater;
  delete mHandler;
}



uint64_t CellStoreV0::GetLogCutoffTime() {
  return mTrailer.timestamp;
}



uint16_t CellStoreV0::GetFlags() {
  return mTrailer.flags;
}

KeyT *CellStoreV0::GetSplitKey() {
  return mSplitKey.get();
}



int CellStoreV0::Create(const char *fname, size_t blockSize) {

  mBlockSize = blockSize;
  mBuffer.reserve(mBlockSize*2);

  mHandler = new CallbackHandlerSynchronizer();
  mFd = -1;
  mOffset = 0;
  mGotFirstIndex = false;
  mFixIndexBuffer.reserve(mBlockSize);
  mVarIndexBuffer.reserve(mBlockSize);

  memset(&mTrailer, 0, sizeof(mTrailer));

  mFilename = fname;

  return mClient->Create(mFilename.c_str(), true, -1, -1, -1, &mFd);

}


int CellStoreV0::Add(const KeyT *key, const ByteString32T *value) {
  int error;
  Event *event = 0;
  DynamicBuffer zBuffer(0);

  if (!mGotFirstIndex) {
    AddIndexEntry(key, mOffset);
    mGotFirstIndex = true;
  }

  if (mBuffer.fill() > mBlockSize) {

    mBlockDeflater->deflate(mBuffer, zBuffer, Constants::DATA_BLOCK_MAGIC);
    mBuffer.clear();

    if (mOutstandingId != 0) {
      if ((event = mHandler->WaitForReply()) != 0) {
	LOG_VA_ERROR("Problem writing to HDFS file '%s' : %s", mFilename.c_str(), mProtocol->StringFormatMessage(event).c_str());
	delete event;
	return -1;
      }
    }

    size_t  zlen;
    uint8_t *zbuf = zBuffer.release(&zlen);

    if ((error = mClient->Write(mFd, zbuf, zlen, mHandler, &mOutstandingId)) != Error::OK) {
      LOG_VA_ERROR("Problem writing to HDFS file '%s' : %s", mFilename.c_str(), Error::GetText(error));
      return -1;
    }
    mOffset += zlen;
    AddIndexEntry(key, mOffset);
  }

  size_t keyLen = sizeof(int32_t) + key->len;
  size_t valueLen = sizeof(int32_t) + value->len;

  mBuffer.ensure( keyLen + valueLen );

  memcpy(mBuffer.ptr, key, keyLen);
  mBuffer.ptr += keyLen;

  memcpy(mBuffer.ptr, value, valueLen);
  mBuffer.ptr += valueLen;

  return 0;
}



int CellStoreV0::Finalize(uint64_t timestamp) {
  Event *event = 0;
  int error = -1;
  uint8_t *zbuf;
  size_t zlen;
  DynamicBuffer zBuffer(0);
  size_t len;
  uint8_t *base;

  if (mBuffer.fill() > 0) {

    mBlockDeflater->deflate(mBuffer, zBuffer, Constants::DATA_BLOCK_MAGIC);
    zbuf = zBuffer.release(&zlen);

    if (mOutstandingId != 0 && (event = mHandler->WaitForReply()) != 0) {
      LOG_VA_ERROR("Problem writing to HDFS file '%s' : %s", mFilename.c_str(), mProtocol->StringFormatMessage(event).c_str());
      goto abort;
    }

    if ((error = mClient->Write(mFd, zbuf, zlen, mHandler, &mOutstandingId)) != Error::OK) {
      LOG_VA_ERROR("Problem writing to HDFS file '%s' : %s", mFilename.c_str(), mProtocol->StringFormatMessage(event).c_str());
      goto abort;
    }
    mOffset += zlen;
  }

  mTrailer.fixIndexOffset = mOffset;
  mTrailer.timestamp = timestamp;
  mTrailer.flags = 0;
  mTrailer.compressionType = Constants::COMPRESSION_TYPE_ZLIB;
  mTrailer.version = 1;

  /**
   * Chop the Index buffers down to the exact length
   */
  base = mFixIndexBuffer.release(&len);
  mFixIndexBuffer.reserve(len);
  mFixIndexBuffer.addNoCheck(base, len);
  delete [] base;
  base = mVarIndexBuffer.release(&len);
  mVarIndexBuffer.reserve(len);
  mVarIndexBuffer.addNoCheck(base, len);
  delete [] base;

  /**
   * Write fixed index
   */
  mBlockDeflater->deflate(mFixIndexBuffer, zBuffer, Constants::INDEX_FIXED_BLOCK_MAGIC);
  zbuf = zBuffer.release(&zlen);

  /**
   * wait for last Client op
   */
  if (mOutstandingId != 0 && (event = mHandler->WaitForReply()) != 0) {
    LOG_VA_ERROR("Problem writing to HDFS file '%s' : %s", mFilename.c_str(), mProtocol->StringFormatMessage(event).c_str());
    goto abort;
  }

  if (mClient->Write(mFd, zbuf, zlen, mHandler, &mOutstandingId) != Error::OK)
    goto abort;
  mOffset += zlen;

  /**
   * Write variable index + trailer
   */
  mTrailer.varIndexOffset = mOffset;
  mBlockDeflater->deflate(mVarIndexBuffer, zBuffer, Constants::INDEX_VARIABLE_BLOCK_MAGIC, sizeof(mTrailer));

  // wait for fixed index write
  if (mOutstandingId == 0 || !mHandler->WaitForReply(&event)) {
    LOG_VA_ERROR("Problem writing fixed index to HDFS file '%s' : %s", mFilename.c_str(), mProtocol->StringFormatMessage(event).c_str());
    goto abort;
  }
  delete event;
  event = 0;

  /**
   * Set up mIndex map
   */
  uint32_t offset;
  KeyT *key;
  mFixIndexBuffer.ptr = mFixIndexBuffer.buf;
  mVarIndexBuffer.ptr = mVarIndexBuffer.buf;
  for (size_t i=0; i<mTrailer.indexEntries; i++) {
    // variable portion
    key = (KeyT *)mVarIndexBuffer.ptr;
    mVarIndexBuffer.ptr += sizeof(int32_t) + key->len;
    // fixed portion (e.g. offset)
    memcpy(&offset, mFixIndexBuffer.ptr, sizeof(offset));
    mFixIndexBuffer.ptr += sizeof(offset);
    mIndex.insert(mIndex.end(), IndexMapT::value_type(key, offset));
    if (i == mTrailer.indexEntries/2) {
      mTrailer.splitKeyOffset = mVarIndexBuffer.ptr - mVarIndexBuffer.buf;
      RecordSplitKey(mVarIndexBuffer.ptr);
    }
  }

  // deallocate fix index data
  delete [] mFixIndexBuffer.release();

  zBuffer.add(&mTrailer, sizeof(mTrailer));

  zbuf = zBuffer.release(&zlen);

  if (mClient->Write(mFd, zbuf, zlen) != Error::OK)
    goto abort;
  mOffset += zlen;

  /** close file for writing **/
  if (mClient->Close(mFd) != Error::OK)
    goto abort;

  /** Set file length **/
  mFileLength = mOffset;

  /** Re-open file for reading **/
  if ((error = mClient->Open(mFilename.c_str(), &mFd)) != Error::OK)
    goto abort;

  mDiskUsage = (uint32_t)mFileLength;

  error = 0;

 abort:
  delete event;
  return error;
}



/**
 *
 */
void CellStoreV0::AddIndexEntry(const KeyT *key, uint32_t offset) {

  size_t keyLen = sizeof(int32_t) + key->len;
  mVarIndexBuffer.ensure( keyLen );
  memcpy(mVarIndexBuffer.ptr, key, keyLen);
  mVarIndexBuffer.ptr += keyLen;

  // Serialize offset into fix index buffer
  mFixIndexBuffer.ensure(sizeof(offset));
  memcpy(mFixIndexBuffer.ptr, &offset, sizeof(offset));
  mFixIndexBuffer.ptr += sizeof(offset);

  mTrailer.indexEntries++;
}



/**
 *
 */
int CellStoreV0::Open(const char *fname, const KeyT *startKey, const KeyT *endKey) {
  int error = 0;

  if (startKey != 0)
    mStartKeyPtr.reset( CreateCopy(startKey) );

  if (endKey != 0)
    mEndKeyPtr.reset( CreateCopy(endKey) );

  mFd = -1;

  mFilename = fname;

  /** Get the file length **/
  if ((error = mClient->Length(mFilename.c_str(), (int64_t *)&mFileLength)) != Error::OK)
    goto abort;

  if (mFileLength < sizeof(StoreTrailerT)) {
    LOG_VA_ERROR("Bad length of CellStore file '%s' - %lld", mFilename.c_str(), mFileLength);
    goto abort;
  }

  /** Open the HDFS file **/
  if ((error = mClient->Open(mFilename.c_str(), &mFd)) != Error::OK)
    goto abort;

  /** Read trailer **/
  uint32_t len;
  if ((error = mClient->Pread(mFd, mFileLength-sizeof(StoreTrailerT), sizeof(StoreTrailerT), (uint8_t *)&mTrailer, &len)) != Error::OK)
    goto abort;

  if (len != sizeof(StoreTrailerT)) {
    LOG_VA_ERROR("Problem reading trailer for CellStore file '%s' - only read %d of %d bytes", mFilename.c_str(), len, sizeof(StoreTrailerT));
    goto abort;
  }

  /** Sanity check trailer **/
  if (mTrailer.version != 1) {
    LOG_VA_ERROR("Unsupported CellStore version (%d) for file '%s'", mTrailer.version, fname);
    goto abort;
  }
  if (mTrailer.compressionType != Constants::COMPRESSION_TYPE_ZLIB) {
    LOG_VA_ERROR("Unsupported CellStore compression type (%d) for file '%s'", mTrailer.compressionType, fname);
    goto abort;
  }
  if (!(mTrailer.fixIndexOffset < mTrailer.varIndexOffset &&
	mTrailer.varIndexOffset < mFileLength)) {
    LOG_VA_ERROR("Bad index offsets in CellStore trailer fix=%lld, var=%lld, length=%lld, file='%s'",
		 mTrailer.fixIndexOffset, mTrailer.varIndexOffset, mFileLength, fname);
    goto abort;
  }

#if 0
  cout << "mIndex.size() = " << mIndex.size() << endl;
  cout << "Fixed Index Offset: " << mTrailer.fixIndexOffset << endl;
  cout << "Variable Index Offset: " << mTrailer.varIndexOffset << endl;
  cout << "Replaced Files Offset: " << mTrailer.replacedFilesOffset << endl;
  cout << "Replaced Files Count: " << mTrailer.replacedFilesCount << endl;
  cout << "Number of Index Entries: " << mTrailer.indexEntries << endl;
  cout << "Flags: " << mTrailer.flags << endl;
  cout << "Compression Type: " << mTrailer.compressionType << endl;
  cout << "Version: " << mTrailer.version << endl;
  for (size_t i=0; i<mReplacedFiles.size(); i++)
    cout << "Replaced File: '" << mReplacedFiles[i] << "'" << endl;
#endif

  return Error::OK;

 abort:
  return Error::LOCAL_IO_ERROR;
}


int CellStoreV0::LoadIndex() {
  int error = -1;
  uint8_t *buf = 0;
  uint32_t amount;
  uint8_t *vbuf = 0;
  uint8_t *fEnd;
  uint8_t *vEnd;
  uint32_t len;
  BlockInflaterZlib *inflater = new BlockInflaterZlib();

  amount = (mFileLength-sizeof(StoreTrailerT)) - mTrailer.fixIndexOffset;
  buf = new uint8_t [ amount ];

  /** Read index data **/
  if ((error = mClient->Pread(mFd, mTrailer.fixIndexOffset, amount, buf, &len)) != Error::OK)
    goto abort;

  if (len != amount) {
    LOG_VA_ERROR("Problem loading index for CellStore '%s' : tried to read %d but only got %d", mFilename.c_str(), amount, len);
    goto abort;
  }

  /** inflate fixed index **/
  if (!inflater->inflate(buf, mTrailer.varIndexOffset-mTrailer.fixIndexOffset, Constants::INDEX_FIXED_BLOCK_MAGIC, mFixIndexBuffer))
    goto abort;

  vbuf = buf + (mTrailer.varIndexOffset-mTrailer.fixIndexOffset);
  amount = (mFileLength-sizeof(StoreTrailerT)) - mTrailer.varIndexOffset;

  /** inflate variable index **/
  if (!inflater->inflate(vbuf, amount, Constants::INDEX_VARIABLE_BLOCK_MAGIC, mVarIndexBuffer))
    goto abort;

  mIndex.clear();

  KeyT *key;
  uint32_t offset;

  // record end offsets for sanity checking and reset ptr
  fEnd = mFixIndexBuffer.ptr;
  mFixIndexBuffer.ptr = mFixIndexBuffer.buf;
  vEnd = mVarIndexBuffer.ptr;
  mVarIndexBuffer.ptr = mVarIndexBuffer.buf;

  for (size_t i=0; i< mTrailer.indexEntries; i++) {

    assert(mFixIndexBuffer.ptr < fEnd);
    assert(mVarIndexBuffer.ptr < vEnd);

    // Deserialized cell key (variable portion)
    key = (KeyT *)mVarIndexBuffer.ptr;
    mVarIndexBuffer.ptr += sizeof(int32_t) + key->len;

    // Deserialize offset
    memcpy(&offset, mFixIndexBuffer.ptr, sizeof(offset));
    mFixIndexBuffer.ptr += sizeof(offset);

    mIndex.insert(mIndex.end(), IndexMapT::value_type(key, offset));

  }

  /**
   * Compute disk usage
   */
  {
    uint32_t start = 0;
    uint32_t end = (uint32_t)mFileLength;
    CellStoreV0::IndexMapT::iterator iter;
    if (mStartKeyPtr.get() != 0) {
      iter = mIndex.upper_bound(mStartKeyPtr.get());
      iter--;
      start = (*iter).second;
    }
    if (mEndKeyPtr.get() != 0) {
      iter = mIndex.lower_bound(mEndKeyPtr.get());
      end = (*iter).second;
    }
    mDiskUsage = end - start;
  }

  RecordSplitKey(mVarIndexBuffer.buf + mTrailer.splitKeyOffset);

#if 0
  cout << "mIndex.size() = " << mIndex.size() << endl;
  cout << "Fixed Index Offset: " << mTrailer.fixIndexOffset << endl;
  cout << "Variable Index Offset: " << mTrailer.varIndexOffset << endl;
  cerr << "Split Key Offset: " << mTrailer.splitKeyOffset << endl;
  cout << "Number of Index Entries: " << mTrailer.indexEntries << endl;
  cout << "Flags: " << mTrailer.flags << endl;
  cout << "Compression Type: " << mTrailer.compressionType << endl;
  cout << "Version: " << mTrailer.version << endl;
#endif

  error = 0;

 abort:
  delete inflater;
  delete [] mFixIndexBuffer.release();
  delete [] buf;
  return error;
}


int CellStoreV0::Close() {
  int error = Error::OK;
  if (mFd != -1)
    error = mClient->Close(mFd);
  return error;
}



/**
 * 
 */
CellListScanner *CellStoreV0::CreateScanner(bool suppressDeleted) {
  return new CellStoreScannerV0(this);
}


void CellStoreV0::RecordSplitKey(const uint8_t *keyBytes) {
  mSplitKey.reset( CreateCopy((KeyT *)keyBytes) );
}
