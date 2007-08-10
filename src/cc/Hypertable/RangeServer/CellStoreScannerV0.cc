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

#include <cassert>

#include "Common/Error.h"
#include "Common/System.h"

#include "HdfsClient/HdfsClient.h"

#include "BlockInflaterZlib.h"
#include "Global.h"
#include "CellStoreScannerV0.h"

using namespace hypertable;

CellStoreScannerV0::CellStoreScannerV0(CellStorePtr &cellStorePtr) : CellListScanner(), mCellStorePtr(cellStorePtr), mCurKey(0), mCurValue(0), mCheckForRangeEnd(false) {

  mCellStoreV0 = dynamic_cast< CellStoreV0*>(mCellStorePtr.get());
  assert(mCellStoreV0);

  mIndex = mCellStoreV0->mIndex;
  mFileId = mCellStoreV0->mFileId;
  mBlockInflater = new BlockInflaterZlib();
  memset(&mBlock, 0, sizeof(mBlock));
}


CellStoreScannerV0::~CellStoreScannerV0() {
  if (mBlock.base != 0)
    Global::blockCache->Checkin(mFileId, mBlock.offset);
  delete mBlockInflater;
}


/**
 *
 */
void CellStoreScannerV0::Reset() {
  ByteString32T *tmpKey;

  // compute mStartKey
  tmpKey = mCellStoreV0->mStartKeyPtr.get();
  if (mRangeStart != 0)
    mStartKey = (tmpKey != 0 && *mRangeStart < *tmpKey) ? tmpKey : mRangeStart;
  else if (tmpKey != 0)
    mStartKey = tmpKey;
  else
    mStartKey = 0;

  // compute mEndKey
  tmpKey = mCellStoreV0->mEndKeyPtr.get();
  if (mRangeEnd != 0)
    mEndKey = (tmpKey != 0 && *tmpKey < *mRangeEnd) ? tmpKey : mRangeEnd;
  else if (tmpKey != 0)
    mEndKey = tmpKey;
  else
    mEndKey = 0;

  if (mBlock.base != 0)
    Global::blockCache->Checkin(mFileId, mBlock.offset);
  memset(&mBlock, 0, sizeof(mBlock));

  mCheckForRangeEnd = false;

  if (mStartKey != 0) {
    mIter = mIndex.upper_bound(mStartKey);
    mIter--;
  }
  else
    mIter = mIndex.begin();

  mCurKey = 0;
  mReset = true;
}



bool CellStoreScannerV0::Get(ByteString32T **keyp, ByteString32T **valuep) {

  assert(mReset);

  if (mCurKey == 0) {
    if (!Initialize())
      return false;
  }

  if (mIter == mIndex.end())
    return false;

  *keyp = mCurKey;
  *valuep = mCurValue;

  return true;
}



void CellStoreScannerV0::Forward() {
  KeyComponentsT keyComps;

  while (true) {

    if (mIter == mIndex.end())
      return;

    mBlock.ptr = ((uint8_t *)mCurValue) + Length(mCurValue);    

    if (mBlock.ptr >= mBlock.end) {
      if (!FetchNextBlock())
	return;
    }

    mCurKey = (ByteString32T *)mBlock.ptr;
    mCurValue = (ByteString32T *)(mBlock.ptr + Length(mCurKey));

    if (mCheckForRangeEnd) {
      if (!(*mCurKey < *mEndKey)) {
	mIter = mIndex.end();
	break;
      }
    }

    /**
     * Column family check
     */
    if (!Load(mCurKey, keyComps)) {
      LOG_ERROR("Problem parsing key!");
      break;
    }
    if (mFamilyMask[keyComps.columnFamily])
      break;
  }
}



/**
 * This method fetches the 'next' compressed block of key/value pairs from
 * the underlying CellStore.
 *
 * Preconditions required to call this method:
 *  1. mBlock is cleared and mIter points to the mIndex entry of the first block to fetch
 *    'or'
 *  2. mBlock is loaded with the current block and mIter points to the mIndex entry of
 *     the current block
 *
 * @return true if next block successfully fetched, false if no next block
 */
bool CellStoreScannerV0::FetchNextBlock() {
  uint8_t *buf = 0;
  bool rval = false;

  // If we're at the end of the current block, deallocate and move to next
  if (mBlock.base != 0 && mBlock.ptr >= mBlock.end) {
    Global::blockCache->Checkin(mFileId, mBlock.offset);
    memset(&mBlock, 0, sizeof(mBlock));
    mIter++;
  }

  if (mBlock.base == 0 && mIter != mIndex.end()) {
    DynamicBuffer expandBuffer(0);
    uint32_t len;

    mBlock.offset = (*mIter).second;

    CellStoreV0::IndexMapT::iterator iterNext = mIter;
    iterNext++;
    if (iterNext == mIndex.end()) {
      mBlock.zlength = mCellStoreV0->mTrailer.fixIndexOffset - mBlock.offset;
      if (mEndKey != 0)
	mCheckForRangeEnd = true;
    }
    else {
      if (mEndKey != 0 && !(*((*iterNext).first) < *mEndKey))
	mCheckForRangeEnd = true;
      mBlock.zlength = (*iterNext).second - mBlock.offset;
    }

    /**
     * Cache lookup / block read
     */
    if (!Global::blockCache->Checkout(mFileId, (uint32_t)mBlock.offset, &mBlock.base, &len)) {
      /** Read compressed block **/
      buf = new uint8_t [ mBlock.zlength ];
      uint32_t nread;
      if (mCellStoreV0->mClient->Pread(mCellStoreV0->mFd, mBlock.offset, mBlock.zlength, buf, &nread) != Error::OK)
	goto abort;

      /** inflate compressed block **/
      if (!mBlockInflater->inflate(buf, mBlock.zlength, Constants::DATA_BLOCK_MAGIC, expandBuffer))
	goto abort;

      /** take ownership of inflate buffer **/
      size_t fill;
      mBlock.base = expandBuffer.release(&fill);
      len = fill;

      /** Insert block into cache  **/
      if (!Global::blockCache->InsertAndCheckout(mFileId, (uint32_t)mBlock.offset, mBlock.base, len)) {
	delete [] mBlock.base;
	if (!Global::blockCache->Checkout(mFileId, (uint32_t)mBlock.offset, &mBlock.base, &len)) {
	  LOG_VA_ERROR("Problem checking out block from cache fileId=%d, offset=%ld", mFileId, (uint32_t)mBlock.offset);
	  DUMP_CORE;
	}
      }
    }
    mBlock.ptr = mBlock.base;
    mBlock.end = mBlock.base + len;

    rval = true;
  }

 abort:
  delete [] buf;
  return rval;
}



bool CellStoreScannerV0::Initialize() {

  if (!FetchNextBlock())
    return false;

  /**
   * Seek to start of range in block
   */
  mCurKey = (ByteString32T *)mBlock.ptr;
  mCurValue = (ByteString32T *)(mBlock.ptr + Length(mCurKey));
  if (mStartKey != 0) {
    while (*mCurKey < *mStartKey) {
      mBlock.ptr = ((uint8_t *)mCurValue) + Length(mCurValue);
      if (mBlock.ptr >= mBlock.end)
	break;
      mCurKey = (ByteString32T *)mBlock.ptr;
      mCurValue = (ByteString32T *)(mBlock.ptr + Length(mCurKey));
    }
  }

  /**
   * If we seeked to the end of the block, fetch the next one
   */
  if (mBlock.ptr >= mBlock.end) {
    if (!FetchNextBlock())
      return false;
    mCurKey = (ByteString32T *)mBlock.ptr;
    mCurValue = (ByteString32T *)(mBlock.ptr + Length(mCurKey));
  }

  /**
   * End of range check
   */
  if (mCheckForRangeEnd) {
    if (!(*mCurKey < *mEndKey)) {
      mIter = mIndex.end();
      return false;
    }
  }


  return true;
}


