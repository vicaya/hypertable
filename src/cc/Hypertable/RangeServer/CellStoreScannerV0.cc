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

#include "BlockInflaterZlib.h"
#include "Global.h"
#include "CellStoreScannerV0.h"

using namespace hypertable;

CellStoreScannerV0::CellStoreScannerV0(CellStorePtr &cellStorePtr, ScanContextPtr &scanContextPtr) : CellListScanner(scanContextPtr), mCellStorePtr(cellStorePtr), mCurKey(0), mCurValue(0), mCheckForRangeEnd(false), mEndKeyPtr() {
  ByteString32T  *tmpKey, *startKey, *endKey;

  mCellStoreV0 = dynamic_cast< CellStoreV0*>(mCellStorePtr.get());
  assert(mCellStoreV0);

  mIndex = mCellStoreV0->mIndex;
  mFileId = mCellStoreV0->mFileId;
  mBlockInflater = new BlockInflaterZlib();
  memset(&mBlock, 0, sizeof(mBlock));

  // compute startKey
  tmpKey = mCellStoreV0->mStartKeyPtr.get();
  if (scanContextPtr->spec && scanContextPtr->spec->startRow->len != 0)
    startKey = (tmpKey != 0 && *scanContextPtr->spec->startRow < *tmpKey) ? tmpKey : scanContextPtr->spec->startRow;
  else
    startKey = tmpKey;

  // compute mEndKeyPtr
  if (scanContextPtr->endKeyPtr.get() != 0) {
    if (mCellStoreV0->mEndKeyPtr.get() == 0 || *scanContextPtr->endKeyPtr.get() < *mCellStoreV0->mEndKeyPtr.get())
      mEndKeyPtr = scanContextPtr->endKeyPtr;
    else
      mEndKeyPtr = mCellStoreV0->mEndKeyPtr;
  }
  else
    mEndKeyPtr = mCellStoreV0->mEndKeyPtr;

  if (mBlock.base != 0)
    Global::blockCache->Checkin(mFileId, mBlock.offset);
  memset(&mBlock, 0, sizeof(mBlock));

  mCheckForRangeEnd = false;

  if (startKey != 0)
    mIter = mIndex.lower_bound(startKey);
  else
    mIter = mIndex.begin();

  mCurKey = 0;

  if (!FetchNextBlock()) {
    mIter = mIndex.end();
    return;
  }

  /**
   * Seek to start of range in block
   */
  mCurKey = (ByteString32T *)mBlock.ptr;
  mCurValue = (ByteString32T *)(mBlock.ptr + Length(mCurKey));
  if (startKey != 0) {
    while (*mCurKey < *startKey) {
      mBlock.ptr = ((uint8_t *)mCurValue) + Length(mCurValue);
      if (mBlock.ptr >= mBlock.end) {
	mIter = mIndex.end();
	return;
      }
      mCurKey = (ByteString32T *)mBlock.ptr;
      mCurValue = (ByteString32T *)(mBlock.ptr + Length(mCurKey));
    }
  }

  /**
   * End of range check
   */
  if (mCheckForRangeEnd) {
    if (!(*mCurKey < *mEndKeyPtr.get())) {
      mIter = mIndex.end();
      return;
    }
  }

}


CellStoreScannerV0::~CellStoreScannerV0() {
  if (mBlock.base != 0)
    Global::blockCache->Checkin(mFileId, mBlock.offset);
  delete mBlockInflater;
}


bool CellStoreScannerV0::Get(ByteString32T **keyp, ByteString32T **valuep) {

  if (mIter == mIndex.end())
    return false;

  *keyp = mCurKey;
  *valuep = mCurValue;

  return true;
}



void CellStoreScannerV0::Forward() {
  Key keyComps;

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
      if (!(*mCurKey < *mEndKeyPtr.get())) {
	mIter = mIndex.end();
	break;
      }
    }

    /**
     * Column family check
     */
    if (!keyComps.load(mCurKey)) {
      LOG_ERROR("Problem parsing key!");
      break;
    }
    if (mScanContextPtr->familyMask[keyComps.columnFamily])
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
      if (mEndKeyPtr.get() != 0)
	mCheckForRangeEnd = true;
    }
    else {
      if (mEndKeyPtr.get() != 0 && !(*((*iterNext).first) < *mEndKeyPtr.get()))
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
      if (mCellStoreV0->mFilesys->Pread(mCellStoreV0->mFd, mBlock.offset, mBlock.zlength, buf, &nread) != Error::OK)
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
