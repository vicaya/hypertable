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

#include "Common/Logger.h"

#include "Key.h"
#include "CellCacheScanner.h"


/**
 * 
 */
CellCacheScanner::CellCacheScanner(CellCachePtr &cellCachePtr) : CellListScanner(), mCellCachePtr(cellCachePtr), mCellCacheMutex(cellCachePtr->mMutex), mCurKey(0), mCurValue(0), mEos(false) {
}


/**
 *
 */
void CellCacheScanner::Reset() {
  boost::mutex::scoped_lock lock(mCellCacheMutex);

  if (mRangeStart == 0)
    mStartIter = mCellCachePtr->mCellMap.begin();
  else
    mStartIter = mCellCachePtr->mCellMap.lower_bound(mRangeStart);
  if (mRangeEnd == 0)
    mEndIter = mCellCachePtr->mCellMap.end();
  else
    mEndIter = mCellCachePtr->mCellMap.lower_bound(mRangeEnd);
  mCurIter = mStartIter;
  if (mCurIter != mEndIter) {
    mCurKey = (*mCurIter).first;
    mCurValue = (*mCurIter).second;
    mEos = false;
  }
  else
    mEos = true;
  mReset = true;
}


bool CellCacheScanner::Get(ByteString32T **keyp, ByteString32T **valuep) {
  assert(mReset);
  if (!mEos) {
    *keyp = (ByteString32T *)mCurKey;
    *valuep = (ByteString32T *)mCurValue;
    return true;
  }
  return false;
}

void CellCacheScanner::Forward() {
  boost::mutex::scoped_lock lock(mCellCacheMutex);
  KeyComponentsT keyComps;

  mCurIter++;
  while (mCurIter != mEndIter) {
    if (!Load((*mCurIter).first, keyComps)) {
      LOG_ERROR("Problem parsing key!");
    }
    else if (mFamilyMask[keyComps.columnFamily]) {
      mCurKey = (*mCurIter).first;
      mCurValue = (*mCurIter).second;
      return;
    }
    mCurIter++;
  }
  mEos = true;
}
