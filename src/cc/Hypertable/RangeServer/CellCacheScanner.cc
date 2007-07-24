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
CellCacheScanner::CellCacheScanner(CellCache *memtable) : CellListScanner(), mTable(memtable) {
}


/**
 *
 */
void CellCacheScanner::Reset() {
  if (mRangeStart == 0)
    mStartIter = mTable->mCellMap->begin();
  else
    mStartIter = mTable->mCellMap->lower_bound(mRangeStartPtr);
  if (mRangeEnd == 0)
    mEndIter = mTable->mCellMap->end();
  else
    mEndIter = mTable->mCellMap->lower_bound(mRangeEndPtr);
  mCurIter = mStartIter;
  mReset = true;
}


bool CellCacheScanner::Get(KeyT **keyp, ByteString32T **valuep) {
  assert(mReset);
  if (mCurIter != mEndIter) {
    *keyp = (KeyT *)(*mCurIter).first.get();
    *valuep = (ByteString32T *)(*mCurIter).second.get();
    return true;
  }
  return false;
}

void CellCacheScanner::Forward() {
  KeyT *key;
  KeyComponentsT keyComps;

  mCurIter++;
  while (mCurIter != mEndIter) {
    key = (KeyT *)(*mCurIter).first.get();
    if (!Load(key, keyComps)) {
      LOG_ERROR("Problem parsing key!");
      break;
    }
    if (mFamilyMask[keyComps.columnFamily])
      break;
    mCurIter++;
  }
}
