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
