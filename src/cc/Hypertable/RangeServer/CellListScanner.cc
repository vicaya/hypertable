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

#include "CellListScanner.h"

using namespace hypertable;

/**
 */
CellListScanner::CellListScanner() : mReset(false), mRangeStart(0), mRangeEnd(0), mRangeStartPtr(), mRangeEndPtr() {
  bool *mask = new bool [ 256 ];
  memset(mask, 1, 256*sizeof(bool));
  mFamilyMask.reset(mask);
}


/**
 */
void CellListScanner::RestrictRange(KeyT *start, KeyT *end) {
  if (start != 0) {
    mRangeStart = CreateCopy(start);
    mRangeStartPtr.reset(mRangeStart);
  }
  if (end != 0) {
    mRangeEnd = CreateCopy(end);
    mRangeEndPtr.reset(mRangeEnd);
  }
  mReset = false;
}

void CellListScanner::RestrictColumns(const uint8_t *families, size_t count) {
  bool *mask = mFamilyMask.get();
  memset(mask, 0, 256*sizeof(bool));
  for (size_t i=0; i<count; i++)
    mask[ families[i] ] = true;
}
