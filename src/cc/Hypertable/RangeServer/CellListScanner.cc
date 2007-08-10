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
void CellListScanner::RestrictRange(ByteString32T *start, ByteString32T *end) {
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
