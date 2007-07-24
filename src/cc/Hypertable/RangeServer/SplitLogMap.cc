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
#include <string>

#include "SplitLogMap.h"
#include "RangeInfo.h"


/**
 *
 */
void SplitLogMap::Insert(SplitLogInfoT *splitLogInfo) {
  std::string startRow;
  SplitLogInfoPtr splitLogInfoPtr(splitLogInfo);

  splitLogInfo->rangeInfo->GetStartRow(startRow);
  mSplitLogInfoMap[startRow] = splitLogInfoPtr;
}


/**
 * 
 */
bool SplitLogMap::Lookup(string &row, SplitLogInfoPtr &splitLogInfoPtr) {
  boost::read_write_mutex::scoped_read_lock lock(mRwMutex);

  if (mSplitLogInfoMap.empty())
    return false;

  {
    SplitLogInfoMapT::iterator iter = mSplitLogInfoMap.upper_bound(row);
    std::string endRow;

    if (iter == mSplitLogInfoMap.begin())
      return false;

    iter--;

    (*iter).second->rangeInfo->GetEndRow(endRow);
    if (row < endRow) {
      splitLogInfoPtr = (*iter).second;
      return true;
    }
  }

  return false;
}
