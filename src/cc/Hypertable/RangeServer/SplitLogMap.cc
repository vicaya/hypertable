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
