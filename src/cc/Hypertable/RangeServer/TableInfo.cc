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


#include "TableInfo.h"

using namespace hypertable;


/**
 * 
 */
bool TableInfo::GetRange(string &startRow, RangePtr &rangePtr) {
  boost::mutex::scoped_lock lock(mMutex);

  RangeMapT::iterator iter = mRangeMap.find(startRow);

  if (iter == mRangeMap.end())
    return false;

  rangePtr = (*iter).second;

  return true;
}



/**
 * 
 */
void TableInfo::AddRange(RangeInfoPtr &rangeInfoPtr) {
  boost::mutex::scoped_lock lock(mMutex);
  std::string rangeStartRow;

  rangeInfoPtr->GetStartRow(rangeStartRow);

  RangeMapT::iterator iter = mRangeMap.find(rangeStartRow);
  assert(iter == mRangeMap.end());

  RangePtr rangePtr( new Range(mSchema, rangeInfoPtr) );

  mRangeMap[rangeStartRow] = rangePtr;
}

