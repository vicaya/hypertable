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

