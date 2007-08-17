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

#include "Common/Logger.h"

#include "TableInfo.h"

using namespace hypertable;


/**
 * 
 */
bool TableInfo::GetRange(RangeSpecificationT *rangeSpec, RangePtr &rangePtr) {
  boost::mutex::scoped_lock lock(mMutex);
  string startRow = rangeSpec->startRow;
  string endRow = rangeSpec->endRow;

  RangeMapT::iterator iter = mRangeMap.find(endRow);

  if (iter == mRangeMap.end())
    return false;

  rangePtr = (*iter).second;

  if (rangePtr->StartRow() != startRow)
    return false;

  return true;
}



/**
 * 
 */
void TableInfo::AddRange(RangeInfoPtr &rangeInfoPtr) {
  boost::mutex::scoped_lock lock(mMutex);
  std::string rangeEndRow;

  rangeInfoPtr->GetEndRow(rangeEndRow);

  RangeMapT::iterator iter = mRangeMap.find(rangeEndRow);
  assert(iter == mRangeMap.end());

  RangePtr rangePtr( new Range(mSchema, rangeInfoPtr) );

  mRangeMap[rangeEndRow] = rangePtr;
}

