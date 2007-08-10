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

#include "ScannerMap.h"

using namespace hypertable;

atomic_t ScannerMap::msNextId = ATOMIC_INIT(0);

/**
 *
 */
uint32_t ScannerMap::Put(CellListScannerPtr &scannerPtr, RangePtr &rangePtr) {
  boost::mutex::scoped_lock lock(mMutex);
  ScanInfoT scanInfo;
  scanInfo.scannerPtr = scannerPtr;
  scanInfo.rangePtr = rangePtr;
  uint32_t id = atomic_inc_return(&msNextId);
  mScannerMap[id] = scanInfo;
  return id;
}



/**
 *
 */
bool ScannerMap::Get(uint32_t id, CellListScannerPtr &scannerPtr, RangePtr &rangePtr) {
  boost::mutex::scoped_lock lock(mMutex);
  CellListScannerMapT::iterator iter = mScannerMap.find(id);
  if (iter == mScannerMap.end())
    return false;
  scannerPtr = (*iter).second.scannerPtr;
  rangePtr = (*iter).second.rangePtr;
  return true;
}



/**
 * 
 */
bool ScannerMap::Remove(uint32_t id) {
  boost::mutex::scoped_lock lock(mMutex);
  return (mScannerMap.erase(id) == 0) ? false : true;
}
