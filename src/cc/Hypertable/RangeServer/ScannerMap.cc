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

atomic_t ScannerMap::ms_next_id = ATOMIC_INIT(0);

/**
 *
 */
uint32_t ScannerMap::put(CellListScannerPtr &scannerPtr, RangePtr &rangePtr) {
  boost::mutex::scoped_lock lock(m_mutex);
  ScanInfoT scanInfo;
  scanInfo.scannerPtr = scannerPtr;
  scanInfo.rangePtr = rangePtr;
  uint32_t id = atomic_inc_return(&ms_next_id);
  m_scanner_map[id] = scanInfo;
  return id;
}



/**
 *
 */
bool ScannerMap::get(uint32_t id, CellListScannerPtr &scannerPtr, RangePtr &rangePtr) {
  boost::mutex::scoped_lock lock(m_mutex);
  CellListScannerMapT::iterator iter = m_scanner_map.find(id);
  if (iter == m_scanner_map.end())
    return false;
  scannerPtr = (*iter).second.scannerPtr;
  rangePtr = (*iter).second.rangePtr;
  return true;
}



/**
 * 
 */
bool ScannerMap::remove(uint32_t id) {
  boost::mutex::scoped_lock lock(m_mutex);
  return (m_scanner_map.erase(id) == 0) ? false : true;
}
