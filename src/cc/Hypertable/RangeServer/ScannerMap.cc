/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

using namespace Hypertable;

atomic_t ScannerMap::ms_next_id = ATOMIC_INIT(0);

/**
 *
 */
uint32_t ScannerMap::put(CellListScannerPtr &scannerPtr, RangePtr &rangePtr) {
  boost::mutex::scoped_lock lock(m_mutex);
  ScanInfoT scanInfo;
  scanInfo.scannerPtr = scannerPtr;
  scanInfo.rangePtr = rangePtr;
  scanInfo.last_access = get_timestamp();
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
  (*iter).second.last_access = get_timestamp();
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

void ScannerMap::purge_expired(time_t expire_time) {
  boost::mutex::scoped_lock lock(m_mutex);
  time_t now = get_timestamp();
  CellListScannerMapT::iterator iter = m_scanner_map.begin();

  while (iter != m_scanner_map.end()) {
    if ((now - (*iter).second.last_access) > expire_time) {
      CellListScannerMapT::iterator tmp_iter = iter;
      HT_WARNF("Destroying scanner %d because it has not been used in %u seconds", (*iter).first, (uint32_t)expire_time);
      iter++;
      (*tmp_iter).second.scannerPtr = 0;
      (*tmp_iter).second.rangePtr = 0;
      m_scanner_map.erase(tmp_iter);
    }
    else
      iter++;
  }

}


time_t ScannerMap::get_timestamp() {
  boost::xtime now;
  boost::xtime_get(&now, boost::TIME_UTC);
  return (time_t)now.sec;
}
