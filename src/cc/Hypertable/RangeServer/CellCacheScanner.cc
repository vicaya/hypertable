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

#include <cassert>

#include "Common/Logger.h"

#include "Hypertable/Lib/Key.h"

#include "CellCacheScanner.h"


/**
 * 
 */
CellCacheScanner::CellCacheScanner(CellCachePtr &cellCachePtr, ScanContextPtr &scanContextPtr) : CellListScanner(scanContextPtr), m_cell_cache_ptr(cellCachePtr), m_cell_cache_mutex(cellCachePtr->m_mutex), m_cur_key(0), m_cur_value(0), m_eos(false) {

  /** set start iterator **/
  if (!scanContextPtr->startKeyPtr || (scanContextPtr->startKeyPtr)->len == 0)
    m_start_iter = m_cell_cache_ptr->m_cell_map.begin();
  else
    m_start_iter = m_cell_cache_ptr->m_cell_map.lower_bound(scanContextPtr->startKeyPtr.get());

  /** set end iterator **/
  if (!scanContextPtr->endKeyPtr || scanContextPtr->endKeyPtr->len == 0)
    m_end_iter = m_cell_cache_ptr->m_cell_map.end();
  else
    m_end_iter = m_cell_cache_ptr->m_cell_map.lower_bound(scanContextPtr->endKeyPtr.get());

  m_cur_iter = m_start_iter;

  if (m_cur_iter != m_end_iter) {
    m_cur_key = (*m_cur_iter).first;
    m_cur_value = (*m_cur_iter).second;
    m_eos = false;
  }
  else
    m_eos = true;
}


bool CellCacheScanner::get(ByteString32T **keyp, ByteString32T **valuep) {
  if (!m_eos) {
    *keyp = (ByteString32T *)m_cur_key;
    *valuep = (ByteString32T *)m_cur_value;
    return true;
  }
  return false;
}

void CellCacheScanner::forward() {
  boost::mutex::scoped_lock lock(m_cell_cache_mutex);
  Key keyComps;

  m_cur_iter++;
  while (m_cur_iter != m_end_iter) {
    if (!keyComps.load((*m_cur_iter).first)) {
      LOG_ERROR("Problem parsing key!");
    }
    else if (m_scan_context_ptr->familyMask[keyComps.columnFamily]) {
      m_cur_key = (*m_cur_iter).first;
      m_cur_value = (*m_cur_iter).second;
      return;
    }
    m_cur_iter++;
  }
  m_eos = true;
}
