/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
#include <algorithm>
#include <cassert>

#include "Common/Logger.h"

#include "Hypertable/Lib/Key.h"

#include "CellCacheScanner.h"

using namespace Hypertable;

/**
 *
 */
CellCacheScanner::CellCacheScanner(CellCachePtr &cellcache, ScanContextPtr &scan_ctx) : CellListScanner(scan_ctx), m_cell_cache_ptr(cellcache), m_cell_cache_mutex(cellcache->m_mutex), m_cur_key(0), m_cur_value(0), m_eos(false) {
  ByteString bs;
  size_t start_row_len = scan_ctx->start_row.length() + 1;
  size_t end_row_len = scan_ctx->end_row.length() + 1;
  DynamicBuffer dbuf(7 + std::max(start_row_len, end_row_len));

  {
    boost::mutex::scoped_lock lock(m_cell_cache_mutex);
    Key key;

    assert(scan_ctx->start_row <= scan_ctx->end_row);

    /** set start iterator **/
    dbuf.clear();
    append_as_byte_string(dbuf, scan_ctx->start_row.c_str(), start_row_len);
    bs.ptr = dbuf.base;
    m_start_iter = m_cell_cache_ptr->m_cell_map.lower_bound(bs);

    /** set end iterator **/
    dbuf.clear();
    append_as_byte_string(dbuf, scan_ctx->end_row.c_str(), end_row_len);
    bs.ptr = dbuf.base;
    m_end_iter = m_cell_cache_ptr->m_cell_map.lower_bound(bs);

    m_cur_iter = m_start_iter;

    while (m_cur_iter != m_end_iter) {
      if (!key.load((*m_cur_iter).first)) {
        HT_ERROR("Problem parsing key!");
      }
      else if (key.flag == FLAG_DELETE_ROW || m_scan_context_ptr->family_mask[key.column_family_code]) {
        m_cur_key = (*m_cur_iter).first;
        m_cur_value.ptr = m_cur_key.ptr + (CellCache::OFFSET_BIT_MASK & (*m_cur_iter).second);
        return;
      }
      m_cur_iter++;
    }
    m_eos = true;
    return;
  }

}


bool CellCacheScanner::get(ByteString &key, ByteString &value) {
  if (!m_eos) {
    key = m_cur_key;
    value = m_cur_value;
    return true;
  }
  return false;
}



void CellCacheScanner::forward() {
  boost::mutex::scoped_lock lock(m_cell_cache_mutex);
  Key key;

  m_cur_iter++;
  while (m_cur_iter != m_end_iter) {
    if (!key.load((*m_cur_iter).first)) {
      HT_ERROR("Problem parsing key!");
    }
    else if (key.flag == FLAG_DELETE_ROW || m_scan_context_ptr->family_mask[key.column_family_code]) {
      m_cur_key = (*m_cur_iter).first;
      m_cur_value.ptr = m_cur_key.ptr + (CellCache::OFFSET_BIT_MASK & (*m_cur_iter).second);
      return;
    }
    m_cur_iter++;
  }
  m_eos = true;
}
