/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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
#include "Global.h"

using namespace Hypertable;

/**
 *
 */
CellCacheScanner::CellCacheScanner(CellCachePtr &cellcache,
                                   ScanContextPtr &scan_ctx)
  : CellListScanner(scan_ctx), m_cell_cache_ptr(cellcache),
    m_cell_cache_mutex(cellcache->m_mutex), m_entry_cache_next(0),
    m_in_deletes(false), m_eos(false), m_keys_only(false) {
  ScopedLock lock(m_cell_cache_mutex);
  DynamicBuffer current_buf;
  Key current;
  String tmp_str;

  m_keys_only = (scan_ctx->spec) ? scan_ctx->spec->keys_only : false;

  current_buf.grow(scan_ctx->start_key.row_len +
                   scan_ctx->start_key.column_qualifier_len +
                   scan_ctx->end_key.row_len +
                   scan_ctx->end_key.column_qualifier_len + 32);


  /**
   * Figure out what potential start ROW and CF delete keys look like.
   * We only need to worry about this if the scan starts in the middle of the row, ie
   * the scan ctx has defined cell intervals. Further we only need to worry
   * about CF deletes if this scan starts in the middle of a column family
   * ie, the scan contains a qualified column.
   */
  if (scan_ctx->has_cell_interval) {
    CellCache::CellMap::iterator iter;

    /**
     * Look for any DELETE_ROW records for this row and add them
     * to the m_deletes map
     */
    create_key_and_append(current_buf, FLAG_DELETE_ROW,
                          scan_ctx->start_key.row, 0,
                          "", TIMESTAMP_MAX, 0);

    current.serial.ptr = current_buf.base;

    for (iter = m_cell_cache_ptr->m_cell_map.lower_bound(current.serial);
         iter != m_cell_cache_ptr->m_cell_map.end(); ++iter) {
      current.load(iter->first);
      if (current.flag != FLAG_DELETE_ROW ||
          strcmp(current.row, scan_ctx->start_key.row))
        break;
      m_deletes.insert(CellCache::CellMap::value_type(iter->first, iter->second));
    }

    if (scan_ctx->has_start_cf_qualifier) {

      current_buf.clear();
      create_key_and_append(current_buf, FLAG_DELETE_COLUMN_FAMILY,
                            scan_ctx->start_key.row,
                            scan_ctx->start_key.column_family_code,
                            "", TIMESTAMP_MAX, 0);

      current.serial.ptr = current_buf.base;

      for (iter = m_cell_cache_ptr->m_cell_map.lower_bound(current.serial);
           iter != m_cell_cache_ptr->m_cell_map.end(); ++iter) {
        current.load(iter->first);
        if (current.flag != FLAG_DELETE_COLUMN_FAMILY ||
            current.column_family_code != scan_ctx->start_key.column_family_code ||
            strcmp(current.row, scan_ctx->start_key.row))
          break;
        m_deletes.insert(CellCache::CellMap::value_type(iter->first, iter->second));
      }
    }
  }

  m_start_iter = m_cell_cache_ptr->m_cell_map.lower_bound(scan_ctx->start_serkey);
  if (m_start_iter != m_cell_cache_ptr->m_cell_map.end())
    m_end_iter = m_cell_cache_ptr->m_cell_map.lower_bound(scan_ctx->end_serkey);
  else
    m_end_iter = m_cell_cache_ptr->m_cell_map.end();
  m_cur_iter = m_start_iter;

  if (!m_deletes.empty()) {
    m_in_deletes = true;
    m_delete_iter = m_deletes.begin();
  }

  while (m_cur_iter != m_end_iter) {
    m_cur_entry.key.load( (*m_cur_iter).first );
    if (m_cur_entry.key.flag == FLAG_DELETE_ROW
        || m_scan_context_ptr->family_mask[m_cur_entry.key.column_family_code]) {
      m_cur_entry.value.ptr = m_cur_entry.key.serial.ptr + (*m_cur_iter).second;
      return;
    }
    ++m_cur_iter;
  }
  m_eos = true;
  return;
}

bool CellCacheScanner::get(Key &key, ByteString &value) {

 try_again:

  if (m_entry_cache_next < m_entry_cache.size()) {
    memcpy(&key, &m_entry_cache[m_entry_cache_next].key, sizeof(key));
    memcpy(&value, &m_entry_cache[m_entry_cache_next].value, sizeof(value));
    return true;
  }

  if (m_eos)
    return false;

  load_entry_cache();
  goto try_again;

}

void CellCacheScanner::forward() {
  m_entry_cache_next++;
}


bool CellCacheScanner::internal_get() {

  if (m_in_deletes) {
    m_cur_entry.key.load( (*m_delete_iter).first );
    m_cur_entry.value.ptr = m_cur_entry.key.serial.ptr + (*m_delete_iter).second;
    return true;
  }

  if (!m_eos) {
    if (m_keys_only)
      m_cur_entry.value = (ByteString)0;
    return true;
  }

  return false;
}



void CellCacheScanner::internal_forward() {

  if (m_in_deletes) {
    ++m_delete_iter;
    if (m_delete_iter == m_deletes.end()) {
      m_in_deletes = false;
      // reset current entry since its loaded with the last entry in m_deletes
      m_cur_entry.key.load( (*m_cur_iter).first );
      m_cur_entry.value.ptr = m_cur_entry.key.serial.ptr + (*m_cur_iter).second;
    }
    return;
  }

  ++m_cur_iter;
  while (m_cur_iter != m_end_iter) {

    m_cur_entry.key.load( (*m_cur_iter).first );
    if (m_cur_entry.key.flag == FLAG_DELETE_ROW
        || m_scan_context_ptr->family_mask[m_cur_entry.key.column_family_code]) {
      m_cur_entry.value.ptr = m_cur_entry.key.serial.ptr + (*m_cur_iter).second;
      return;
    }
    ++m_cur_iter;
  }
  m_eos = true;
}


/*
 * std::vector<CellCacheEntry>    m_entry_cache;
 * size_t                         m_entry_cache_next;
 */
void CellCacheScanner::load_entry_cache() {
  ScopedLock lock(m_cell_cache_mutex);

  m_entry_cache_next = 0;
  m_entry_cache.clear();

  if (m_eos)
    return;

  while (m_entry_cache.size() < (size_t)Global::cell_cache_scanner_cache_size) {

    if (!internal_get()) {
      m_eos = true;
      break;
    }
    m_entry_cache.push_back(m_cur_entry);

    internal_forward();
  }


}
