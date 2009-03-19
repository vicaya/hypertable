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

using namespace Hypertable;

/**
 *
 */
CellCacheScanner::CellCacheScanner(CellCachePtr &cellcache,
                                   ScanContextPtr &scan_ctx)
  : CellListScanner(scan_ctx), m_cell_cache_ptr(cellcache),
    m_cell_cache_mutex(cellcache->m_mutex), m_cur_value(0), m_eos(false),
    m_has_start_deletes(false), m_has_start_row_delete(false),
    m_has_start_cf_delete(false) {

  {
    ScopedLock lock(m_cell_cache_mutex);


   /**
    * Figure out what potential start ROW and CF delete keys look like.
    * We only need to worry about this if the scan starts in the middle of the row, ie
    * the scan ctx has defined cell intervals. Further we only need to worry
    * about CF deletes if this scan start in the middle of a column family
    * ie, the scan contains a qualified column.
    */
    if (scan_ctx->has_cell_interval) {
      CellCache::CellMap::iterator iter;
      DynamicBuffer current_buf;
      Key current;
      Key start_key;
      size_t start_delete_cf_offset=0;

      start_key.load(scan_ctx->start_key);

      // Allocate buffers with a little wiggle room so we don't have to re-alloc
      current_buf.ensure(start_key.serial.length()*2);
      m_start_delete_buf.ensure(start_key.serial.length()*4);

      create_key_and_append(current_buf, FLAG_DELETE_ROW,
          start_key.row, 0,
          "", start_key.timestamp,
          start_key.revision);

      current.serial.ptr = current_buf.base;

      // compare row first since if row doesnt exist dont
      // bother checking for column family
      iter = m_cell_cache_ptr->m_cell_map.lower_bound(current.serial);
      if (iter != m_cell_cache_ptr->m_cell_map.end())
        current.load(iter->first);

      if (iter != m_cell_cache_ptr->m_cell_map.end() && !strcmp(current.row,start_key.row)) {
        if (current.flag == FLAG_DELETE_ROW) {
          create_key_and_append(m_start_delete_buf,
              FLAG_DELETE_ROW, current.row, 0, current.column_qualifier,
              current.timestamp, current.revision);
          start_delete_cf_offset = m_start_delete_buf.fill();
          m_has_start_row_delete = true;
          m_has_start_deletes = true;
        }

        current_buf.clear();
        if (scan_ctx->has_start_cf_qualifier) {
          create_key_and_append(current_buf,
              FLAG_DELETE_COLUMN_FAMILY, start_key.row,
              start_key.column_family_code, "",
              start_key.timestamp, start_key.revision);

          current.serial.ptr = current_buf.base;
          iter = m_cell_cache_ptr->m_cell_map.lower_bound(current.serial);
          if (iter != m_cell_cache_ptr->m_cell_map.end())
            current.load(iter->first);
          if (iter != m_cell_cache_ptr->m_cell_map.end() &&
              !strcmp(current.row,start_key.row) &&
              current.column_family_code == start_key.column_family_code &&
              current.flag == FLAG_DELETE_COLUMN_FAMILY) {
            create_key_and_append(m_start_delete_buf,
                FLAG_DELETE_COLUMN_FAMILY, current.row,
                current.column_family_code, current.column_qualifier,
                current.timestamp, current.revision);

            m_has_start_cf_delete = true;
            m_has_start_deletes = true;
          }
        }
        // Load delete keys from dynamic buffer
        if (m_has_start_row_delete) {
          m_start_deletes[0].serial.ptr = m_start_delete_buf.base;
          m_start_deletes[0].load(m_start_deletes[0].serial);
        }

        if (m_has_start_cf_delete) {
          m_start_deletes[1].serial.ptr = m_start_delete_buf.base + start_delete_cf_offset;
          m_start_deletes[1].load(m_start_deletes[1].serial);
        }
      }
    }

    /**
     * Initialize first key that matches the scan
     */
    m_start_iter =
        m_cell_cache_ptr->m_cell_map.lower_bound(scan_ctx->start_key);
    m_end_iter = m_cell_cache_ptr->m_cell_map.lower_bound(scan_ctx->end_key);

    m_cur_iter = m_start_iter;

    while (m_cur_iter != m_end_iter) {
      m_cur_key.load( (*m_cur_iter).first );
      if (m_cur_key.flag == FLAG_DELETE_ROW
          || m_scan_context_ptr->family_mask[m_cur_key.column_family_code]) {
        m_cur_value.ptr = m_cur_key.serial.ptr + (*m_cur_iter).second;
        return;
      }
      ++m_cur_iter;
    }
    m_eos = true;
    return;
  }

}


bool CellCacheScanner::get(Key &key, ByteString &value) {
  if (!m_eos) {
    /**
     * Check for row/cf deletes for the start key
     */
    if (m_has_start_deletes) {
      if (m_has_start_row_delete) {
        key = m_start_deletes[0];
        value = 0;
      }
      else {
        key = m_start_deletes[1];
        value = 0;
      }
      return true;
    }
    key = m_cur_key;
    value = m_cur_value;
    return true;
  }
  return false;
}



void CellCacheScanner::forward() {
  ScopedLock lock(m_cell_cache_mutex);

  /**
   * Check for row/cf deletes for the start key
   */
  if (m_has_start_deletes) {
    if (m_has_start_row_delete) {
      m_has_start_row_delete = false;
    }
    else {
      m_has_start_cf_delete = false;
    }

    if (!m_has_start_row_delete && !m_has_start_cf_delete)
      m_has_start_deletes = false;
    return;
  }

  ++m_cur_iter;
  while (m_cur_iter != m_end_iter) {

    m_cur_key.load( (*m_cur_iter).first );
    if (m_cur_key.flag == FLAG_DELETE_ROW
        || m_scan_context_ptr->family_mask[m_cur_key.column_family_code]) {
      m_cur_value.ptr = m_cur_key.serial.ptr + (*m_cur_iter).second;
      return;
    }
    ++m_cur_iter;
  }
  m_eos = true;
}
