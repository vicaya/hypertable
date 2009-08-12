/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
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
#include <cassert>

#include "Common/Error.h"
#include "Common/System.h"

#include "Hypertable/Lib/BlockCompressionHeader.h"
#include "Global.h"
#include "CellStoreBlockIndexMap.h"
#include "CellStoreScanner.h"

#include "CellStoreScannerInterval.h"
#include "CellStoreScannerIntervalBlockIndex.h"
#include "CellStoreScannerIntervalReadahead.h"

using namespace Hypertable;


template <typename IndexT>
CellStoreScanner<IndexT>::CellStoreScanner(CellStore *cellstore, ScanContextPtr &scan_ctx, IndexT *index) :
  CellListScanner(scan_ctx), m_cellstore(cellstore), m_interval_index(0),
  m_interval_max(0), m_keys_only(false), m_eos(false) {
  SerializedKey start_key, end_key;

  m_keys_only = (scan_ctx->spec) ? scan_ctx->spec->keys_only : false;

  memset(m_interval_scanners, 0, 3*sizeof(CellStoreScannerInterval *));

  if (scan_ctx->has_cell_interval) {

    // maybe assert that there isn't more restrictive cellstore start/end row ?

    m_key_buf.grow(4*(scan_ctx->start_key.row_len +
		      scan_ctx->start_key.column_qualifier_len + 32));

    /**
     * Fetch ROW deletes
     */
    start_key.ptr = m_key_buf.ptr;
    create_key_and_append(m_key_buf, FLAG_DELETE_ROW, scan_ctx->start_key.row,
			  0, "", TIMESTAMP_MAX, scan_ctx->revision);

    end_key.ptr = m_key_buf.ptr;
    create_key_and_append(m_key_buf, FLAG_DELETE_COLUMN_FAMILY,
			  scan_ctx->start_key.row, 0, "", TIMESTAMP_MAX,
			  scan_ctx->revision);
    
    m_interval_scanners[m_interval_max++] =
      new CellStoreScannerIntervalBlockIndex<IndexT>(cellstore, index, start_key, end_key);
    
    /**
     * Fetch COLUMN FAMILY deletes
     */
    start_key.ptr = m_key_buf.ptr;
    create_key_and_append(m_key_buf, FLAG_DELETE_COLUMN_FAMILY,
			  scan_ctx->start_key.row,
			  scan_ctx->start_key.column_family_code,
			  "", TIMESTAMP_MAX, scan_ctx->revision);

    end_key.ptr = m_key_buf.ptr;
    create_key_and_append(m_key_buf, FLAG_DELETE_CELL,
			  scan_ctx->start_key.row,
			  scan_ctx->start_key.column_family_code,
			  "", TIMESTAMP_MAX, scan_ctx->revision);

    m_interval_scanners[m_interval_max++] =
      new CellStoreScannerIntervalBlockIndex<IndexT>(cellstore, index, start_key, end_key);

    if (strcmp(scan_ctx->end_key.row, cellstore->get_end_row()) > 0)
      end_key.ptr = 0;
    else
      end_key.ptr = scan_ctx->end_serkey.ptr;

    if (scan_ctx->single_row)
      m_interval_scanners[m_interval_max++] = new CellStoreScannerIntervalBlockIndex<IndexT>(cellstore, index, scan_ctx->start_serkey, end_key);
    else
      m_interval_scanners[m_interval_max++] = new CellStoreScannerIntervalReadahead<IndexT>(cellstore, index, scan_ctx->start_serkey, scan_ctx->end_serkey);
  }
  else {
    String tmp_str;
    bool readahead = false;
    size_t buf_needed = 128 + strlen(cellstore->get_start_row()) + strlen(cellstore->get_end_row());

    m_key_buf.grow(buf_needed);

    if (strcmp(scan_ctx->start_key.row, cellstore->get_start_row()) < 0) {
      tmp_str = cellstore->get_start_row();
      if (tmp_str.length() > 0)
        tmp_str.append(1, 1);
      create_key_and_append(m_key_buf, 0, tmp_str.c_str(), 0, "", TIMESTAMP_MAX, 0);
      start_key.ptr = m_key_buf.base;
      readahead = true;
    }
    else {
      start_key.ptr = scan_ctx->start_serkey.ptr;
      readahead = scan_ctx->start_key.row_len == 0;
    }

    if (strcmp(scan_ctx->end_key.row, cellstore->get_end_row()) > 0) {
      end_key.ptr = m_key_buf.ptr;
      tmp_str = cellstore->get_end_row();
      tmp_str.append(1, 1);
      create_key_and_append(m_key_buf, 0, tmp_str.c_str(), 0, "", TIMESTAMP_MAX, 0);
      readahead = true;
    }
    else {
      end_key.ptr = scan_ctx->end_serkey.ptr;
      readahead = !strcmp(scan_ctx->end_key.row, Key::END_ROW_MARKER);
    }

    if (readahead)
      m_interval_scanners[m_interval_max++] = new CellStoreScannerIntervalReadahead<IndexT>(cellstore, index, start_key, end_key);
    else {
      HT_ASSERT(index);
      m_interval_scanners[m_interval_max++] = new CellStoreScannerIntervalBlockIndex<IndexT>(cellstore, index, start_key, end_key);
    }

  }
    
}



template <typename IndexT>
CellStoreScanner<IndexT>::~CellStoreScanner() {
  for (size_t i=0; i<m_interval_max; i++)
    delete m_interval_scanners[i];
}



template <typename IndexT>
bool CellStoreScanner<IndexT>::get(Key &key, ByteString &value) {

  if (m_eos)
    return false;

  if (m_interval_scanners[m_interval_index]->get(key, value)) {
    if (m_keys_only)
      value = 0;
    return true;
  }

  m_interval_index++;

  while (m_interval_index < m_interval_max) {
    if (m_interval_scanners[m_interval_index]->get(key, value)) {
      if (m_keys_only)
	value = 0;
      return true;
    }
    m_interval_index++;
  }

  m_eos = true;
  return false;
}



template <typename IndexT>
void CellStoreScanner<IndexT>::forward() {
  if (m_eos)
    return;
  m_interval_scanners[m_interval_index]->forward();
}

template class CellStoreScanner<CellStoreBlockIndexMap<uint32_t> >;
template class CellStoreScanner<CellStoreBlockIndexMap<int64_t> >;
