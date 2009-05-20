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
#include <cassert>

#include "Common/Error.h"
#include "Common/System.h"

#include "Hypertable/Lib/BlockCompressionHeader.h"
#include "Global.h"
#include "CellStoreScanner.h"
#include "CellStoreBlockIndexMap.h"

using namespace Hypertable;

namespace {
  const uint32_t MINIMUM_READAHEAD_AMOUNT = 65536;
}


template <typename IndexT>
CellStoreScanner<IndexT>::CellStoreScanner(CellStore *cellstore, IndexT &index,
                                           ScanContextPtr &scan_ctx) :
    CellListScanner(scan_ctx), m_cellstore(cellstore),
    m_index(index), m_check_for_range_end(false),
    m_readahead(true), m_close_fd_on_exit(false), m_fd(-1),
    m_start_offset(0), m_end_offset(0), m_returned(0), m_has_start_deletes(false),
    m_has_start_row_delete(false), m_has_start_cf_delete(false) {
  int start_key_offset = -1, end_key_offset = -1;
  IndexIteratorT start_iter;
  bool start_block_loaded = false;

  for (int ii=0; ii < 3; ++ii) {
    m_start_delete_buf_offsets[ii] = 0;
  }

  assert(m_cellstore);
  m_file_id = m_cellstore->get_file_id();
  m_zcodec = m_cellstore->create_block_compression_codec();
  memset(&m_block, 0, sizeof(m_block));


  // compute start key (and row)
  m_start_row = m_cellstore->get_start_row();
  if (m_start_row.compare(scan_ctx->start_row) < 0) {
    m_start_row = scan_ctx->start_row;
    m_start_key.ptr = scan_ctx->start_key.ptr;
  }
  else {
    start_key_offset = m_key_buf.fill();
    if (m_start_row != "")
      m_start_row.append(1,1);  // bump to next row
    create_key_and_append(m_key_buf, m_start_row.c_str());
  }

  // compute end row
  m_end_row = m_cellstore->get_end_row();
  if (scan_ctx->end_row.compare(m_end_row) < 0) {
    m_end_row = scan_ctx->end_row;
    m_end_key.ptr = scan_ctx->end_key.ptr;
  }
  else {
    end_key_offset = m_key_buf.fill();
    if (m_end_row != Key::END_ROW_MARKER)
      m_end_row.append(1,1);  // bump to next row
    create_key_and_append(m_key_buf, m_end_row.c_str());
  }
  if (start_key_offset != -1)
    m_start_key.ptr = m_key_buf.base + start_key_offset;

  if (end_key_offset != -1)
    m_end_key.ptr = m_key_buf.base + end_key_offset;

  m_cur_key.ptr = 0;


  /**
   * Figure out what potential start ROW and CF delete keys look like.
   * We only need to worry about this if the scan starts in the middle of the row, ie
   * the scan ctx has defined cell intervals. Further we only need to worry
   * about CF deletes if this scan start in the middle of a column family
   * ie, the scan contains a qualified column
   */
  if (scan_ctx->has_cell_interval)
    set_search_delete_keys(scan_ctx->has_start_cf_qualifier);

  /**
   * If we're just scanning a single row, turn off readahead
   */
  if (scan_ctx->single_row == true) {
    m_readahead = false;

    if (scan_ctx->has_cell_interval)
      set_start_deletes(scan_ctx->has_start_cf_qualifier);

    // Done with checking for deletes before start key
    // now move to start key
    start_iter = m_index.lower_bound(m_start_key);
    if (start_iter == m_iter)
      start_block_loaded = true;
    m_iter = start_iter;
    if (m_iter == m_index.end())
      return;
    if (!start_block_loaded) {
      memset(&m_block, 0, sizeof(m_block));
      m_fd = m_cellstore->get_fd();
      if (!fetch_next_block()) {
        m_iter = m_index.end();
        return;
      }
    }
  }
  else {
    if (scan_ctx->has_cell_interval) {
      // Look for preceeding ROW + CF deletes
      set_start_deletes_readahead(scan_ctx->has_start_cf_qualifier);
      // Done with checking for deletes before start key now move to start key
      start_iter = m_index.lower_bound(m_start_key);
      if (start_iter == m_iter)
        start_block_loaded = true;
      m_iter = start_iter;
      if (m_iter == m_index.end())
        return;
    }
    else {
      // No need to look for ROW/CF deletes
      m_iter = m_index.lower_bound(m_start_key);
      if (m_iter == m_index.end())
        return;
      start_buffered_read();
      start_block_loaded = true;
    }

    if (!start_block_loaded) {
      // move to block which has start key
      memset(&m_block, 0, sizeof(m_block));
      if (!fetch_next_block_readahead()) {
        m_iter = m_index.end();
        return;
      }
    }
  }

  /**
   * Seek to start of range in block
   */
  m_cur_key.ptr = m_block.ptr;
  m_cur_value.ptr = m_block.ptr + m_cur_key.length();

  while (m_cur_key < m_start_key) {
    m_block.ptr = m_cur_value.ptr + m_cur_value.length();
    if (m_block.ptr >= m_block.end) {
      if (m_readahead) {
        if (!fetch_next_block_readahead()) {
          HT_ERRORF("Unable to find start of range (row='%s') in %s",
              m_start_row.c_str(), m_cellstore->get_filename().c_str());
          return;
        }
      }
      else if (!fetch_next_block()) {
        HT_ERRORF("Unable to find start of range (row='%s') in %s",
            m_start_row.c_str(), m_cellstore->get_filename().c_str());
        return;
      }
    }
    m_cur_key.ptr = m_block.ptr;
    m_cur_value.ptr = m_block.ptr + m_cur_key.length();
  }

  /**
   * End of range check
   */
  if (m_cur_key >= m_end_key) {
    m_iter = m_index.end();
    return;
  }


  /**
   * Column family check
   */
  if (!m_key.load(m_cur_key)) {
    HT_ERROR("Problem parsing key!");
  }
  else if (m_key.flag != FLAG_DELETE_ROW &&
           !m_scan_context_ptr->family_mask[m_key.column_family_code])
    forward();

}


template <typename IndexT>
CellStoreScanner<IndexT>::~CellStoreScanner() {
  try {
    if (m_fd != -1 && m_close_fd_on_exit) {
      try { Global::dfs->close(m_fd, 0); }
      catch (Exception &e) {
        HT_THROW2F(e.code(), e, "Problem closing cellstore: %s",
                   m_cellstore->get_filename().c_str());
      }
    }

    if (m_readahead)
      delete [] m_block.base;
    else {
      if (m_block.base != 0)
        Global::block_cache->checkin(m_file_id, m_block.offset);
    }
    delete m_zcodec;

#ifdef STAT
    cout << flush;
    cout << "STAT[~CellStoreScanner]\tget\t" << m_returned << "\t";
    cout << m_cellstore->get_filename() << "[" << m_start_row << ".."
         << m_end_row << "]" << endl;
#endif
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
  }
  catch (...) {
    HT_ERRORF("Unknown exception caught in %s", HT_FUNC);
  }
}

/**
 * Set keys to scan for ROW/CF deletes at start of scan
 */
template <typename IndexT>
void CellStoreScanner<IndexT>::set_search_delete_keys(bool search_cf_delete)
{
  Key start_key;
  start_key.load(m_start_key);

  m_start_delete_buf.ensure(start_key.serial.length()*6);

  // construct keys to search for start ROW / CF deletes
  create_key_and_append(m_start_delete_buf, FLAG_DELETE_ROW,
      start_key.row, 0,
      "", start_key.timestamp,
      start_key.revision);
  m_start_delete_buf_offsets[0] = m_start_delete_buf.fill();
  if (search_cf_delete) {
    create_key_and_append(m_start_delete_buf, FLAG_DELETE_COLUMN_FAMILY,
        start_key.row, start_key.column_family_code,
        "", start_key.timestamp,
        start_key.revision);
  }
  m_start_delete_buf_offsets[1] = m_start_delete_buf.fill();
}

template <typename IndexT>
void CellStoreScanner<IndexT>::set_start_deletes(bool search_cf_delete) {
  IndexIteratorT row_delete_iter;
  IndexIteratorT cf_delete_iter;
  bool row_match = false;
  bool same_block = false;

  /**
   * Check to see if DELETE_ROW exists for scan start key
   */
  m_delete_search_keys[0].serial.ptr = m_start_delete_buf.base;
  m_delete_search_keys[0].load(m_delete_search_keys[0].serial);

  row_delete_iter = m_iter = m_index.lower_bound(m_delete_search_keys[0].serial);
  if (m_iter == m_index.end())
    return;

  m_has_start_row_delete = search_start_delete_keys(m_delete_search_keys[0], false, row_match);
  if (m_has_start_row_delete) {
    m_has_start_deletes = true;
  }

  m_start_delete_buf_offsets[2]= m_start_delete_buf.fill();

  /**
   * Check to see if DELETE_COLUMN_FAMILY exists for scan start key
   */
  if (search_cf_delete) {
    m_delete_search_keys[1].serial.ptr = m_start_delete_buf.base +
                                         m_start_delete_buf_offsets[0];
    m_delete_search_keys[1].load(m_delete_search_keys[1].serial);

    cf_delete_iter = m_iter = m_index.lower_bound(m_delete_search_keys[1].serial);
    if (m_iter == m_index.end())
      return;

    // if row delete and cf delete shd be in same block then theres no need to reload the block
    if (cf_delete_iter == row_delete_iter)
      same_block = true;

    // don't bother checking for col family delete if
    // it shd be in the same block as the row delete and the row was not found
    if (!same_block || row_match) {
      m_has_start_cf_delete = search_start_delete_keys(m_delete_search_keys[1],
                                                      same_block, row_match);
      if (m_has_start_cf_delete) {
        m_has_start_deletes = true;
      }
    }
  }
  // Load delete keys from buffer
  if (m_has_start_row_delete) {
    m_start_deletes[0].serial.ptr = m_start_delete_buf.base + m_start_delete_buf_offsets[1];
    m_start_deletes[0].load(m_start_deletes[0].serial);
  }

  if (m_has_start_cf_delete) {
    m_start_deletes[1].serial.ptr = m_start_delete_buf.base + m_start_delete_buf_offsets[2];
    m_start_deletes[1].load(m_start_deletes[1].serial);
  }

  return;
}

template <typename IndexT>
bool CellStoreScanner<IndexT>::search_start_delete_keys(Key &search_key,
                                                bool block_loaded, bool &row_match) {
  SerializedKey cur_key;
  ByteString cur_value;
  Key key;

  row_match = false;

  if (!block_loaded) {
    memset(&m_block, 0, sizeof(m_block));
    m_fd = m_cellstore->get_fd();
    if (!fetch_next_block())
      return false;
  }

  /**
   *  move to start of search key
   */
  cur_key.ptr = m_block.ptr;
  cur_value.ptr = m_block.ptr + cur_key.length();

  while (cur_key < search_key.serial) {
    m_block.ptr = cur_value.ptr + cur_value.length();
    if (m_block.ptr >= m_block.end) {
      if (!fetch_next_block()) {
        return false;
      }
    }
    cur_key.ptr = m_block.ptr;
    cur_value.ptr = m_block.ptr + cur_key.length();
  }

  if (!key.load(cur_key)) {
    HT_ERROR("Problem parsing key!");
  }

  if (strcmp(key.row, search_key.row)) {
    return false;
  }

  row_match = true;
  /**
   * The delete we were looking for has to be here or doesn't exist
   */
  if (key.column_family_code != search_key.column_family_code ||
      key.flag != search_key.flag) {
    return false;
  }

  create_key_and_append(m_start_delete_buf,
      key.flag, key.row, key.column_family_code, key.column_qualifier,
      key.timestamp, key.revision);

  return true;
}

template <typename IndexT>
void CellStoreScanner<IndexT>::set_start_deletes_readahead(bool search_cf_delete) {
  IndexIteratorT row_delete_iter;
  IndexIteratorT cf_delete_iter;
  bool same_block = false;
  bool row_match = false;

  // Set row delete as start offset for buffered read
  m_delete_search_keys[0].serial.ptr = m_start_delete_buf.base;
  m_delete_search_keys[0].load(m_delete_search_keys[0].serial);

  row_delete_iter = m_iter = m_index.lower_bound(m_delete_search_keys[0].serial);
  if (m_iter == m_index.end())
    return;

  start_buffered_read();

  /**
   * Check to see if DELETE_ROW exists for scan start key
   */
  m_has_start_row_delete = search_start_delete_keys_readahead(
      m_delete_search_keys[0], false, row_match);
  if (m_has_start_row_delete) {
    m_has_start_deletes = true;
  }
  m_start_delete_buf_offsets[2]= m_start_delete_buf.fill();

  /**
   * Check to see if DELETE_COLUMN_FAMILY exists for scan start key
   */
  if (search_cf_delete) {
    m_delete_search_keys[1].serial.ptr = m_start_delete_buf.base +
                                         m_start_delete_buf_offsets[0];
    m_delete_search_keys[1].load(m_delete_search_keys[1].serial);

    cf_delete_iter = m_iter = m_index.lower_bound(m_delete_search_keys[1].serial);
    if (m_iter == m_index.end())
      return;

    // if row delete and cf delete shf be in same block then theres no need to reload the block
    if (cf_delete_iter == row_delete_iter)
      same_block = true;

    // don't bother checking for col family delete if
    // it shd be in the same block as the row delete and the row was not found
    if (!same_block || row_match) {
      m_has_start_cf_delete = search_start_delete_keys_readahead(
          m_delete_search_keys[1], same_block, row_match);
      if (m_has_start_cf_delete) {
        m_has_start_deletes = true;
      }
    }
  }
  // Load delete keys from buffer
  if (m_has_start_row_delete) {
    m_start_deletes[0].serial.ptr = m_start_delete_buf.base + m_start_delete_buf_offsets[1];
    m_start_deletes[0].load(m_start_deletes[0].serial);
  }

  if (m_has_start_cf_delete) {
    m_start_deletes[1].serial.ptr = m_start_delete_buf.base + m_start_delete_buf_offsets[2];
    m_start_deletes[1].load(m_start_deletes[1].serial);
  }

  return;
}

/**
 * Open CellStore file and start async buffered read starting at position specified by m_iter
 */
template <typename IndexT>
void CellStoreScanner<IndexT>::start_buffered_read()
{
  IndexIteratorT  end_iter;
  uint32_t buf_size = m_cellstore->get_blocksize();

  if (buf_size < MINIMUM_READAHEAD_AMOUNT)
    buf_size = MINIMUM_READAHEAD_AMOUNT;

  m_start_offset = m_iter.value();

  if ((end_iter = m_index.upper_bound(m_end_key)) == m_index.end())
    m_end_offset = m_cellstore->get_data_end();
  else {
    ++end_iter;
    if (end_iter == m_index.end())
      m_end_offset = m_cellstore->get_data_end();
    else
      m_end_offset = end_iter.value();
  }

  try {
    m_fd = Global::dfs->open_buffered(m_cellstore->get_filename(), buf_size,
                                      2, m_start_offset, m_end_offset);
    m_close_fd_on_exit = true;
  }
  catch (Exception &e) {
    m_iter = m_index.end();
    HT_THROW2F(e.code(), e, "Problem opening cell store in "
               "readahead mode: %s", e.what());
  }

  if (!fetch_next_block_readahead()) {
    m_iter = m_index.end();
    return;
  }
}

template <typename IndexT>
bool CellStoreScanner<IndexT>::search_start_delete_keys_readahead(Key &search_key,
    bool block_loaded, bool &row_match) {
  SerializedKey cur_key;
  ByteString cur_value;
  Key key;

  row_match = false;

  if (!block_loaded) {
    if (!fetch_next_block_readahead())
      return false;
  }

  /**
   * move to start of search key in block
   */
  cur_key.ptr = m_block.ptr;
  cur_value.ptr = m_block.ptr + cur_key.length();

  while (cur_key < search_key.serial) {
    m_block.ptr = cur_value.ptr + cur_value.length();
    if (m_block.ptr >= m_block.end) {
      if (!fetch_next_block_readahead()) {
        return false;
      }
    }
    cur_key.ptr = m_block.ptr;
    cur_value.ptr = m_block.ptr + cur_key.length();
  }

  if (!key.load(cur_key)) {
    HT_ERROR("Problem parsing key!");
  }

  if (strcmp(key.row, search_key.row)) {
    return false;
  }

  row_match = true;
  if (key.column_family_code != search_key.column_family_code ||
      key.flag != search_key.flag) {
    return false;
  }

  create_key_and_append(m_start_delete_buf,
      key.flag, key.row, key.column_family_code, key.column_qualifier,
      key.timestamp, key.revision);

  return true;
}


template <typename IndexT>
bool CellStoreScanner<IndexT>::get(Key &key, ByteString &value) {

  if (m_has_start_deletes) {
    if (m_has_start_row_delete) {
      key = m_start_deletes[0];
      value = 0;
    }
    else {
      key = m_start_deletes[1];
      value = 0;
    }
  #ifdef STAT
    m_returned++;
  #endif
    return true;
  }

  if (m_iter == m_index.end())
    return false;

#ifdef STAT
  m_returned++;
#endif

  key = m_key;
  value = m_cur_value;

  return true;
}



template <typename IndexT>
void CellStoreScanner<IndexT>::forward() {

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

  while (true) {

    if (m_iter == m_index.end())
      return;

    m_block.ptr = m_cur_value.ptr + m_cur_value.length();

    if (m_block.ptr >= m_block.end) {
      if (m_readahead) {
        if (!fetch_next_block_readahead())
          return;
      }
      else if (!fetch_next_block())
        return;
    }

    m_cur_key.ptr = m_block.ptr;
    m_cur_value.ptr = m_block.ptr + m_cur_key.length();

    if (m_check_for_range_end && m_cur_key >= m_end_key) {
      m_iter = m_index.end();
      return;
    }

    /**
     * Column family check
     */
    if (!m_key.load(m_cur_key)) {
      HT_ERROR("Problem parsing key!");
      break;
    }
    if (m_key.flag == FLAG_DELETE_ROW
        || m_scan_context_ptr->family_mask[m_key.column_family_code])
      break;
  }
}



/**
 * This method fetches the 'next' compressed block of key/value pairs from the
 * underlying CellStore.
 *
 * Preconditions required to call this method: 1. m_block is cleared and m_iter
 * points to the m_index entry of the first block to fetch 'or' 2. m_block is
 * loaded with the current block and m_iter points to the m_index entry of the
 * current block
 *
 * @return true if next block successfully fetched, false if no next block
 */
template <typename IndexT>
bool CellStoreScanner<IndexT>::fetch_next_block() {
  // If we're at the end of the current block, deallocate and move to next
  if (m_block.base != 0 && m_block.ptr >= m_block.end) {
    Global::block_cache->checkin(m_file_id, m_block.offset);
    memset(&m_block, 0, sizeof(m_block));
    ++m_iter;
  }

  if (m_block.base == 0 && m_iter != m_index.end()) {
    DynamicBuffer expand_buf(0);
    uint32_t len;

    m_block.offset = m_iter.value();

    IndexIteratorT it_next = m_iter;
    ++it_next;
    if (it_next == m_index.end()) {
      m_block.zlength = m_cellstore->get_data_end() - m_block.offset;
      if (m_end_row.c_str()[0] != (char)0xff)
        m_check_for_range_end = true;
    }
    else {
      if (strcmp(it_next.key().row(), m_end_row.c_str()) >= 0)
        m_check_for_range_end = true;
      m_block.zlength = it_next.value() - m_block.offset;
    }

    /**
     * Cache lookup / block read
     */
    if (!Global::block_cache->checkout(m_file_id, (uint32_t)m_block.offset,
                                      (uint8_t **)&m_block.base, &len)) {
      bool second_try = false;
    try_again:
      try {
        DynamicBuffer buf(m_block.zlength);

        if (second_try)
          m_fd = m_cellstore->reopen_fd();

        /** Read compressed block **/
        Global::dfs->pread(m_fd, buf.ptr, m_block.zlength, m_block.offset);
        
        buf.ptr += m_block.zlength;
        /** inflate compressed block **/
        BlockCompressionHeader header;

        m_zcodec->inflate(buf, expand_buf, header);

        if (!header.check_magic(CellStore::DATA_BLOCK_MAGIC))
          HT_THROW(Error::BLOCK_COMPRESSOR_BAD_MAGIC,
                   "Error inflating cell store block - magic string mismatch");
      }
      catch (Exception &e) {
        HT_ERROR_OUT <<"Error reading cell store ("
                     << m_cellstore->get_filename() <<") : "
                     << e << HT_END;
        HT_ERROR_OUT << "pread(fd=" << m_fd << ", zlen="
                     << m_block.zlength << ", offset=" << m_block.offset
                     << HT_END;
        if (second_try)
          throw;
        second_try = true;
        goto try_again;
      }

      /** take ownership of inflate buffer **/
      size_t fill;
      m_block.base = expand_buf.release(&fill);
      len = fill;

      /** Insert block into cache  **/
      if (!Global::block_cache->insert_and_checkout(m_file_id, m_block.offset,
                                         (uint8_t *)m_block.base, len)) {
        delete [] m_block.base;

        if (!Global::block_cache->checkout(m_file_id, m_block.offset,
                                          (uint8_t **)&m_block.base, &len)) {
          HT_FATALF("Problem checking out block from cache file_id=%d, "
                    "offset=%lld", m_file_id, (Lld)m_block.offset);
        }
      }
    }
    m_block.ptr = m_block.base;
    m_block.end = m_block.base + len;

    return true;
  }
  return false;
}



/**
 * This method fetches the 'next' compressed block of key/value pairs from
 * the underlying CellStore.
 *
 * Preconditions required to call this method:
 *  1. m_block is cleared and m_iter points to the m_index entry of the first
 *     block to fetch
 *    'or'
 *  2. m_block is loaded with the current block and m_iter points to the
 *     m_index entry of the current block
 *
 * @return true if next block successfully fetched, false if no next block
 */
template <typename IndexT>
bool CellStoreScanner<IndexT>::fetch_next_block_readahead() {
  // If we're at the end of the current block, deallocate and move to next
  if (m_block.base != 0 && m_block.ptr >= m_block.end) {
    delete [] m_block.base;
    memset(&m_block, 0, sizeof(m_block));
    ++m_iter;
  }

  if (m_block.base == 0 && m_iter != m_index.end()) {
    DynamicBuffer expand_buf(0);
    uint32_t len;
    uint32_t nread;

    m_block.offset = m_iter.value();
    assert(m_block.offset == m_start_offset);

    IndexIteratorT it_next = m_iter;
    ++it_next;
    if (it_next == m_index.end()) {
      m_block.zlength = m_cellstore->get_data_end() - m_block.offset;
      if (m_end_row.c_str()[0] != (char)0xff)
        m_check_for_range_end = true;
    }
    else {
      if (strcmp(it_next.key().row(), m_end_row.c_str()) >= 0)
        m_check_for_range_end = true;
      m_block.zlength = it_next.value() - m_block.offset;
    }

    try {
      DynamicBuffer buf(m_block.zlength);
      /** Read compressed block **/
      nread = Global::dfs->read(m_fd, buf.ptr, m_block.zlength);
      buf.ptr += m_block.zlength;
      /** inflate compressed block **/
      BlockCompressionHeader header;

      m_zcodec->inflate(buf, expand_buf, header);

      if (!header.check_magic(CellStore::DATA_BLOCK_MAGIC))
        HT_THROW(Error::BLOCK_COMPRESSOR_BAD_MAGIC,
                 "Error inflating cell store block - magic string mismatch");
    }
    catch (Exception &e) {
      HT_ERROR_OUT <<"Error reading cell store ("
                   << m_cellstore->get_filename() <<") block: "
                   << e << HT_END;
      HT_THROW2(e.code(), e, e.what());
    }
    // Errors should've been caught by checksum/decompression
    HT_EXPECT(nread == m_block.zlength, Error::UNPOSSIBLE);
    m_start_offset += nread;

    /** take ownership of inflate buffer **/
    size_t fill;
    m_block.base = expand_buf.release(&fill);
    len = fill;

    m_block.ptr = m_block.base;
    m_block.end = m_block.base + len;

    return true;
  }
  return false;
}

template class CellStoreScanner<CellStoreBlockIndexMap<uint32_t> >;
template class CellStoreScanner<CellStoreBlockIndexMap<int64_t> >;
