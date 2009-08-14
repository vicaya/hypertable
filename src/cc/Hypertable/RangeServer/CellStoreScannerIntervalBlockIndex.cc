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

#include "CellStoreScannerIntervalBlockIndex.h"

using namespace Hypertable;


template <typename IndexT>
CellStoreScannerIntervalBlockIndex<IndexT>::CellStoreScannerIntervalBlockIndex(CellStore *cellstore,
  IndexT *index, SerializedKey start_key, SerializedKey end_key, ScanContextPtr &scan_ctx) :
  m_cellstore(cellstore), m_index(index), m_start_key(start_key),
  m_end_key(end_key), m_fd(-1), m_check_for_range_end(false), m_scan_ctx(scan_ctx) {

  memset(&m_block, 0, sizeof(m_block));
  m_file_id = m_cellstore->get_file_id();
  m_zcodec = m_cellstore->create_block_compression_codec();

  m_end_row = (m_end_key) ? m_end_key.row() : Key::END_ROW_MARKER;
  m_fd = m_cellstore->get_fd();

  if (m_start_key && (m_iter = m_index->lower_bound(m_start_key)) == m_index->end())
    return;

  if (!fetch_next_block()) {
    m_iter = m_index->end();
    return;
  }

  if (m_start_key) {
    while (m_cur_key < m_start_key) {
      m_cur_key.ptr = m_cur_value.ptr + m_cur_value.length();
      m_cur_value.ptr = m_cur_key.ptr + m_cur_key.length();
      if (m_cur_key.ptr >= m_block.end && !fetch_next_block()) {
        m_iter = m_index->end();
        return;
      }
    }
  }

  /**
   * End of range check
   */
  if (m_end_key && m_cur_key >= m_end_key) {
    m_iter = m_index->end();
    return;
  }

  /**
   * Column family check
   */
  if (!m_key.load(m_cur_key))
    HT_ERROR("Problem parsing key!");
  else if (m_key.flag != FLAG_DELETE_ROW &&
           !m_scan_ctx->family_mask[m_key.column_family_code])
    forward();


}


template <typename IndexT>
CellStoreScannerIntervalBlockIndex<IndexT>::~CellStoreScannerIntervalBlockIndex() {
  if (m_block.base != 0)
    Global::block_cache->checkin(m_file_id, m_block.offset);
  delete m_zcodec;
}

template <typename IndexT>
bool CellStoreScannerIntervalBlockIndex<IndexT>::get(Key &key, ByteString &value) {

  if (m_iter == m_index->end())
    return false;

  key = m_key;
  value = m_cur_value;

  return true;
}



template <typename IndexT>
void CellStoreScannerIntervalBlockIndex<IndexT>::forward() {

  while (true) {

    if (m_iter == m_index->end())
      return;

    m_cur_key.ptr = m_cur_value.ptr + m_cur_value.length();

    if (m_cur_key.ptr >= m_block.end && !fetch_next_block())
      return;

    if (m_check_for_range_end && m_cur_key >= m_end_key) {
      m_iter = m_index->end();
      return;
    }

    m_cur_value.ptr = m_cur_key.ptr + m_cur_key.length();

    /**
     * Column family check
     */
    if (!m_key.load(m_cur_key))
      HT_ERROR("Problem parsing key!");
    if (m_key.flag == FLAG_DELETE_ROW
        || m_scan_ctx->family_mask[m_key.column_family_code])
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
bool CellStoreScannerIntervalBlockIndex<IndexT>::fetch_next_block() {

  // If we're at the end of the current block, deallocate and move to next
  if (m_block.base != 0 && m_cur_key.ptr >= m_block.end) {
    Global::block_cache->checkin(m_file_id, m_block.offset);
    memset(&m_block, 0, sizeof(m_block));
    ++m_iter;
  }

  if (m_block.base == 0 && m_iter != m_index->end()) {
    DynamicBuffer expand_buf(0);
    uint32_t len;

    m_block.offset = m_iter.value();

    IndexIteratorT it_next = m_iter;
    ++it_next;
    if (it_next == m_index->end()) {
      m_block.zlength = m_index->end_of_last_block() - m_block.offset;
      if (m_end_row[0] != (char)0xff)
        m_check_for_range_end = true;
    }
    else {
      if (strcmp(it_next.key().row(), m_end_row) >= 0)
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
    m_block.end = m_block.base + len;
    m_cur_key.ptr = m_block.base;
    m_cur_value.ptr = m_cur_key.ptr + m_cur_key.length();

    return true;
  }
  return false;
}


template class CellStoreScannerIntervalBlockIndex<CellStoreBlockIndexMap<uint32_t> >;
template class CellStoreScannerIntervalBlockIndex<CellStoreBlockIndexMap<int64_t> >;
