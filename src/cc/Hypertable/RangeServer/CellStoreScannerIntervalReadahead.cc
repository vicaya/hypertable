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
#include "Common/Filesystem.h"
#include "Common/System.h"

#include "Hypertable/Lib/BlockCompressionHeader.h"
#include "Global.h"
#include "CellStoreBlockIndexMap.h"

#include "CellStoreScannerIntervalReadahead.h"


using namespace Hypertable;

namespace {
  const uint32_t MINIMUM_READAHEAD_AMOUNT = 524288;
}


template <typename IndexT>
CellStoreScannerIntervalReadahead<IndexT>::CellStoreScannerIntervalReadahead(CellStore *cellstore,
     IndexT *index, SerializedKey start_key, SerializedKey end_key, ScanContextPtr &scan_ctx) :
  m_cellstore(cellstore), m_end_key(end_key), m_zcodec(0), m_fd(-1), m_offset(0),
  m_end_offset(0), m_check_for_range_end(false), m_eos(false), m_scan_ctx(scan_ctx),
  m_oflags(0) {
  int64_t start_offset;

  memset(&m_block, 0, sizeof(m_block));
  m_zcodec = m_cellstore->create_block_compression_codec();
  m_key_decompressor = m_cellstore->create_key_decompressor();

  uint16_t csversion = boost::any_cast<uint16_t>(cellstore->get_trailer()->get("version"));
  if (csversion >= 4)
    m_oflags = Filesystem::OPEN_FLAG_DIRECTIO;

  if (index) {
    IndexIteratorT iter, end_iter;

    iter = (start_key) ? index->lower_bound(start_key) : index->begin();
    if (iter == index->end()) {
      m_eos = true;
      return;
    }

    start_offset = iter.value();

    if (!end_key || (end_iter = index->upper_bound(end_key)) == index->end())
      m_end_offset = index->end_of_last_block();
    else {
      ++end_iter;
      if (end_iter == index->end())
        m_end_offset = index->end_of_last_block();
      else
        m_end_offset = end_iter.value();
    }
  }
  else {
    start_offset = 0;
    m_end_offset = cellstore->end_of_last_block();
  }
  m_offset = start_offset;

  uint32_t buf_size = cellstore->get_blocksize();

  if (buf_size < MINIMUM_READAHEAD_AMOUNT)
    buf_size = MINIMUM_READAHEAD_AMOUNT;

  try {
    m_fd = Global::dfs->open_buffered(cellstore->get_filename(), m_oflags,
                                      buf_size, 5, start_offset, m_end_offset);
  }
  catch (Exception &e) {
    m_eos = true;
    HT_THROW2F(e.code(), e, "Problem opening cell store in "
               "readahead mode: %s", e.what());
  }

  if (!fetch_next_block_readahead()) {
    m_eos = true;
    return;
  }

  /**
   * Seek to start of range in block
   */

  if (start_key) {
    const uint8_t *ptr;
    while (m_key_decompressor->less_than(start_key)) {
      ptr = m_cur_value.ptr + m_cur_value.length();
      if (ptr >= m_block.end) {
        if (!fetch_next_block_readahead(true)) {
          m_eos = true;
          return;
        }
      }
      else
        m_cur_value.ptr = m_key_decompressor->add(ptr);
    }
  }

  /**
   * End of range check
   */
  if (m_end_key && !m_key_decompressor->less_than(m_end_key)) {
    m_eos = true;
    return;
  }

  /**
   * Column family check
   */
  m_key_decompressor->load(m_key);
  if (m_key.flag != FLAG_DELETE_ROW &&
      !m_scan_ctx->family_mask[m_key.column_family_code])
    forward();
}


template <typename IndexT>
CellStoreScannerIntervalReadahead<IndexT>::~CellStoreScannerIntervalReadahead() {
  try {
    if (m_fd != -1)
      Global::dfs->close(m_fd, 0);
    delete [] m_block.base;
    delete m_zcodec;
    delete m_key_decompressor;
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
  }
  catch (...) {
    HT_ERRORF("Unknown exception caught in %s", HT_FUNC);
  }
}



template <typename IndexT>
bool CellStoreScannerIntervalReadahead<IndexT>::get(Key &key, ByteString &value) {

  if (m_eos)
    return false;

  key = m_key;
  value = m_cur_value;

  return true;
}



template <typename IndexT>
void CellStoreScannerIntervalReadahead<IndexT>::forward() {
  const uint8_t *ptr;

  while (true) {

    if (m_eos)
      return;

    ptr = m_cur_value.ptr + m_cur_value.length();

    if (ptr >= m_block.end) {
      if (!fetch_next_block_readahead(true)) {
        m_eos = true;
        return;
      }
      if (m_check_for_range_end && !m_key_decompressor->less_than(m_end_key)) {
        m_eos = true;
        return;
      }
    }
    else {
      m_cur_value.ptr = m_key_decompressor->add(ptr);
      if (m_check_for_range_end && !m_key_decompressor->less_than(m_end_key)) {
        m_eos = true;
        return;
      }
    }

    /**
     * Column family check
     */
    m_key_decompressor->load(m_key);
    if (m_key.flag == FLAG_DELETE_ROW
        || m_scan_ctx->family_mask[m_key.column_family_code])
      break;
  }
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
 * @param eob true if at end of block
 * @return true if next block successfully fetched, false if no next block
 */
template <typename IndexT>
bool CellStoreScannerIntervalReadahead<IndexT>::fetch_next_block_readahead(bool eob) {

  // If we're at the end of the current block, deallocate and move to next
  if (m_block.base != 0 && eob) {
    delete [] m_block.base;
    memset(&m_block, 0, sizeof(m_block));
  }

  if (m_offset >= m_end_offset)
    m_eos = true;

  if (m_block.base == 0 && !m_eos) {
    DynamicBuffer expand_buf(0);
    uint32_t len;
    uint32_t nread;

    m_block.offset = m_offset;

    /** Read header **/
    try {
      BlockCompressionHeader header;
      DynamicBuffer input_buf( header.length() );

      nread = Global::dfs->read(m_fd, input_buf.base, header.length() );
      HT_EXPECT(nread == header.length(), Error::RANGESERVER_SHORT_CELLSTORE_READ);

      size_t remaining = nread;

      header.decode((const uint8_t **)&input_buf.ptr, &remaining);

      size_t extra = 0;
      if (m_oflags & Filesystem::OPEN_FLAG_DIRECTIO) {
        if ((header.length()+header.get_data_zlength())%HT_DIRECT_IO_ALIGNMENT)
          extra = HT_DIRECT_IO_ALIGNMENT - ((header.length()+header.get_data_zlength())%HT_DIRECT_IO_ALIGNMENT);
      }

      input_buf.grow( input_buf.fill() + header.get_data_zlength() + extra );
      nread = Global::dfs->read(m_fd, input_buf.ptr,  header.get_data_zlength()+extra);
      HT_EXPECT(nread == header.get_data_zlength()+extra, Error::RANGESERVER_SHORT_CELLSTORE_READ);
      input_buf.ptr += header.get_data_zlength() + extra;

      if (m_offset + (int64_t)input_buf.fill() >= m_end_offset && m_end_key)
        m_check_for_range_end = true;
      m_offset += input_buf.fill();

      m_zcodec->inflate(input_buf, expand_buf, header);

      if (!header.check_magic(CellStore::DATA_BLOCK_MAGIC))
        HT_THROW(Error::BLOCK_COMPRESSOR_BAD_MAGIC,
                 "Error inflating cell store block - magic string mismatch");
    }
    catch (Exception &e) {
      HT_ERROR_OUT <<"Error reading cell store ( fd=" << m_fd << " file="
                   << m_cellstore->get_filename() <<") block: "
                   << e << HT_END;
      HT_THROW2(e.code(), e, e.what());
    }

    /** take ownership of inflate buffer **/
    size_t fill;
    m_block.base = expand_buf.release(&fill);
    len = fill;

    m_key_decompressor->reset();
    m_block.end = m_block.base + len;
    m_cur_value.ptr = m_key_decompressor->add(m_block.base);

    return true;
  }
  return false;
}

template class CellStoreScannerIntervalReadahead<CellStoreBlockIndexMap<uint32_t> >;
template class CellStoreScannerIntervalReadahead<CellStoreBlockIndexMap<int64_t> >;
