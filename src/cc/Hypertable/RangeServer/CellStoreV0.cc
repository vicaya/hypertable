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

#include <boost/algorithm/string.hpp>
#include <boost/scoped_array.hpp>

#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/System.h"

#include "AsyncComm/Protocol.h"

#include "Hypertable/Lib/BlockCompressionHeader.h"
#include "Hypertable/Lib/CompressorFactory.h"
#include "Hypertable/Lib/Key.h"
#include "Hypertable/Lib/Schema.h"

#include "CellStoreV0.h"
#include "CellStoreTrailerV0.h"
#include "CellStoreScanner.h"

#include "FileBlockCache.h"
#include "Global.h"
#include "Config.h"

using namespace std;
using namespace Hypertable;

namespace {
  const uint32_t MAX_APPENDS_OUTSTANDING = 3;
}


CellStoreV0::CellStoreV0(Filesystem *filesys)
  : m_filesys(filesys), m_fd(-1), m_filename(), m_compressor(0), m_buffer(0),
    m_fix_index_buffer(0), m_var_index_buffer(0), m_memory_consumed(0),
    m_outstanding_appends(0), m_offset(0), m_last_key(0), m_file_length(0),
    m_disk_usage(0), m_file_id(0), m_uncompressed_blocksize(0),
    m_bloom_filter_mode(BLOOM_FILTER_DISABLED), m_bloom_filter(0),
    m_bloom_filter_items(0) {

  m_file_id = FileBlockCache::get_next_file_id();
  assert(sizeof(float) == 4);
}


CellStoreV0::~CellStoreV0() {
  try {
    delete m_compressor;

    if (m_bloom_filter != 0) {
      delete m_bloom_filter;
    }
    if (m_bloom_filter_items != 0) {
      delete m_bloom_filter_items;
    }
    if (m_fd != -1)
      m_filesys->close(m_fd);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
  }
  if (m_memory_consumed)
    Global::memory_tracker->subtract(m_memory_consumed);
}


BlockCompressionCodec *CellStoreV0::create_block_compression_codec() {
  return CompressorFactory::create_block_codec(
      (BlockCompressionCodec::Type)m_trailer.compression_type);
}


const char *CellStoreV0::get_split_row() {
  if (m_split_row != "")
    return m_split_row.c_str();
  return 0;
}

CellListScanner *CellStoreV0::create_scanner(ScanContextPtr &scan_ctx) {
  return new CellStoreScanner<CellStoreBlockIndexMap<uint32_t> >(this, scan_ctx, &m_index_map32);
}


void
CellStoreV0::create(const char *fname, size_t max_entries,
                    PropertiesPtr &props) {
  uint32_t blocksize = props->get("blocksize", uint32_t(0));
  String compressor = props->get("compressor", String());

  assert(Config::properties); // requires Config::init* first

  if (blocksize == 0)
    blocksize = Config::get_i32("Hypertable.RangeServer.CellStore"
                                ".DefaultBlockSize");
  if (compressor.empty())
    compressor = Config::get_str("Hypertable.RangeServer.CellStore"
                                 ".DefaultCompressor");
  if (!props->has("bloom-filter-mode")) {
    // probably not called from AccessGroup
    Schema::parse_bloom_filter(Config::get_str("Hypertable.RangeServer"
        ".CellStore.DefaultBloomFilter"), props);
  }

  m_buffer.reserve(blocksize*4);

  m_max_entries = max_entries;

  m_fd = -1;
  m_offset = 0;
  m_last_key = 0;
  m_fix_index_buffer.reserve(blocksize);
  m_var_index_buffer.reserve(blocksize);

  m_uncompressed_data = 0.0;
  m_compressed_data = 0.0;

  m_trailer.clear();
  m_trailer.blocksize = blocksize;
  m_uncompressed_blocksize = blocksize;

  m_filename = fname;

  m_start_row = "";
  m_end_row = Key::END_ROW_MARKER;

  m_trailer.compression_type = CompressorFactory::parse_block_codec_spec(
      compressor, m_compressor_args);

  m_compressor = CompressorFactory::create_block_codec(
      (BlockCompressionCodec::Type)m_trailer.compression_type,
      m_compressor_args);

  m_fd = m_filesys->create(m_filename, Filesystem::OPEN_FLAG_OVERWRITE, -1, -1, -1);

  m_bloom_filter_mode = props->get<BloomFilterMode>("bloom-filter-mode");
  m_max_approx_items = props->get_i32("max-approx-items");
  m_trailer.filter_false_positive_prob = props->get_f64("false-positive");

  if (m_bloom_filter_mode != BLOOM_FILTER_DISABLED) {
    m_bloom_filter_items = new BloomFilterItems(); // aproximator items
  }
  HT_DEBUG_OUT <<"bloom-filter-mode="<< m_bloom_filter_mode
      <<" max-approx-items="<< m_max_approx_items <<" false-positive="
      << m_trailer.filter_false_positive_prob << HT_END;
}


void CellStoreV0::create_bloom_filter(bool is_approx) {
  assert(!m_bloom_filter && m_bloom_filter_items);

  HT_DEBUG_OUT << "Creating new BloomFilter for CellStore '"
    << m_filename <<"' for "<< (is_approx ? "estimated " : "")
    << m_trailer.num_filter_items << " items"<< HT_END;
  try {
    m_bloom_filter = new BloomFilter(m_trailer.num_filter_items,
        m_trailer.filter_false_positive_prob);
  }
  catch (Exception &e) {
    HT_FATAL_OUT << "Error creating new BloomFilter for CellStore '"
                 << m_filename <<"' for "<< (is_approx ? "estimated " : "")
                 << m_trailer.num_filter_items << " items - "<< e << HT_END;
  }

  foreach(const Blob &blob, *m_bloom_filter_items)
    m_bloom_filter->insert(blob.start, blob.size);

  delete m_bloom_filter_items;
  m_bloom_filter_items = 0;

  HT_DEBUG_OUT << "Created new BloomFilter for CellStore '"
    << m_filename <<"'"<< HT_END;
}


void CellStoreV0::add(const Key &key, const ByteString value) {
  EventPtr event_ptr;
  DynamicBuffer zbuf;

  if (key.revision > m_trailer.revision)
    m_trailer.revision = key.revision;

  if (m_buffer.fill() > m_uncompressed_blocksize) {
    BlockCompressionHeader header(DATA_BLOCK_MAGIC);

    add_index_entry(m_last_key, m_offset);

    m_uncompressed_data += (float)m_buffer.fill();
    m_compressor->deflate(m_buffer, zbuf, header);
    m_compressed_data += (float)zbuf.fill();
    m_buffer.clear();

    uint64_t llval = ((uint64_t)m_trailer.blocksize
        * (uint64_t)m_uncompressed_data) / (uint64_t)m_compressed_data;
    m_uncompressed_blocksize = (uint32_t)llval;

    if (m_outstanding_appends >= MAX_APPENDS_OUTSTANDING) {
      if (!m_sync_handler.wait_for_reply(event_ptr)) {
        if (event_ptr->type == Event::MESSAGE)
          HT_THROWF(Hypertable::Protocol::response_code(event_ptr),
             "Problem writing to DFS file '%s' : %s", m_filename.c_str(),
             Hypertable::Protocol::string_format_message(event_ptr).c_str());
        HT_THROWF(event_ptr->error,
                  "Problem writing to DFS file '%s'", m_filename.c_str());
      }
      m_outstanding_appends--;
    }

    size_t zlen = zbuf.fill();
    StaticBuffer send_buf(zbuf);

    try { m_filesys->append(m_fd, send_buf, 0, &m_sync_handler); }
    catch (Exception &e) {
      HT_THROW2F(e.code(), e, "Problem writing to DFS file '%s'",
                 m_filename.c_str());
    }
    m_outstanding_appends++;
    m_offset += zlen;
  }

  size_t value_len = value.length();

  m_buffer.ensure(key.length + value_len);

  m_last_key.ptr = m_buffer.add_unchecked(key.serial.ptr, key.length);
  m_buffer.add_unchecked(value.ptr, value_len);

  if (m_bloom_filter_mode != BLOOM_FILTER_DISABLED) {
    if (m_trailer.total_entries < m_max_approx_items) {
      m_bloom_filter_items->insert(key.row, key.row_len);

      if (m_bloom_filter_mode == BLOOM_FILTER_ROWS_COLS)
        m_bloom_filter_items->insert(key.row, key.row_len + 2);

      if (m_trailer.total_entries == m_max_approx_items - 1) {
        m_trailer.num_filter_items = (size_t)(((double)m_max_entries
            / (double)m_max_approx_items) * m_bloom_filter_items->size());
        create_bloom_filter(true);
      }
    }
    else {
      assert(!m_bloom_filter_items && m_bloom_filter);

      m_bloom_filter->insert(key.row);

      if (m_bloom_filter_mode == BLOOM_FILTER_ROWS_COLS)
        m_bloom_filter->insert(key.row, key.row_len + 2);
    }
  }

  m_trailer.total_entries++;
}


void CellStoreV0::finalize(TableIdentifier *table_identifier) {
  EventPtr event_ptr;
  size_t zlen;
  DynamicBuffer zbuf(0);
  size_t len;
  uint8_t *base;
  SerializedKey key;
  StaticBuffer send_buf;

  if (m_buffer.fill() > 0) {
    BlockCompressionHeader header(DATA_BLOCK_MAGIC);

    add_index_entry(m_last_key, m_offset);

    m_uncompressed_data += (float)m_buffer.fill();
    m_compressor->deflate(m_buffer, zbuf, header);
    m_compressed_data += (float)zbuf.fill();

    zlen = zbuf.fill();
    send_buf = zbuf;

    if (m_outstanding_appends >= MAX_APPENDS_OUTSTANDING) {
      if (!m_sync_handler.wait_for_reply(event_ptr))
        HT_THROWF(Protocol::response_code(event_ptr),
                  "Problem finalizing CellStore file '%s' : %s",
                  m_filename.c_str(),
                  Protocol::string_format_message(event_ptr).c_str());
      m_outstanding_appends--;
    }

    m_filesys->append(m_fd, send_buf, 0, &m_sync_handler);

    m_outstanding_appends++;
    m_offset += zlen;
  }

  m_buffer.free();

  m_trailer.fix_index_offset = m_offset;
  if (m_uncompressed_data == 0)
    m_trailer.compression_ratio = 1.0;
  else
    m_trailer.compression_ratio = m_compressed_data / m_uncompressed_data;
  m_trailer.version = 0;

  /**
   * Chop the Index buffers down to the exact length
   */
  base = m_fix_index_buffer.release(&len);
  m_fix_index_buffer.reserve(len);
  m_fix_index_buffer.add_unchecked(base, len);
  delete [] base;
  base = m_var_index_buffer.release(&len);
  m_var_index_buffer.reserve(len);
  m_var_index_buffer.add_unchecked(base, len);
  delete [] base;

  /**
   * Write fixed index
   */
  {
    BlockCompressionHeader header(INDEX_FIXED_BLOCK_MAGIC);
    m_compressor->deflate(m_fix_index_buffer, zbuf, header);
  }

  zlen = zbuf.fill();
  send_buf = zbuf;

  m_filesys->append(m_fd, send_buf, 0, &m_sync_handler);

  m_outstanding_appends++;
  m_offset += zlen;

  /**
   * Write variable index
   */
  {
    BlockCompressionHeader header(INDEX_VARIABLE_BLOCK_MAGIC);
    m_trailer.var_index_offset = m_offset;
    m_compressor->deflate(m_var_index_buffer, zbuf, header);
  }

  zlen = zbuf.fill();
  send_buf = zbuf;

  m_filesys->append(m_fd, send_buf, 0, &m_sync_handler);

  m_outstanding_appends++;
  m_offset += zlen;

  // write filter_offset
  m_trailer.filter_offset = m_offset;

  // if bloom_items haven't been spilled to create a bloom filter yet, do it
  if (m_bloom_filter_mode != BLOOM_FILTER_DISABLED) {
    if (m_bloom_filter_items) {
      m_trailer.num_filter_items = m_bloom_filter_items->size();
      create_bloom_filter();
    }
    assert(!m_bloom_filter_items && m_bloom_filter);

    m_bloom_filter->serialize(send_buf);
    m_filesys->append(m_fd, send_buf, 0, &m_sync_handler);

    m_outstanding_appends++;
    m_offset += m_bloom_filter->size();
  }

  /** Set up m_index_map32 **/
  m_index_map32.load(m_fix_index_buffer, m_var_index_buffer,
                     m_trailer.fix_index_offset);
  record_split_row( m_index_map32.middle_key() );

  // deallocate fix index data
  delete [] m_fix_index_buffer.release();

  // Add table information
  m_trailer.table_id = table_identifier->id;
  m_trailer.table_generation = table_identifier->generation;

  // write trailer
  zbuf.clear();
  zbuf.reserve(m_trailer.size());
  m_trailer.serialize(zbuf.ptr);
  zbuf.ptr += m_trailer.size();

  zlen = zbuf.fill();
  send_buf = zbuf;

  m_filesys->append(m_fd, send_buf);

  m_outstanding_appends++;
  m_offset += zlen;

  /** close file for writing **/
  m_filesys->close(m_fd);

  /** Set file length **/
  m_file_length = m_offset;

  /** Re-open file for reading **/
  m_fd = m_filesys->open(m_filename);

  m_disk_usage = (uint32_t)m_file_length;
  if (m_disk_usage < 0)
    HT_WARN_OUT << "[Issue 339] Disk usage for " << m_filename << "=" << m_disk_usage
                << HT_END;

  m_memory_consumed = sizeof(CellStoreV0) + m_index_map32.memory_used();
  if (m_bloom_filter)
    m_memory_consumed += m_bloom_filter->size();
  Global::memory_tracker->add(m_memory_consumed);

  delete m_compressor;
  m_compressor = 0;

}


void CellStoreV0::add_index_entry(const SerializedKey key, uint32_t offset) {

  size_t key_len = key.length();
  m_var_index_buffer.ensure(key_len);
  memcpy(m_var_index_buffer.ptr, key.ptr, key_len);
  m_var_index_buffer.ptr += key_len;

  // Serialize offset into fix index buffer
  m_fix_index_buffer.ensure(sizeof(offset));
  memcpy(m_fix_index_buffer.ptr, &offset, sizeof(offset));
  m_fix_index_buffer.ptr += sizeof(offset);

  m_trailer.index_entries++;
}


void
CellStoreV0::open(const String &fname, const String &start_row,
                  const String &end_row, int32_t fd, int64_t file_length,
                  CellStoreTrailer *trailer) {
  m_filename = fname;
  m_start_row = start_row;
  m_end_row = end_row;
  m_fd = fd;
  m_file_length = file_length;

  CellStoreTrailerV0 *trailer_v0 = static_cast<CellStoreTrailerV0 *>(trailer);

  m_trailer = *trailer_v0;

  /** Sanity check trailer **/
  HT_ASSERT(m_trailer.version == 0);

  if (!(m_trailer.fix_index_offset < m_trailer.var_index_offset &&
        m_trailer.var_index_offset < m_file_length))
    HT_THROWF(Error::RANGESERVER_CORRUPT_CELLSTORE,
              "Bad index offsets in CellStore trailer fix=%u, var=%u, "
              "length=%llu, file='%s'", m_trailer.fix_index_offset,
              m_trailer.var_index_offset, (Llu)m_file_length, fname.c_str());

  load_index();
}


void CellStoreV0::load_index() {
  uint32_t amount, index_amount;
  uint32_t len = 0;
  BlockCompressionHeader header;
  SerializedKey key;
  bool inflating_fixed=true;
  bool second_try = false;

  m_compressor = create_block_compression_codec();

  amount = index_amount = m_trailer.filter_offset
                          - m_trailer.fix_index_offset;

 try_again:

  try {
    DynamicBuffer buf(amount);

    if (second_try)
      reopen_fd();

    /** Read index data **/
    len = m_filesys->pread(m_fd, buf.ptr, amount, m_trailer.fix_index_offset);

    if (len != amount)
      HT_THROWF(Error::DFSBROKER_IO_ERROR, "Error loading index for "
                "CellStore '%s' : tried to read %d but only got %d",
                m_filename.c_str(), amount, len);
    /** inflate fixed index **/
    buf.ptr += (m_trailer.var_index_offset - m_trailer.fix_index_offset);
    m_compressor->inflate(buf, m_fix_index_buffer, header);

    inflating_fixed = false;

    if (!header.check_magic(INDEX_FIXED_BLOCK_MAGIC))
      HT_THROW(Error::BLOCK_COMPRESSOR_BAD_MAGIC, m_filename);

    /** inflate variable index **/
    DynamicBuffer vbuf(0, false);
    amount = m_trailer.filter_offset - m_trailer.var_index_offset;
    vbuf.base = buf.ptr;
    vbuf.ptr = buf.ptr + amount;

    m_compressor->inflate(vbuf, m_var_index_buffer, header);

    if (!header.check_magic(INDEX_VARIABLE_BLOCK_MAGIC))
      HT_THROW(Error::BLOCK_COMPRESSOR_BAD_MAGIC, m_filename);
  }
  catch (Exception &e) {
    String msg;
    if (inflating_fixed) {
      msg = String("Error inflating FIXED index for cellstore '")
            + m_filename + "'";
      HT_ERROR_OUT << msg << ": "<< e << HT_END;
    }
    else {
      msg = "Error inflating VARIABLE index for cellstore '" + m_filename + "'";
      HT_ERROR_OUT << msg << ": " <<  e << HT_END;
    }
    HT_ERROR_OUT << "pread(fd=" << m_fd << ", len=" << len << ", amount="
        << index_amount << ")\n" << HT_END;
    HT_ERROR_OUT << m_trailer << HT_END;
    if (second_try)
      HT_THROW2(e.code(), e, msg);
    second_try = true;
    goto try_again;
  }

  /** Set up m_index_map32 **/
  m_index_map32.load(m_fix_index_buffer, m_var_index_buffer,
                     m_trailer.fix_index_offset, m_start_row, m_end_row);
  record_split_row( m_index_map32.middle_key() );

  // instantiate a bloom filter and read in the bloom filter bits.
  // If num_filter_items in trailer is 0, means bloom_filter is disabled..
  if (m_trailer.num_filter_items != 0) {
    HT_DEBUG_OUT << "Creating new BloomFilter for CellStore '"
          << m_filename <<"' with "<< m_trailer.num_filter_items
          << " items"<< HT_END;
    try {
      m_bloom_filter = new BloomFilter(m_trailer.num_filter_items,
                                       m_trailer.filter_false_positive_prob);
    }
    catch (Exception &e) {
        HT_FATAL_OUT << "Error loading BloomFilter for CellStore '"
                     << m_filename <<"' with "<< m_trailer.num_filter_items
                     << " items -"<< e << HT_END;
    }
    amount = (m_file_length - m_trailer.size()) - m_trailer.filter_offset;
    len = m_filesys->pread(m_fd, m_bloom_filter->ptr(), amount,
                             m_trailer.filter_offset);

    if (len != amount) {
      HT_THROWF(Error::DFSBROKER_IO_ERROR, "Problem loading bloomfilter for"
                "CellStore '%s' : tried to read %d but only got %d",
                m_filename.c_str(), amount, len);

    }
  } else {
    assert((m_file_length - m_trailer.size()) == m_trailer.filter_offset);
  }

  m_disk_usage = m_index_map32.disk_used();
  if (m_disk_usage < 0)
    HT_WARN_OUT << "[Issue 339] Disk usage for " << m_filename << "=" << m_disk_usage
                << HT_END;

  m_memory_consumed = sizeof(CellStoreV0) + m_index_map32.memory_used();
  if (m_bloom_filter)
    m_memory_consumed += m_bloom_filter->size();
  Global::memory_tracker->add(m_memory_consumed);

  delete m_compressor;
  m_compressor = 0;
  delete [] m_fix_index_buffer.release();
}


bool CellStoreV0::may_contain(ScanContextPtr &scan_context) {
  switch (m_bloom_filter_mode) {
    case BLOOM_FILTER_ROWS:
      return may_contain(scan_context->start_row);
    case BLOOM_FILTER_ROWS_COLS:
      if (may_contain(scan_context->start_row)) {
        SchemaPtr &schema = scan_context->schema;
        size_t rowlen = scan_context->start_row.length();
        boost::scoped_array<char> rowcol(new char[rowlen + 2]);
        memcpy(rowcol.get(), scan_context->start_row.c_str(), rowlen + 1);

        foreach(const char *col, scan_context->spec->columns) {
          uint8_t column_family_id = schema->get_column_family(col)->id;
          rowcol[rowlen + 1] = column_family_id;

          if (may_contain(rowcol.get(), rowlen + 2))
            return true;
        }
      }
      return false;
    case BLOOM_FILTER_DISABLED:
      return true;
    default:
      HT_ASSERT(!"unpossible bloom filter mode!");
  }
  return false; // silence stupid compilers
}


bool CellStoreV0::may_contain(const void *ptr, size_t len) {
  assert(m_bloom_filter != 0);
  bool may_contain = m_bloom_filter->may_contain(ptr, len);
  return may_contain;
}


void CellStoreV0::display_block_info() {
  SerializedKey last_key;
  uint32_t last_offset = 0;
  uint32_t block_size;
  size_t i=0;
  for (CellStoreBlockIndexMap<uint32_t>::iterator iter = m_index_map32.begin();
       iter != m_index_map32.end(); ++iter) {
    if (last_key) {
      block_size = iter.value() - last_offset;
      cout << i << ": offset=" << last_offset << " size=" << block_size
           << " row=" << last_key.row() << endl;
      i++;
    }
    last_offset = iter.value();
    last_key = iter.key();
  }
  if (last_key) {
    block_size = m_trailer.filter_offset - last_offset;
    cout << i << ": offset=" << last_offset << " size=" << block_size
         << " row=" << last_key.row() << endl;
  }
}


void CellStoreV0::record_split_row(const SerializedKey key) {
  if (key.ptr) {
    std::string split_row = key.row();
    if (split_row > m_start_row && split_row < m_end_row)
      m_split_row = split_row;
  }
}
