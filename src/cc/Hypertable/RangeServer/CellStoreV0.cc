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

#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/System.h"

#include "AsyncComm/Protocol.h"

#include "Hypertable/Lib/BlockCompressionHeader.h"
#include "Hypertable/Lib/CompressorFactory.h"
#include "Hypertable/Lib/Key.h"

#include "CellStoreScannerV0.h"
#include "CellStoreV0.h"
#include "FileBlockCache.h"
#include "Global.h"

using namespace std;
using namespace Hypertable;

const char CellStoreV0::DATA_BLOCK_MAGIC[10]           =
    { 'D','a','t','a','-','-','-','-','-','-' };
const char CellStoreV0::INDEX_FIXED_BLOCK_MAGIC[10]    =
    { 'I','d','x','F','i','x','-','-','-','-' };
const char CellStoreV0::INDEX_VARIABLE_BLOCK_MAGIC[10] =
    { 'I','d','x','V','a','r','-','-','-','-' };

namespace {
  const uint32_t MAX_APPENDS_OUTSTANDING = 3;
}

CellStoreV0::CellStoreV0(Filesystem *filesys)
  : m_filesys(filesys), m_filename(), m_fd(-1), m_compressor(0), m_buffer(0),
    m_fix_index_buffer(0), m_var_index_buffer(0), m_memory_consumed(0),
    m_outstanding_appends(0), m_offset(0), m_last_key(0), m_file_length(0),
    m_disk_usage(0), m_file_id(0), m_uncompressed_blocksize(0) {
  m_file_id = FileBlockCache::get_next_file_id();
  assert(sizeof(float) == 4);
}



CellStoreV0::~CellStoreV0() {
  try {
    delete m_compressor;

    if (m_fd != -1)
      m_filesys->close(m_fd);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << "Error closing DFS client: "<< e << HT_END;
  }
  if (m_memory_consumed)
    Global::memory_tracker.subtract(m_memory_consumed);
}


BlockCompressionCodec *CellStoreV0::create_block_compression_codec() {
  return CompressorFactory::create_block_codec(
      (BlockCompressionCodec::Type)m_trailer.compression_type);
}



int64_t CellStoreV0::get_revision() {
  return m_trailer.revision;
}


const char *CellStoreV0::get_split_row() {
  if (m_split_row != "")
    return m_split_row.c_str();
  return 0;
}


CellListScanner *CellStoreV0::create_scanner(ScanContextPtr &scan_ctx) {
  CellStorePtr cellstore(this);
  return new CellStoreScannerV0(cellstore, scan_ctx);
}


int
CellStoreV0::create(const char *fname, uint32_t blocksize,
                    const std::string &compressor) {
  m_buffer.reserve(blocksize*4);

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

  if (compressor.empty())
    m_trailer.compression_type = CompressorFactory::parse_block_codec_spec(
        "lzo", m_compressor_args);
  else
    m_trailer.compression_type = CompressorFactory::parse_block_codec_spec(
        compressor, m_compressor_args);

  m_compressor = CompressorFactory::create_block_codec(
      (BlockCompressionCodec::Type)m_trailer.compression_type,
      m_compressor_args);

  try { m_fd = m_filesys->create(m_filename, true, -1, -1, -1); }
  catch (Exception &e) {
    HT_ERRORF("Error creating cellstore: %s", e.what());
    return e.code();
  }
  return Error::OK;
}


int CellStoreV0::add(const Key &key, const ByteString value) {
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
        HT_ERRORF("Problem writing to DFS file '%s' : %s", m_filename.c_str(),
            Hypertable::Protocol::string_format_message(event_ptr).c_str());
        return -1;
      }
      m_outstanding_appends--;
    }

    size_t zlen = zbuf.fill();
    StaticBuffer send_buf(zbuf);

    try { m_filesys->append(m_fd, send_buf, 0, &m_sync_handler); }
    catch (Exception &e) {
      HT_ERRORF("Problem writing to DFS file '%s' : %s",
                m_filename.c_str(), e.what());
      return -1;
    }
    m_outstanding_appends++;
    m_offset += zlen;
  }

  size_t value_len = value.length();

  m_buffer.ensure(key.length + value_len);

  m_last_key.ptr = m_buffer.add_unchecked(key.serial.ptr, key.length);
  m_buffer.add_unchecked(value.ptr, value_len);

  m_trailer.total_entries++;

  return 0;
}



int CellStoreV0::finalize() {
  EventPtr event_ptr;
  int error = -1;
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
      if (!m_sync_handler.wait_for_reply(event_ptr)) {
        HT_ERRORF("Problem writing to DFS file '%s' : %s", m_filename.c_str(),
                  Protocol::string_format_message(event_ptr).c_str());
        goto abort;
      }
      m_outstanding_appends--;
    }

    try { m_filesys->append(m_fd, send_buf, 0, &m_sync_handler); }
    catch (Exception &e) {
      HT_ERRORF("Problem writing to DFS file '%s' : %s", m_filename.c_str(),
                e.what());
      goto abort;
    }
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

  try { m_filesys->append(m_fd, send_buf, 0, &m_sync_handler); }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    goto abort;
  }
  m_outstanding_appends++;
  m_offset += zlen;

  /**
   * Write variable index + trailer
   */
  {
    BlockCompressionHeader header(INDEX_VARIABLE_BLOCK_MAGIC);
    m_trailer.var_index_offset = m_offset;
    m_compressor->deflate(m_var_index_buffer, zbuf, header, m_trailer.size());
  }

  /**
   * Set up m_index map
   */
  uint32_t offset;
  m_fix_index_buffer.ptr = m_fix_index_buffer.base;
  m_var_index_buffer.ptr = m_var_index_buffer.base;
  for (size_t i=0; i<m_trailer.index_entries; i++) {
    // variable portion
    key.ptr = m_var_index_buffer.ptr;
    m_var_index_buffer.ptr += key.length();
    // fixed portion (e.g. offset)
    memcpy(&offset, m_fix_index_buffer.ptr, sizeof(offset));
    m_fix_index_buffer.ptr += sizeof(offset);
    m_index.insert(m_index.end(), IndexMap::value_type(key, offset));
    if (i == m_trailer.index_entries/2) {
      record_split_row(m_var_index_buffer.ptr);
    }
  }

  // deallocate fix index data
  delete [] m_fix_index_buffer.release();

  // write filter_offset (empty for now)
  m_trailer.filter_offset = m_offset + zbuf.fill();

  //
  m_trailer.serialize(zbuf.ptr);
  zbuf.ptr += m_trailer.size();

  zlen = zbuf.fill();
  send_buf = zbuf;

  try { m_filesys->append(m_fd, send_buf); }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    goto abort;
  }
  m_outstanding_appends++;
  m_offset += zlen;

  /** close file for writing **/
  try { m_filesys->close(m_fd); }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    goto abort;
  }

  /** Set file length **/
  m_file_length = m_offset;

  /** Re-open file for reading **/
  try { m_fd = m_filesys->open(m_filename); }
  catch (Exception &e) {
    HT_ERROR_OUT << "Error reopening cellstore: "<<  e << HT_END;
    goto abort;
  }

  m_disk_usage = (uint32_t)m_file_length;
  error = 0;

  m_memory_consumed = sizeof(CellStoreV0) + m_var_index_buffer.size
      + (m_index.size() * 2 * sizeof(IndexMap::value_type));
  Global::memory_tracker.add(m_memory_consumed);

 abort:
  delete m_compressor;
  m_compressor = 0;

  return error;
}



/**
 *
 */
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



/**
 *
 */
int
CellStoreV0::open(const char *fname, const char *start_row,
                  const char *end_row) {
  m_start_row = (start_row) ? start_row : "";
  m_end_row = (end_row) ? end_row : Key::END_ROW_MARKER;

  m_fd = -1;

  m_filename = fname;

  /** Get the file length **/
  try { m_file_length = m_filesys->length(m_filename); }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    goto abort;
  }

  if (m_file_length < m_trailer.size()) {
    HT_ERRORF("Bad length of CellStore file '%s' - %llu", m_filename.c_str(),
              (Llu)m_file_length);
    goto abort;
  }

  /** Open the DFS file **/
  try { m_fd =  m_filesys->open(m_filename); }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    goto abort;
  }

  /**
   * Read and deserialize trailer
   */
  {
    uint32_t len;
    uint8_t *trailer_buf = new uint8_t [m_trailer.size()];

    try {
      len = m_filesys->pread(m_fd, trailer_buf, m_trailer.size(),
                             m_file_length - m_trailer.size());
    }
    catch (Exception &e) {
      HT_ERRORF("Problem reading trailer for CellStore '%s': %s",
                m_filename.c_str(), e.what());
      delete [] trailer_buf;
      goto abort;
    }

    if (len != m_trailer.size()) {
      HT_ERRORF("Problem reading trailer for CellStore file '%s' - only read "
                "%u of %lu bytes", m_filename.c_str(), len, m_trailer.size());
      delete [] trailer_buf;
      goto abort;
    }

    m_trailer.deserialize(trailer_buf);
    delete [] trailer_buf;
  }

  /** Sanity check trailer **/
  if (m_trailer.version != 0) {
    HT_ERRORF("Unsupported CellStore version (%d) for file '%s'",
              m_trailer.version, fname);
    goto abort;
  }
  if (!(m_trailer.fix_index_offset < m_trailer.var_index_offset &&
        m_trailer.var_index_offset < m_file_length)) {
    HT_ERRORF("Bad index offsets in CellStore trailer fix=%u, var=%u, "
              "length=%llu, file='%s'", m_trailer.fix_index_offset,
              m_trailer.var_index_offset, (Llu)m_file_length, fname);
    goto abort;
  }

  return Error::OK;

 abort:
  return Error::LOCAL_IO_ERROR;
}


int CellStoreV0::load_index() {
  int error = -1;
  uint32_t amount, index_amount;
  uint8_t *fix_end;
  uint8_t *var_end;
  uint32_t len = 0;
  BlockCompressionHeader header;
  SerializedKey key;
  bool inflating_fixed=true;
  bool second_try = false;

  m_compressor = create_block_compression_codec();

  amount = index_amount = (m_file_length - m_trailer.size())
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
      HT_THROW(Error::BLOCK_COMPRESSOR_BAD_MAGIC, "");

    /** inflate variable index **/
    DynamicBuffer vbuf(0, false);
    amount = (m_file_length-m_trailer.size()) - m_trailer.var_index_offset;
    vbuf.base = buf.ptr;
    vbuf.ptr = buf.ptr + amount;

    m_compressor->inflate(vbuf, m_var_index_buffer, header);

    if (!header.check_magic(INDEX_VARIABLE_BLOCK_MAGIC))
      HT_THROW(Error::BLOCK_COMPRESSOR_BAD_MAGIC, "");
  }
  catch (Exception &e) {
    if (inflating_fixed)
      HT_ERROR_OUT <<"Error inflating FIXED index for cellstore '"
          << m_filename <<"': "<<  e << HT_END;
    else
      HT_ERROR_OUT <<"Error inflating VARIABLE index for cellstore '"
          << m_filename <<"': "<<  e << HT_END;
    HT_ERROR_OUT << "pread(fd=" << m_fd << ", len=" << len << ", amount="
        << index_amount << ")\n" << HT_END;
    HT_ERROR_OUT << m_trailer << HT_END;
    if (second_try)
      goto abort;
    second_try = true;
    goto try_again;
  }

  m_index.clear();

  uint32_t offset;

  // record end offsets for sanity checking and reset ptr
  fix_end = m_fix_index_buffer.ptr;
  m_fix_index_buffer.ptr = m_fix_index_buffer.base;
  var_end = m_var_index_buffer.ptr;
  m_var_index_buffer.ptr = m_var_index_buffer.base;

  for (size_t i=0; i< m_trailer.index_entries; i++) {
    assert(m_fix_index_buffer.ptr < fix_end);
    assert(m_var_index_buffer.ptr < var_end);

    // Deserialized cell key (variable portion)
    key.ptr = m_var_index_buffer.ptr;
    m_var_index_buffer.ptr += key.length();

    // Deserialize offset
    memcpy(&offset, m_fix_index_buffer.ptr, sizeof(offset));
    m_fix_index_buffer.ptr += sizeof(offset);

    m_index.insert(m_index.end(), IndexMap::value_type(key, offset));
  }

  /**
   * Compute disk usage
   */
  {
    uint32_t start = 0;
    uint32_t end = (uint32_t)m_file_length;
    size_t start_row_length = m_start_row.length() + 1;
    size_t end_row_length = m_end_row.length() + 1;
    DynamicBuffer dbuf(7 + std::max(start_row_length, end_row_length));
    SerializedKey serkey;
    CellStoreV0::IndexMap::const_iterator iter, mid_iter, end_iter;

    dbuf.clear();
    create_key_and_append(dbuf, m_start_row.c_str());
    serkey.ptr = dbuf.base;
    iter = m_index.upper_bound(serkey);
    start = (*iter).second;

    dbuf.clear();
    create_key_and_append(dbuf, m_end_row.c_str());
    serkey.ptr = dbuf.base;
    if ((end_iter = m_index.lower_bound(serkey)) == m_index.end())
      end = m_file_length;
    else
      end = (*end_iter).second;

    m_disk_usage = end - start;

    size_t i=0;
    for (mid_iter=iter; iter!=end_iter; ++iter,++i) {
      if ((i%2)==0)
        ++mid_iter;
    }
    if (mid_iter != m_index.end())
      record_split_row((*mid_iter).first);
  }

  error = 0;

 abort:
  delete m_compressor;
  m_compressor = 0;
  delete [] m_fix_index_buffer.release();
  return error;
}


/**
 *
 */
void CellStoreV0::display_block_info() {
  SerializedKey last_key;
  uint32_t last_offset = 0;
  uint32_t block_size;
  size_t i=0;
  for (IndexMap::const_iterator iter = m_index.begin();
       iter != m_index.end(); ++iter) {
    if (last_key) {
      block_size = (*iter).second - last_offset;
      cout << i << ": offset=" << last_offset << " size=" << block_size
           << " row=" << last_key.row() << endl;
      i++;
    }
    last_offset = (*iter).second;
    last_key = (*iter).first;
  }
  if (last_key) {
    block_size = m_trailer.filter_offset - last_offset;
    cout << i << ": offset=" << last_offset << " size=" << block_size
         << " row=" << last_key.row() << endl;
  }
}



void CellStoreV0::record_split_row(const SerializedKey key) {
  std::string split_row = key.row();
  if (split_row > m_start_row && split_row < m_end_row)
    m_split_row = split_row;
}
