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

#include <boost/scoped_ptr.hpp>

extern "C" {
#include <string.h>
}

#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/System.h"

#include "AsyncComm/Protocol.h"

#include "Hypertable/Lib/Key.h"

#include "BlockDeflaterZlib.h"
#include "BlockInflaterZlib.h"
#include "CellStoreScannerV0.h"
#include "CellStoreV0.h"
#include "Constants.h"
#include "FileBlockCache.h"

using namespace Hypertable;

CellStoreV0::CellStoreV0(Filesystem *filesys) : m_filesys(filesys), m_filename(), m_fd(-1), m_index(),
  m_buffer(0), m_fix_index_buffer(0), m_var_index_buffer(0), m_block_size(Constants::DEFAULT_BLOCKSIZE),
  m_outstanding_appends(0), m_offset(0), m_last_key(0), m_file_length(0), m_disk_usage(0), m_file_id(0) {
  m_block_deflater = new BlockDeflaterZlib();
  m_file_id = FileBlockCache::get_next_file_id();
}



CellStoreV0::~CellStoreV0() {
  int error;
  delete m_block_deflater;
  if (m_fd != -1) {
    if ((error = m_filesys->close(m_fd)) != Error::OK) {
      LOG_VA_ERROR("Problem closing HDFS client - %s", Error::get_text(error));
    }
  }
  //cout << "DELETE CellStoreV0" << endl;
}



uint64_t CellStoreV0::get_log_cutoff_time() {
  return m_trailer.timestamp;
}



uint16_t CellStoreV0::get_flags() {
  return m_trailer.flags;
}

const char *CellStoreV0::get_split_row() {
  if (m_split_row != "")
    return m_split_row.c_str();
  return 0;
}


CellListScanner *CellStoreV0::create_scanner(ScanContextPtr &scanContextPtr) {
  CellStorePtr cellStorePtr(this);
  return new CellStoreScannerV0(cellStorePtr, scanContextPtr);
}


int CellStoreV0::create(const char *fname, size_t blockSize) {

  m_block_size = blockSize;
  m_buffer.reserve(m_block_size*2);

  m_fd = -1;
  m_offset = 0;
  m_last_key = 0;
  m_fix_index_buffer.reserve(m_block_size);
  m_var_index_buffer.reserve(m_block_size);

  memset(&m_trailer, 0, sizeof(m_trailer));

  m_filename = fname;

  m_start_row = "";
  m_end_row = Key::END_ROW_MARKER;

  return m_filesys->create(m_filename, true, -1, -1, -1, &m_fd);

}


int CellStoreV0::add(const ByteString32T *key, const ByteString32T *value) {
  int error;
  EventPtr eventPtr;
  DynamicBuffer zBuffer(0);

  if (m_buffer.fill() > m_block_size) {

    add_index_entry(m_last_key, m_offset);

    m_block_deflater->deflate(m_buffer, zBuffer, Constants::DATA_BLOCK_MAGIC);
    m_buffer.clear();

    if (m_outstanding_appends > 0) {
      if (!m_sync_handler.wait_for_reply(eventPtr)) {
	LOG_VA_ERROR("Problem writing to HDFS file '%s' : %s", m_filename.c_str(), Hypertable::Protocol::string_format_message(eventPtr).c_str());
	return -1;
      }
      m_outstanding_appends--;
    }

    size_t  zlen;
    uint8_t *zbuf = zBuffer.release(&zlen);

    if ((error = m_filesys->append(m_fd, zbuf, zlen, &m_sync_handler)) != Error::OK) {
      LOG_VA_ERROR("Problem writing to HDFS file '%s' : %s", m_filename.c_str(), Error::get_text(error));
      return -1;
    }
    m_outstanding_appends++;
    m_offset += zlen;
  }

  size_t keyLen = sizeof(int32_t) + key->len;
  size_t valueLen = sizeof(int32_t) + value->len;

  m_buffer.ensure( keyLen + valueLen );

  memcpy(m_buffer.ptr, key, keyLen);
  m_last_key = (ByteString32T *)m_buffer.ptr;
  m_buffer.ptr += keyLen;

  memcpy(m_buffer.ptr, value, valueLen);
  m_buffer.ptr += valueLen;

  return 0;
}



int CellStoreV0::finalize(uint64_t timestamp) {
  EventPtr eventPtr;
  int error = -1;
  uint8_t *zbuf;
  size_t zlen;
  DynamicBuffer zBuffer(0);
  size_t len;
  uint8_t *base;

  if (m_buffer.fill() > 0) {

    add_index_entry(m_last_key, m_offset);

    m_block_deflater->deflate(m_buffer, zBuffer, Constants::DATA_BLOCK_MAGIC);
    zbuf = zBuffer.release(&zlen);

    if (m_outstanding_appends > 0) {
      if (!m_sync_handler.wait_for_reply(eventPtr)) {
	LOG_VA_ERROR("Problem writing to HDFS file '%s' : %s", m_filename.c_str(), Protocol::string_format_message(eventPtr).c_str());
	goto abort;
      }
      m_outstanding_appends--;
    }

    if ((error = m_filesys->append(m_fd, zbuf, zlen, &m_sync_handler)) != Error::OK) {
      LOG_VA_ERROR("Problem writing to HDFS file '%s' : %s", m_filename.c_str(), Protocol::string_format_message(eventPtr).c_str());
      goto abort;
    }
    m_outstanding_appends++;
    m_offset += zlen;
  }

  m_trailer.fixIndexOffset = m_offset;
  m_trailer.timestamp = timestamp;
  m_trailer.flags = 0;
  m_trailer.compressionType = Constants::COMPRESSION_TYPE_ZLIB;
  m_trailer.version = 1;

  /**
   * Chop the Index buffers down to the exact length
   */
  base = m_fix_index_buffer.release(&len);
  m_fix_index_buffer.reserve(len);
  m_fix_index_buffer.addNoCheck(base, len);
  delete [] base;
  base = m_var_index_buffer.release(&len);
  m_var_index_buffer.reserve(len);
  m_var_index_buffer.addNoCheck(base, len);
  delete [] base;

  /**
   * Write fixed index
   */
  m_block_deflater->deflate(m_fix_index_buffer, zBuffer, Constants::INDEX_FIXED_BLOCK_MAGIC);
  zbuf = zBuffer.release(&zlen);

  /**
   * wait for last Client op
   */
  if (m_outstanding_appends > 0) {
    if (!m_sync_handler.wait_for_reply(eventPtr)) {
      LOG_VA_ERROR("Problem writing to HDFS file '%s' : %s", m_filename.c_str(), Protocol::string_format_message(eventPtr).c_str());
      goto abort;
    }
    m_outstanding_appends--;
  }

  if (m_filesys->append(m_fd, zbuf, zlen, &m_sync_handler) != Error::OK)
    goto abort;
  m_outstanding_appends++;
  m_offset += zlen;

  /**
   * Write variable index + trailer
   */
  m_trailer.varIndexOffset = m_offset;
  m_block_deflater->deflate(m_var_index_buffer, zBuffer, Constants::INDEX_VARIABLE_BLOCK_MAGIC, sizeof(m_trailer));

  // wait for fixed index write
  if (m_outstanding_appends > 0) {
    if (!m_sync_handler.wait_for_reply(eventPtr)) {
      LOG_VA_ERROR("Problem writing fixed index to HDFS file '%s' : %s", m_filename.c_str(), Protocol::string_format_message(eventPtr).c_str());
      goto abort;
    }
    m_outstanding_appends--;
  }

  /**
   * Set up m_index map
   */
  uint32_t offset;
  ByteString32T *key;
  m_fix_index_buffer.ptr = m_fix_index_buffer.buf;
  m_var_index_buffer.ptr = m_var_index_buffer.buf;
  for (size_t i=0; i<m_trailer.indexEntries; i++) {
    // variable portion
    key = (ByteString32T *)m_var_index_buffer.ptr;
    m_var_index_buffer.ptr += sizeof(int32_t) + key->len;
    // fixed portion (e.g. offset)
    memcpy(&offset, m_fix_index_buffer.ptr, sizeof(offset));
    m_fix_index_buffer.ptr += sizeof(offset);
    m_index.insert(m_index.end(), IndexMapT::value_type(key, offset));
    if (i == m_trailer.indexEntries/2) {
      record_split_row((ByteString32T *)m_var_index_buffer.ptr);
    }
  }

  // deallocate fix index data
  delete [] m_fix_index_buffer.release();

  zBuffer.add(&m_trailer, sizeof(m_trailer));

  zbuf = zBuffer.release(&zlen);

  if (m_filesys->append(m_fd, zbuf, zlen) != Error::OK)
    goto abort;
  m_outstanding_appends++;
  m_offset += zlen;

  /** close file for writing **/
  if (m_filesys->close(m_fd) != Error::OK)
    goto abort;

  /** Set file length **/
  m_file_length = m_offset;

  /** Re-open file for reading **/
  if ((error = m_filesys->open(m_filename, &m_fd)) != Error::OK)
    goto abort;

  m_disk_usage = (uint32_t)m_file_length;

  error = 0;

 abort:
  return error;
}



/**
 *
 */
void CellStoreV0::add_index_entry(const ByteString32T *key, uint32_t offset) {

  size_t keyLen = sizeof(int32_t) + key->len;
  m_var_index_buffer.ensure( keyLen );
  memcpy(m_var_index_buffer.ptr, key, keyLen);
  m_var_index_buffer.ptr += keyLen;

  // Serialize offset into fix index buffer
  m_fix_index_buffer.ensure(sizeof(offset));
  memcpy(m_fix_index_buffer.ptr, &offset, sizeof(offset));
  m_fix_index_buffer.ptr += sizeof(offset);

  m_trailer.indexEntries++;
}



/**
 *
 */
int CellStoreV0::open(const char *fname, const char *start_row, const char *end_row) {
  int error = 0;

  m_start_row = (start_row) ? start_row : "";
  m_end_row = (end_row) ? end_row : Key::END_ROW_MARKER;

  m_fd = -1;

  m_filename = fname;

  /** Get the file length **/
  if ((error = m_filesys->length(m_filename, (int64_t *)&m_file_length)) != Error::OK)
    goto abort;

  if (m_file_length < sizeof(StoreTrailerT)) {
    LOG_VA_ERROR("Bad length of CellStore file '%s' - %lld", m_filename.c_str(), m_file_length);
    goto abort;
  }

  /** Open the HDFS file **/
  if ((error = m_filesys->open(m_filename, &m_fd)) != Error::OK)
    goto abort;

  /** Read trailer **/
  uint32_t len;
  if ((error = m_filesys->pread(m_fd, m_file_length-sizeof(StoreTrailerT), sizeof(StoreTrailerT), (uint8_t *)&m_trailer, &len)) != Error::OK)
    goto abort;

  if (len != sizeof(StoreTrailerT)) {
    LOG_VA_ERROR("Problem reading trailer for CellStore file '%s' - only read %d of %d bytes", m_filename.c_str(), len, sizeof(StoreTrailerT));
    goto abort;
  }

  /** Sanity check trailer **/
  if (m_trailer.version != 1) {
    LOG_VA_ERROR("Unsupported CellStore version (%d) for file '%s'", m_trailer.version, fname);
    goto abort;
  }
  if (m_trailer.compressionType != Constants::COMPRESSION_TYPE_ZLIB) {
    LOG_VA_ERROR("Unsupported CellStore compression type (%d) for file '%s'", m_trailer.compressionType, fname);
    goto abort;
  }
  if (!(m_trailer.fixIndexOffset < m_trailer.varIndexOffset &&
	m_trailer.varIndexOffset < m_file_length)) {
    LOG_VA_ERROR("Bad index offsets in CellStore trailer fix=%lld, var=%lld, length=%lld, file='%s'",
		 m_trailer.fixIndexOffset, m_trailer.varIndexOffset, m_file_length, fname);
    goto abort;
  }

#if 0
  cout << "m_index.size() = " << m_index.size() << endl;
  cout << "Fixed Index Offset: " << m_trailer.fixIndexOffset << endl;
  cout << "Variable Index Offset: " << m_trailer.varIndexOffset << endl;
  cout << "Replaced Files Offset: " << m_trailer.replacedFilesOffset << endl;
  cout << "Replaced Files Count: " << m_trailer.replacedFilesCount << endl;
  cout << "Number of Index Entries: " << m_trailer.indexEntries << endl;
  cout << "Flags: " << m_trailer.flags << endl;
  cout << "Compression Type: " << m_trailer.compressionType << endl;
  cout << "Version: " << m_trailer.version << endl;
  for (size_t i=0; i<m_replaced_files.size(); i++)
    cout << "Replaced File: '" << m_replaced_files[i] << "'" << endl;
#endif

  return Error::OK;

 abort:
  return Error::LOCAL_IO_ERROR;
}


int CellStoreV0::load_index() {
  int error = -1;
  uint8_t *buf = 0;
  uint32_t amount;
  uint8_t *vbuf = 0;
  uint8_t *fEnd;
  uint8_t *vEnd;
  uint32_t len;
  BlockInflaterZlib *inflater = new BlockInflaterZlib();

  amount = (m_file_length-sizeof(StoreTrailerT)) - m_trailer.fixIndexOffset;
  buf = new uint8_t [ amount ];

  /** Read index data **/
  if ((error = m_filesys->pread(m_fd, m_trailer.fixIndexOffset, amount, buf, &len)) != Error::OK)
    goto abort;

  if (len != amount) {
    LOG_VA_ERROR("Problem loading index for CellStore '%s' : tried to read %d but only got %d", m_filename.c_str(), amount, len);
    goto abort;
  }

  /** inflate fixed index **/
  if (!inflater->inflate(buf, m_trailer.varIndexOffset-m_trailer.fixIndexOffset, Constants::INDEX_FIXED_BLOCK_MAGIC, m_fix_index_buffer))
    goto abort;

  vbuf = buf + (m_trailer.varIndexOffset-m_trailer.fixIndexOffset);
  amount = (m_file_length-sizeof(StoreTrailerT)) - m_trailer.varIndexOffset;

  /** inflate variable index **/
  if (!inflater->inflate(vbuf, amount, Constants::INDEX_VARIABLE_BLOCK_MAGIC, m_var_index_buffer))
    goto abort;

  m_index.clear();

  ByteString32T *key;
  uint32_t offset;

  // record end offsets for sanity checking and reset ptr
  fEnd = m_fix_index_buffer.ptr;
  m_fix_index_buffer.ptr = m_fix_index_buffer.buf;
  vEnd = m_var_index_buffer.ptr;
  m_var_index_buffer.ptr = m_var_index_buffer.buf;

  for (size_t i=0; i< m_trailer.indexEntries; i++) {

    assert(m_fix_index_buffer.ptr < fEnd);
    assert(m_var_index_buffer.ptr < vEnd);

    // Deserialized cell key (variable portion)
    key = (ByteString32T *)m_var_index_buffer.ptr;
    m_var_index_buffer.ptr += sizeof(int32_t) + key->len;

    // Deserialize offset
    memcpy(&offset, m_fix_index_buffer.ptr, sizeof(offset));
    m_fix_index_buffer.ptr += sizeof(offset);

    m_index.insert(m_index.end(), IndexMapT::value_type(key, offset));

  }

  /**
   * Compute disk usage
   */
  {
    uint32_t start = 0;
    uint32_t end = (uint32_t)m_file_length;
    ByteString32T *start_key = Create(m_start_row.c_str(), strlen(m_start_row.c_str()));
    ByteString32T *end_key = Create(m_end_row.c_str(), strlen(m_end_row.c_str()));
    CellStoreV0::IndexMapT::const_iterator iter, mid_iter, end_iter;

    iter = m_index.upper_bound(start_key);
    start = (*iter).second;

    if ((end_iter = m_index.lower_bound(end_key)) == m_index.end())
      end = m_file_length;
    else
      end = (*iter).second;

    m_disk_usage = end - start;

    Destroy(start_key);
    Destroy(end_key);

    size_t i=0;
    for (mid_iter=iter; iter!=end_iter; ++iter,++i) {
      if ((i%2)==0)
	++mid_iter;
    }
    if (mid_iter != m_index.end())
      record_split_row((*mid_iter).first);
  }

#if 0
  cout << "m_index.size() = " << m_index.size() << endl;
  cout << "Fixed Index Offset: " << m_trailer.fixIndexOffset << endl;
  cout << "Variable Index Offset: " << m_trailer.varIndexOffset << endl;
  cout << "Number of Index Entries: " << m_trailer.indexEntries << endl;
  cout << "Flags: " << m_trailer.flags << endl;
  cout << "Compression Type: " << m_trailer.compressionType << endl;
  cout << "Version: " << m_trailer.version << endl;
#endif

  error = 0;

 abort:
  delete inflater;
  delete [] m_fix_index_buffer.release();
  delete [] buf;
  return error;
}


/**
 *
 */
void CellStoreV0::display_block_info() {
  ByteString32T *last_key = 0;
  uint32_t last_offset = 0;
  uint32_t block_size;
  size_t i=0;
  for (IndexMapT::const_iterator iter = m_index.begin(); iter != m_index.end(); iter++) {
    if (last_key) {
      block_size = (*iter).second - last_offset;
      cout << i << ": size=" << block_size << " row=" << (const char *)last_key->data << endl;
      i++;
    }
    last_offset = (*iter).second;
    last_key = (*iter).first;
  }
  if (last_key) {
    block_size = m_trailer.fixIndexOffset - last_offset;
    cout << i << ": size=" << block_size << " row=" << (const char *)last_key->data << endl;
  }
}


void CellStoreV0::record_split_row(const ByteString32T *key) {
  m_split_row = (const char *)key->data;
  //cout << "record_split_row = " << m_split_row << endl;
}
