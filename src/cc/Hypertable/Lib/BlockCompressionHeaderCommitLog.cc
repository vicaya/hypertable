/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#include "Common/Error.h"

#include "AsyncComm/Serialization.h"

#include "BlockCompressionHeaderCommitLog.h"

using namespace Hypertable;

BlockCompressionHeaderCommitLog::BlockCompressionHeaderCommitLog() {
  m_length = 0;
  m_zlength = 0;
  m_type = 0;
  m_checksum = 0;
  memset(m_magic, 0, 10);
  m_timestamp = 0;
  m_tablename = 0;
  m_header_length = 0;
}


BlockCompressionHeaderCommitLog::BlockCompressionHeaderCommitLog(const char magic[10], uint64_t timestamp, const char *tablename) {
  set_magic(magic);
  m_timestamp = timestamp;
  m_tablename = tablename;
  m_length = 0;
  m_zlength = 0;
  m_type = 0;
  m_checksum = 0;
}


/**
 *
 */
void BlockCompressionHeaderCommitLog::encode(uint8_t **buf_ptr) {
  m_header_length = encoded_length();
  memcpy(*buf_ptr, m_magic, 10);
  (*buf_ptr) += 10;
  Serialization::encode_short(buf_ptr, m_header_length);
  Serialization::encode_int(buf_ptr, m_length);
  Serialization::encode_int(buf_ptr, m_zlength);
  Serialization::encode_short(buf_ptr, m_type);
  Serialization::encode_int(buf_ptr, m_checksum);
  Serialization::encode_long(buf_ptr, m_timestamp);
  if (m_tablename) {
    strcpy((char *)*buf_ptr, m_tablename);
    (*buf_ptr) += strlen(m_tablename);
  }
  *(*buf_ptr)++ = 0;
}


/**
 *
 */
int BlockCompressionHeaderCommitLog::decode_fixed(uint8_t **buf_ptr, size_t *remaining_ptr) {

  if (*remaining_ptr < 10)
    return Error::BLOCK_COMPRESSOR_TRUNCATED;
  
  memcpy(m_magic, *buf_ptr, 10);
  (*buf_ptr) += 10;
  *remaining_ptr -= 10;

  if (!Serialization::decode_short(buf_ptr, remaining_ptr, &m_header_length))
    return Error::BLOCK_COMPRESSOR_TRUNCATED;

  if (!Serialization::decode_int(buf_ptr, remaining_ptr, &m_length))
    return Error::BLOCK_COMPRESSOR_TRUNCATED;

  if (!Serialization::decode_int(buf_ptr, remaining_ptr, &m_zlength))
    return Error::BLOCK_COMPRESSOR_TRUNCATED;

  if (!Serialization::decode_short(buf_ptr, remaining_ptr, &m_type))
    return Error::BLOCK_COMPRESSOR_TRUNCATED;

  if (!Serialization::decode_int(buf_ptr, remaining_ptr, &m_checksum))
    return Error::BLOCK_COMPRESSOR_TRUNCATED;

  if (!Serialization::decode_long(buf_ptr, remaining_ptr, &m_timestamp))
    return Error::BLOCK_COMPRESSOR_TRUNCATED;    

  return Error::OK;
}


/**
 *
 */
int BlockCompressionHeaderCommitLog::decode_variable(uint8_t **buf_ptr, size_t *remaining_ptr) {
  size_t remaining_header;

  if (m_header_length <= fixed_length())
    return Error::BLOCK_COMPRESSOR_TRUNCATED;

  remaining_header = m_header_length - fixed_length();

  if (*remaining_ptr < remaining_header)
    return Error::BLOCK_COMPRESSOR_TRUNCATED;
    
  m_tablename = (const char *)*buf_ptr;

  if (m_tablename[remaining_header-1] != 0)
    return Error::BLOCK_COMPRESSOR_BAD_HEADER;

  (*buf_ptr) += remaining_header;
  (*remaining_ptr) -= remaining_header;

  return Error::OK;
}


