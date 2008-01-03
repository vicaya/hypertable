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
  m_flags = 0;
  m_checksum = 0;
  memset(m_magic, 0, 12);
  m_timestamp = 0;
  m_tablename = 0;
}


BlockCompressionHeaderCommitLog::BlockCompressionHeaderCommitLog(const char magic[12], uint64_t timestamp, const char *tablename) {
  set_magic(magic);
  m_timestamp = timestamp;
  m_tablename = tablename;
}


/**
 *
 */
void BlockCompressionHeaderCommitLog::encode(uint8_t **buf_ptr) {
  memcpy(*buf_ptr, m_magic, 12);
  (*buf_ptr) += 12;
  Serialization::encode_int(buf_ptr, m_length);
  Serialization::encode_int(buf_ptr, m_zlength);
  Serialization::encode_short(buf_ptr, m_flags);
  Serialization::encode_short(buf_ptr, m_checksum);
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
int BlockCompressionHeaderCommitLog::decode(uint8_t **buf_ptr, size_t *remaining_ptr) {

  if (*remaining_ptr < 12)
    return Error::BLOCK_COMPRESSOR_TRUNCATED;
  
  memcpy(m_magic, *buf_ptr, 12);
  (*buf_ptr) += 12;
  *remaining_ptr -= 12;

  if (!Serialization::decode_int(buf_ptr, remaining_ptr, &m_length))
    return Error::BLOCK_COMPRESSOR_TRUNCATED;

  if (!Serialization::decode_int(buf_ptr, remaining_ptr, &m_zlength))
    return Error::BLOCK_COMPRESSOR_TRUNCATED;

  if (!Serialization::decode_short(buf_ptr, remaining_ptr, &m_flags))
    return Error::BLOCK_COMPRESSOR_TRUNCATED;

  if (!Serialization::decode_short(buf_ptr, remaining_ptr, &m_checksum))
    return Error::BLOCK_COMPRESSOR_TRUNCATED;

  if (!Serialization::decode_long(buf_ptr, remaining_ptr, &m_timestamp))
    return Error::BLOCK_COMPRESSOR_TRUNCATED;    

  m_tablename = (const char *)*buf_ptr;
  for (; *(*buf_ptr) && *remaining_ptr > 0; ++(*buf_ptr),--(*remaining_ptr))
    ;

  if (*(*buf_ptr) != 0)
    return Error::BLOCK_COMPRESSOR_TRUNCATED;

  ++(*buf_ptr);
  --(*remaining_ptr);
  return Error::OK;
}
