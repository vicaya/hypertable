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

#include "Common/Error.h"

#include "AsyncComm/Serialization.h"

#include "BlockCompressionHeaderCellStore.h"

using namespace Hypertable;

BlockCompressionHeaderCellStore::BlockCompressionHeaderCellStore() {
  m_length = 0;
  m_zlength = 0;
  m_type = 0;
  m_checksum = 0;
  memset(m_magic, 0, 10);
}

/**
 *
 */
void BlockCompressionHeaderCellStore::encode(uint8_t **buf_ptr) {
  memcpy(*buf_ptr, m_magic, 10);
  (*buf_ptr) += 10;
  Serialization::encode_short(buf_ptr, encoded_length());
  Serialization::encode_int(buf_ptr, m_length);
  Serialization::encode_int(buf_ptr, m_zlength);
  Serialization::encode_short(buf_ptr, m_type);
  Serialization::encode_int(buf_ptr, m_checksum);
}


/**
 *
 */
int BlockCompressionHeaderCellStore::decode_fixed(uint8_t **buf_ptr, size_t *remaining_ptr) {
  uint16_t header_len;

  if (*remaining_ptr < encoded_length())
    return Error::BLOCK_COMPRESSOR_TRUNCATED;

  memcpy(m_magic, *buf_ptr, 10);
  (*buf_ptr) += 10;
  *remaining_ptr -= 10;

  Serialization::decode_short(buf_ptr, remaining_ptr, &header_len);
  Serialization::decode_int(buf_ptr, remaining_ptr, &m_length);
  Serialization::decode_int(buf_ptr, remaining_ptr, &m_zlength);
  Serialization::decode_short(buf_ptr, remaining_ptr, &m_type);
  Serialization::decode_int(buf_ptr, remaining_ptr, &m_checksum);

  return Error::OK;
}


/**
 *
 */
int BlockCompressionHeaderCellStore::decode_variable(uint8_t **buf_ptr, size_t *remaining_ptr) {
  return Error::OK;
}
