/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 * 
 * This file is part of Hypertable.
 * 
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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

extern "C" {
#include <stdint.h>
#include <string.h>
}

#include "AsyncComm/Serialization.h"
#include "Common/Error.h"
#include "Common/Logger.h"

#include "BlockCompressionCodec.h"
#include "BlockCompressionHeader.h"

using namespace Hypertable;

const size_t BlockCompressionHeader::LENGTH;


/**
 */
void BlockCompressionHeader::encode(uint8_t **buf_ptr) {
  uint8_t *base = *buf_ptr;
  memcpy(*buf_ptr, m_magic, 10);
  (*buf_ptr) += 10;
  Serialization::encode_short(buf_ptr, 0);
  Serialization::encode_short(buf_ptr, length());
  Serialization::encode_short(buf_ptr, m_compression_type);
  Serialization::encode_int(buf_ptr, m_data_checksum);
  Serialization::encode_int(buf_ptr, m_data_length);
  Serialization::encode_int(buf_ptr, m_data_zlength);
  if ((size_t)(*buf_ptr - base) == length()) 
    write_header_checksum(base);
}

void BlockCompressionHeader::write_header_checksum(uint8_t *base) {
  uint16_t header_checksum = 0;
  uint8_t *ptr = base;
  for (size_t i=0; i<length(); i++)
    header_checksum += *ptr++;
  ptr = base + 10;
  Serialization::encode_short(&ptr, header_checksum);
}


/**
 */
int BlockCompressionHeader::decode(uint8_t **buf_ptr, size_t *remaining_ptr) {
  uint16_t header_length, header_checksum, header_checksum_computed = 0;
  uint8_t *ptr = *buf_ptr;

  if (*remaining_ptr < length())
    return Error::BLOCK_COMPRESSOR_TRUNCATED;

  for (size_t i=0; i<length(); i++)
    header_checksum_computed += *ptr++;

  memcpy(m_magic, *buf_ptr, 10);
  (*buf_ptr) += 10;
  *remaining_ptr -= 10;

  // subtract out the stored checksum bytes
  header_checksum_computed -= (*buf_ptr)[0];
  header_checksum_computed -= (*buf_ptr)[1];

  if (!Serialization::decode_short(buf_ptr, remaining_ptr, &header_checksum))
    return Error::BLOCK_COMPRESSOR_TRUNCATED;

  if (header_checksum_computed != header_checksum)
    return Error::BLOCK_COMPRESSOR_CHECKSUM_MISMATCH;

  if (!Serialization::decode_short(buf_ptr, remaining_ptr, &header_length))
    return Error::BLOCK_COMPRESSOR_TRUNCATED;
  HT_EXPECT(header_length == length(), Error::FAILED_EXPECTATION);

  if (!Serialization::decode_short(buf_ptr, remaining_ptr, &m_compression_type))
    return Error::BLOCK_COMPRESSOR_TRUNCATED;
  HT_EXPECT(m_compression_type < BlockCompressionCodec::COMPRESSION_TYPE_LIMIT, Error::FAILED_EXPECTATION);  

  if (!Serialization::decode_int(buf_ptr, remaining_ptr, &m_data_checksum))
    return Error::BLOCK_COMPRESSOR_TRUNCATED;

  if (!Serialization::decode_int(buf_ptr, remaining_ptr, &m_data_length))
    return Error::BLOCK_COMPRESSOR_TRUNCATED;

  if (!Serialization::decode_int(buf_ptr, remaining_ptr, &m_data_zlength))
    return Error::BLOCK_COMPRESSOR_TRUNCATED;


  return Error::OK;
}

