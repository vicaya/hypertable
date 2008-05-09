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

#include "Common/Serialization.h"
#include "Common/Checksum.h"
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
  *(*buf_ptr)++ = (uint8_t)length();
  *(*buf_ptr)++ = (uint8_t)m_compression_type;
  Serialization::encode_int(buf_ptr, m_data_checksum);
  Serialization::encode_int(buf_ptr, m_data_length);
  Serialization::encode_int(buf_ptr, m_data_zlength);
  if ((size_t)(*buf_ptr - base) + 2 == length())
    write_header_checksum(base, buf_ptr);
}

void BlockCompressionHeader::write_header_checksum(uint8_t *base, uint8_t **buf_ptr) {
  uint16_t checksum16 = (uint16_t)(fletcher32(base, *buf_ptr-base) >> 16);
  Serialization::encode_short(buf_ptr, checksum16);
}


/**
 */
int BlockCompressionHeader::decode(uint8_t **buf_ptr, size_t *remaining_ptr) {
  uint8_t *base = *buf_ptr;
  uint16_t header_length;
  uint8_t bval = 0;

  if (*remaining_ptr < length())
    return Error::BLOCK_COMPRESSOR_TRUNCATED;

  // verify checksum
  {
    uint16_t header_checksum, header_checksum_computed;
    size_t remaining = 2;
    uint8_t *ptr = *buf_ptr + length() - 2;
    header_checksum_computed = (uint16_t)(fletcher32(*buf_ptr, length()-2) >> 16);
    Serialization::decode_short(&ptr, &remaining, &header_checksum);
    if (header_checksum_computed != header_checksum)
      return Error::BLOCK_COMPRESSOR_BAD_HEADER;
  }

  memcpy(m_magic, *buf_ptr, 10);
  (*buf_ptr) += 10;
  *remaining_ptr -= 10;

  Serialization::decode_byte(buf_ptr, remaining_ptr, &bval);
  header_length = bval;
  HT_EXPECT(header_length == length(), Error::FAILED_EXPECTATION);

  Serialization::decode_byte(buf_ptr, remaining_ptr, &bval);
  m_compression_type = bval;
  HT_EXPECT(m_compression_type < BlockCompressionCodec::COMPRESSION_TYPE_LIMIT, Error::FAILED_EXPECTATION);  

  Serialization::decode_int(buf_ptr, remaining_ptr, &m_data_checksum);
  Serialization::decode_int(buf_ptr, remaining_ptr, &m_data_length);
  Serialization::decode_int(buf_ptr, remaining_ptr, &m_data_zlength);

  if ((size_t)(*buf_ptr - base) == length() - 2) {
    *buf_ptr += 2;
    *remaining_ptr -= 2;
  }

  return Error::OK;
}

