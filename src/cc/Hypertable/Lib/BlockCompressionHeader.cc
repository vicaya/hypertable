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

#include "Common/Serialization.h"
#include "Common/Checksum.h"
#include "Common/Error.h"
#include "Common/Logger.h"

#include "BlockCompressionCodec.h"
#include "BlockCompressionHeader.h"

using namespace Hypertable;
using namespace Serialization;

const size_t BlockCompressionHeader::LENGTH;


/**
 */
void BlockCompressionHeader::encode(uint8_t **bufp) {
  uint8_t *base = *bufp;
  memcpy(*bufp, m_magic, 10);
  (*bufp) += 10;
  *(*bufp)++ = (uint8_t)length();
  *(*bufp)++ = (uint8_t)m_compression_type;
  encode_i32(bufp, m_data_checksum);
  encode_i32(bufp, m_data_length);
  encode_i32(bufp, m_data_zlength);

  if ((size_t)(*bufp - base) + 2 == length())
    write_header_checksum(base, bufp);
}

void
BlockCompressionHeader::write_header_checksum(uint8_t *base, uint8_t **bufp) {
  uint16_t checksum16 = fletcher32(base, *bufp-base);
  encode_i16(bufp, checksum16);
}


/**
 */
void BlockCompressionHeader::decode(const uint8_t **bufp, size_t *remainp) {
  const uint8_t *base = *bufp;
  uint16_t header_length;

  if (*remainp < length())
    HT_THROW(Error::BLOCK_COMPRESSOR_TRUNCATED, "");

  // verify checksum
  uint16_t header_checksum, header_checksum_computed;
  size_t remaining = 2;
  const uint8_t *ptr = *bufp + length() - 2;
  header_checksum_computed = fletcher32(*bufp, length() - 2);
  header_checksum = decode_i16(&ptr, &remaining);

  if (header_checksum_computed != header_checksum)
    HT_THROWF(Error::BLOCK_COMPRESSOR_BAD_HEADER, "Header checksum mismatch at %p: %u (computed) != %u (stored)",
              *bufp, (unsigned)header_checksum_computed, (unsigned)header_checksum);

  memcpy(m_magic, *bufp, 10);
  (*bufp) += 10;
  *remainp -= 10;

  header_length = decode_byte(bufp, remainp);

  if (header_length != length())
    HT_THROWF(Error::BLOCK_COMPRESSOR_BAD_HEADER, "Unexpected header length"
              ": %lu, expecting: %lu", (Lu)header_length, (Lu)length());

  m_compression_type = decode_byte(bufp, remainp);

  if (m_compression_type >= BlockCompressionCodec::COMPRESSION_TYPE_LIMIT)
    HT_THROWF(Error::BLOCK_COMPRESSOR_BAD_HEADER, "Bad compression type: %d",
              (int)m_compression_type);

  m_data_checksum = decode_i32(bufp, remainp);
  m_data_length = decode_i32(bufp, remainp);
  m_data_zlength = decode_i32(bufp, remainp);

  if ((size_t)(*bufp - base) == length() - 2) {
    *bufp += 2;
    *remainp -= 2;
  }
}

