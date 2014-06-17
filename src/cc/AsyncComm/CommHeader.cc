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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */
#include "Common/Compat.h"
#include "Common/Checksum.h"
#include "Common/Error.h"
#include "Common/Serialization.h"
#include "Common/Logger.h"

#include "CommHeader.h"

using namespace Hypertable;

void CommHeader::encode(uint8_t **bufp) {
  uint8_t *base = *bufp;
  Serialization::encode_i8(bufp, version);
  Serialization::encode_i8(bufp, header_len);
  Serialization::encode_i16(bufp, alignment);
  Serialization::encode_i16(bufp, flags);
  Serialization::encode_i32(bufp, 0);
  Serialization::encode_i32(bufp, id);
  Serialization::encode_i32(bufp, gid);
  Serialization::encode_i32(bufp, total_len);
  Serialization::encode_i32(bufp, timeout_ms);
  Serialization::encode_i32(bufp, payload_checksum);
  Serialization::encode_i64(bufp, command);
  // compute and serialize header checksum
  header_checksum = fletcher32(base, (*bufp)-base);
  base += 6;
  Serialization::encode_i32(&base, header_checksum);
}

void CommHeader::decode(const uint8_t **bufp, size_t *remainp) {
  const uint8_t *base = *bufp;
  if (*remainp < FIXED_LENGTH)
    HT_THROWF(Error::COMM_BAD_HEADER,
              "Header size %d is less than the minumum fixed length %d",
              (int)*remainp, (int)FIXED_LENGTH);
  HT_TRY("decoding comm header",
         version = Serialization::decode_i8(bufp, remainp);
         header_len = Serialization::decode_i8(bufp, remainp);
         alignment = Serialization::decode_i16(bufp, remainp);
         flags = Serialization::decode_i16(bufp, remainp);
         header_checksum = Serialization::decode_i32(bufp, remainp);
         id = Serialization::decode_i32(bufp, remainp);
         gid = Serialization::decode_i32(bufp, remainp);
         total_len = Serialization::decode_i32(bufp, remainp);
         timeout_ms = Serialization::decode_i32(bufp, remainp);
         payload_checksum = Serialization::decode_i32(bufp, remainp);
         command = Serialization::decode_i64(bufp, remainp));
  memset((void *)(base+6), 0, 4);
  uint32_t checksum = fletcher32(base, *bufp-base);
  if (checksum != header_checksum)
    HT_THROWF(Error::COMM_HEADER_CHECKSUM_MISMATCH, "%u != %u", checksum,
              header_checksum);
}
