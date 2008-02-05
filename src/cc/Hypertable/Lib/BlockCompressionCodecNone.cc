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

#include "Common/Checksum.h"
#include "Common/DynamicBuffer.h"
#include "Common/Error.h"
#include "Common/Logger.h"

#include "BlockCompressionCodecNone.h"

using namespace Hypertable;


/**
 *
 */
BlockCompressionCodecNone::BlockCompressionCodecNone(const Args &) {
}


/**
 * 
 */
int
BlockCompressionCodecNone::deflate(const DynamicBuffer &input, DynamicBuffer &output, BlockCompressionHeader &header, size_t reserve) {
  output.clear();
  output.reserve( header.encoded_length() + input.fill() + reserve );

  header.set_type(NONE);
  memcpy(output.buf+header.encoded_length(), input.buf, input.fill());
  header.set_length(input.fill());
  header.set_zlength(input.fill());
  header.set_checksum(fletcher32(output.buf + header.encoded_length(), header.get_zlength()));
  
  output.ptr = output.buf;
  header.encode(&output.ptr);
  output.ptr += header.get_zlength();

  return Error::OK;
}


/**
 * 
 */
int
BlockCompressionCodecNone::inflate(const DynamicBuffer &input, DynamicBuffer &output, BlockCompressionHeader &header) {

  int error;
  uint8_t *msg_ptr = input.buf;
  size_t remaining = input.fill();

  if ((error = header.decode_fixed(&msg_ptr, &remaining)) != Error::OK)
    return error;

  if ((error = header.decode_variable(&msg_ptr, &remaining)) != Error::OK)
    return error;

  if (header.get_zlength() != remaining) {
    HT_ERRORF("Block decompression error, header zlength = %d, actual = %d", header.get_zlength(), remaining);
    return Error::BLOCK_COMPRESSOR_BAD_HEADER;
  }

  uint32_t checksum = fletcher32(msg_ptr, remaining);
  if (checksum != header.get_checksum()) {
    HT_ERRORF("Compressed block checksum mismatch header=%d, computed=%d", header.get_checksum(), checksum);
    return Error::BLOCK_COMPRESSOR_CHECKSUM_MISMATCH;
  }

  output.reserve(header.get_length());

  memcpy(output.buf, msg_ptr, header.get_length());
  output.ptr = output.buf + header.get_length();

  return Error::OK;
}
