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

#include "Common/DynamicBuffer.h"
#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/Checksum.h"

#include "lzo/minilzo.h"
#include "BlockCompressionCodecLzo.h"

using namespace Hypertable;


/**
 *
 */
BlockCompressionCodecLzo::BlockCompressionCodecLzo(const Args &args) {
  if (lzo_init() != LZO_E_OK)
    throw Exception(Error::BLOCK_COMPRESSOR_INIT_ERROR, "Problem initializing lzo library");
  m_workmem = new uint8_t [ LZO1X_1_MEM_COMPRESS ];
}


/**
 *
 */
BlockCompressionCodecLzo::~BlockCompressionCodecLzo() {
  HT_ASSERT_SAME_THREAD(m_creator_thread);
  delete [] m_workmem;
}



/**
 * 
 */
int BlockCompressionCodecLzo::deflate(const DynamicBuffer &input, DynamicBuffer &output, BlockCompressionHeader &header, size_t reserve) {
  uint32_t avail_out = (input.fill() + input.fill() / 16 + 64 + 3);
  int ret;
  lzo_uint out_len = avail_out;

  output.clear();
  output.reserve( header.encoded_length() + avail_out + reserve );

  ret = lzo1x_1_compress(input.buf, input.fill(), output.buf+header.encoded_length(), &out_len, m_workmem);
  assert(ret == LZO_E_OK);

  /* check for an incompressible block */
  if (out_len >= input.fill()) {
    header.set_type(NONE);
    memcpy(output.buf+header.encoded_length(), input.buf, input.fill());
    header.set_length(input.fill());
    header.set_zlength(input.fill());
  }
  else {
    header.set_type(LZO);
    header.set_length(input.fill());
    header.set_zlength(out_len);
  }
  header.set_checksum(fletcher32(output.buf + header.encoded_length(), header.get_zlength()) );
  
  output.ptr = output.buf;
  header.encode(&output.ptr);
  output.ptr += header.get_zlength();

  return Error::OK;
}


/**
 * 
 */
int BlockCompressionCodecLzo::inflate(const DynamicBuffer &input, DynamicBuffer &output, BlockCompressionHeader &header) {
  int ret;
  int error;
  uint8_t *msg_ptr = input.buf;
  size_t remaining = input.fill();
  lzo_uint new_len;

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

   // check compress bit
  if (header.get_type() == NONE)
    memcpy(output.buf, msg_ptr, header.get_length());
  else {
    new_len = header.get_length();
    ret = lzo1x_decompress(msg_ptr, header.get_zlength(), output.buf, &new_len, 0);
    if (ret != LZO_E_OK || new_len != header.get_length()) {
      HT_ERRORF("Lzo decompression error, rval = %d", ret);
      return Error::BLOCK_COMPRESSOR_INFLATE_ERROR;
    }
  }
  output.ptr = output.buf + header.get_length();

  return Error::OK;
}
