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

#include "Common/Checksum.h"
#include "Common/Thread.h"
#include "Common/DynamicBuffer.h"
#include "Common/Error.h"
#include "Common/Logger.h"

#include "BlockCompressionCodecQuicklz.h"

#include "quicklz.c"

using namespace Hypertable;

/**
 *
 */
BlockCompressionCodecQuicklz::BlockCompressionCodecQuicklz(const Args &args) {
  size_t amount = ((SCRATCH_DECOMPRESS) < (SCRATCH_COMPRESS)) ? (SCRATCH_COMPRESS) : (SCRATCH_DECOMPRESS);
  m_workmem = new uint8_t [ amount ];
}


/**
 *
 */
BlockCompressionCodecQuicklz::~BlockCompressionCodecQuicklz() {
  HT_ASSERT_SAME_THREAD(m_creator_thread);
  delete [] m_workmem;
}



/**
 * 
 */
int BlockCompressionCodecQuicklz::deflate(const DynamicBuffer &input, DynamicBuffer &output, BlockCompressionHeader &header, size_t reserve) {
  uint32_t avail_out = input.fill() + 400;
  size_t len;

  output.clear();
  output.reserve( header.encoded_length() + avail_out + reserve );

  // compress 
  len = qlz_compress((char *)input.buf, (char *)output.buf+header.encoded_length(), input.fill(), (char *)m_workmem);

  /* check for an incompressible block */
  if (len >= input.fill()) {
    header.set_type(NONE);
    memcpy(output.buf+header.encoded_length(), input.buf, input.fill());
    header.set_length(input.fill());
    header.set_zlength(input.fill());
  }
  else {
    header.set_type(QUICKLZ);
    header.set_length(input.fill());
    header.set_zlength(len);
  }
  header.set_checksum(fletcher32(output.buf + header.encoded_length(), header.get_zlength()));
  
  output.ptr = output.buf;
  header.encode(&output.ptr);
  output.ptr += header.get_zlength();

  return Error::OK;
}


/**
 * 
 */
int BlockCompressionCodecQuicklz::inflate(const DynamicBuffer &input, DynamicBuffer &output, BlockCompressionHeader &header) {
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

   // check compress type
  if (header.get_type() == NONE)
    memcpy(output.buf, msg_ptr, header.get_length());
  else {
    size_t len;
    // decompress 
    len = qlz_decompress((char *)msg_ptr, (char *)output.buf, (char *)m_workmem);
    assert(len == header.get_length());
  }
  output.ptr = output.buf + header.get_length();

  return Error::OK;
}
