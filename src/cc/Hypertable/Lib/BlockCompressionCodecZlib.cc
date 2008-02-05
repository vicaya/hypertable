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
#include "Common/Logger.h"
#include "Common/Checksum.h"

#include "BlockCompressionCodecZlib.h"

using namespace Hypertable;


/**
 *
 */
BlockCompressionCodecZlib::BlockCompressionCodecZlib(const Args &args) : m_inflate_initialized(false), m_deflate_initialized(false), m_level(Z_BEST_SPEED) {
  if (!args.empty())
    set_args(args);
}



/**
 *
 */
BlockCompressionCodecZlib::~BlockCompressionCodecZlib() {
  HT_ASSERT_SAME_THREAD(m_creator_thread);
  if (m_deflate_initialized)
    deflateEnd(&m_stream_deflate);
  if (m_inflate_initialized)
    inflateEnd(&m_stream_inflate);
}



int BlockCompressionCodecZlib::set_args(const Args &args) {
  Args::const_iterator it = args.begin(), arg_end = args.end();

  for (; it != arg_end; ++it) {
    if (*it == "--best" || *it == "-9") {
      m_level = Z_BEST_COMPRESSION;
      if (m_deflate_initialized) {
        deflateEnd(&m_stream_deflate);
        m_deflate_initialized = false;
      }
    }
    else if (*it == "--normal") {
      m_level = Z_DEFAULT_COMPRESSION;
      if (m_deflate_initialized) {
        deflateEnd(&m_stream_deflate);
        m_deflate_initialized = false;
      }
    }
    else {
      HT_ERRORF("Unrecognized argument to Zlib codec: '%s'", (*it).c_str());
      return Error::BLOCK_COMPRESSOR_INVALID_ARG;
    }
  }
  return Error::OK;
}



/**
 * 
 */
int BlockCompressionCodecZlib::deflate(const DynamicBuffer &input, DynamicBuffer &output, BlockCompressionHeader &header, size_t reserve) {
  uint32_t avail_out = input.fill() + 6 + (((input.fill() / 16000) + 1 ) * 5);  // see http://www.zlib.net/zlib_tech.html

  if (!m_deflate_initialized) {
    memset(&m_stream_deflate, 0, sizeof(m_stream_deflate));
    m_stream_deflate.zalloc = Z_NULL;
    m_stream_deflate.zfree = Z_NULL;
    m_stream_deflate.opaque = Z_NULL;
    int ret = deflateInit(&m_stream_deflate, m_level);
    assert(ret == Z_OK);
    (void)ret;
    m_deflate_initialized = true;
  }

  output.clear();
  output.reserve( header.encoded_length() + avail_out + reserve );

  m_stream_deflate.avail_in = input.fill();
  m_stream_deflate.next_in = input.buf;

  m_stream_deflate.avail_out = avail_out;
  m_stream_deflate.next_out = output.buf + header.encoded_length();

  int ret = ::deflate(&m_stream_deflate, Z_FINISH); 
  assert(ret == Z_STREAM_END);
  (void)ret;

  uint32_t zlen = avail_out - m_stream_deflate.avail_out;

  /* check for an incompressible block */
  if (zlen >= input.fill()) {
    header.set_type(NONE);
    memcpy(output.buf+header.encoded_length(), input.buf, input.fill());
    header.set_length(input.fill());
    header.set_zlength(input.fill());
  }
  else {
    header.set_type(ZLIB);
    header.set_length(input.fill());
    header.set_zlength(zlen);
  }

  header.set_checksum( fletcher32(output.buf + header.encoded_length(), header.get_zlength()) );
  
  deflateReset(&m_stream_deflate);

  output.ptr = output.buf;
  header.encode(&output.ptr);
  output.ptr += header.get_zlength();

  return Error::OK;
}


/**
 * 
 */
int BlockCompressionCodecZlib::inflate(const DynamicBuffer &input, DynamicBuffer &output, BlockCompressionHeader &header) {
  int ret;
  int error;
  uint8_t *msg_ptr = input.buf;
  size_t remaining = input.fill();

  if (!m_inflate_initialized) {
    memset(&m_stream_inflate, 0, sizeof(m_stream_inflate));
    m_stream_inflate.zalloc = Z_NULL;
    m_stream_inflate.zfree = Z_NULL;
    m_stream_inflate.opaque = Z_NULL;
    m_stream_inflate.avail_in = 0;
    m_stream_inflate.next_in = Z_NULL;
    int ret = inflateInit(&m_stream_inflate);
    assert(ret == Z_OK);
    (void)ret;
    m_inflate_initialized = true;
  }

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

    m_stream_inflate.avail_in = remaining;
    m_stream_inflate.next_in = msg_ptr;

    m_stream_inflate.avail_out = header.get_length();
    m_stream_inflate.next_out = output.buf;

    ret = ::inflate(&m_stream_inflate, Z_NO_FLUSH);
    if (ret != Z_STREAM_END) {
      HT_ERRORF("Compressed block inflate error (return value = %d)", ret);
      goto abort;
    }

    if (m_stream_inflate.avail_out != 0) {
      HT_ERRORF("Compressed block inflate error, expected %d but only inflated to %d bytes", header.get_length(), header.get_length()-m_stream_inflate.avail_out);
      goto abort;
    }
    ::inflateReset(&m_stream_inflate);  
  }

  output.ptr = output.buf + header.get_length();

  return Error::OK;

 abort:
  ::inflateReset(&m_stream_inflate);
  output.free();
  return Error::BLOCK_COMPRESSOR_INFLATE_ERROR;
}
