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

#include <boost/algorithm/string.hpp>

#include "Common/DynamicBuffer.h"
#include "Common/Logger.h"

#include "BlockCompressionCodecZlib.h"
#include "fletcher16.h"

using namespace Hypertable;


/**
 *
 */
BlockCompressionCodecZlib::BlockCompressionCodecZlib(std::string args) : m_inflate_initialized(false), m_deflate_initialized(false), m_level(Z_BEST_SPEED) {
  if (args != "")
    set_args(args);
}



/**
 *
 */
BlockCompressionCodecZlib::~BlockCompressionCodecZlib() {
  if (m_deflate_initialized)
    deflateEnd(&m_stream_deflate);
  if (m_inflate_initialized)
    inflateEnd(&m_stream_inflate);
}



int BlockCompressionCodecZlib::set_args(std::string args) {
  boost::trim(args);

  if (args == "")
    return Error::OK;

  if (args == "--best" || args == "-9") {
    m_level = Z_BEST_COMPRESSION;
    if (m_deflate_initialized) {
      deflateEnd(&m_stream_deflate);
      m_deflate_initialized = false;
    }
  }
  else if (args == "--normal") {
    m_level = Z_DEFAULT_COMPRESSION;
    if (m_deflate_initialized) {
      deflateEnd(&m_stream_deflate);
      m_deflate_initialized = false;
    }
  }
  else {
    LOG_VA_ERROR("Unrecognized argument to Zlib codec: '%s'", args.c_str());
    return Error::BLOCK_COMPRESSOR_INVALID_ARG;
  }
  return Error::OK;
}



/**
 * 
 */
int BlockCompressionCodecZlib::deflate(const DynamicBuffer &input, DynamicBuffer &output, BlockCompressionHeader *header, size_t reserve) {
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
  output.reserve( header->encoded_length() + avail_out + reserve );

  m_stream_deflate.avail_in = input.fill();
  m_stream_deflate.next_in = input.buf;

  m_stream_deflate.avail_out = avail_out;
  m_stream_deflate.next_out = output.buf + header->encoded_length();

  int ret = ::deflate(&m_stream_deflate, Z_FINISH); 
  assert(ret == Z_STREAM_END);
  (void)ret;

  header->set_flags(BlockCompressionHeader::FLAGS_COMPRESSED);
  header->set_length(input.fill());
  header->set_zlength(avail_out - m_stream_deflate.avail_out);
  header->set_checksum( fletcher16(output.buf + header->encoded_length(), header->get_zlength()) );
  
  deflateReset(&m_stream_deflate);

  output.ptr = output.buf;
  header->encode(&output.ptr);
  output.ptr += header->get_zlength();

  return Error::OK;
}


/**
 * 
 */
int BlockCompressionCodecZlib::inflate(const DynamicBuffer &input, DynamicBuffer &output, BlockCompressionHeader *header) {
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

  if ((error = header->decode(&msg_ptr, &remaining)) != Error::OK)
    return error;

  if (header->get_zlength() != remaining) {
    LOG_VA_ERROR("Block decompression error, header zlength = %d, actual = %d", header->get_zlength(), remaining);
    return Error::BLOCK_COMPRESSOR_BAD_HEADER;
  }

  uint16_t checksum = fletcher16(msg_ptr, remaining);
  if (checksum != header->get_checksum()) {
    LOG_VA_ERROR("Compressed block checksum mismatch header=%d, computed=%d", header->get_checksum(), checksum);
    return Error::BLOCK_COMPRESSOR_CHECKSUM_MISMATCH;
  }

  m_stream_inflate.avail_in = remaining;
  m_stream_inflate.next_in = msg_ptr;

  output.reserve(header->get_length());
  m_stream_inflate.avail_out = header->get_length();
  m_stream_inflate.next_out = output.buf;

  ret = ::inflate(&m_stream_inflate, Z_NO_FLUSH);
  if (ret != Z_STREAM_END) {
    LOG_VA_ERROR("Compressed block inflate error (return value = %d)", ret);
    goto abort;
  }

  if (m_stream_inflate.avail_out != 0) {
    LOG_VA_ERROR("Compressed block inflate error, expected %d but only inflated to %d bytes", header->get_length(), header->get_length()-m_stream_inflate.avail_out);
    goto abort;
  }

  output.ptr = output.buf + header->get_length();

  ::inflateReset(&m_stream_inflate);  
  return Error::OK;

 abort:
  ::inflateReset(&m_stream_inflate);
  output.free();
  return Error::BLOCK_COMPRESSOR_INFLATE_ERROR;
}
