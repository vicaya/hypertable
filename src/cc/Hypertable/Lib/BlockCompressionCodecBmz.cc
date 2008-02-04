/**
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#include "Common/Checksum.h"
#include "Common/Thread.h"
#include "Common/Logger.h"
#include "BlockCompressionCodecBmz.h"
#include "bmz/bmz.h"

using namespace Hypertable;
using namespace std;
using namespace boost;

BlockCompressionCodecBmz::BlockCompressionCodecBmz(const Args &args) :
                                                   m_workmem(0) {
  HT_EXPECT(bmz_init() == BMZ_E_OK, Error::BLOCK_COMPRESSOR_INIT_ERROR);
  // defaults
  m_offset = 0;
  m_fp_len = 19;
  set_args(args);
}

BlockCompressionCodecBmz::~BlockCompressionCodecBmz() {
  HT_ASSERT_SAME_THREAD(m_creator_thread);
}

#define _NEXT_ARG(_code_) do { \
  ++it; \
  HT_EXPECT(it != arg_end, Error::BLOCK_COMPRESSOR_INVALID_ARG); \
  _code_; \
} while (0)

int
BlockCompressionCodecBmz::set_args(const Args &args) {
  Args::const_iterator it = args.begin(), arg_end = args.end();

  for (; it != arg_end; ++it) {
    if (*it == "--fp-len")
      _NEXT_ARG(m_fp_len = atoi((*it).c_str()));
    else if (*it == "--offset")
      _NEXT_ARG(m_offset = atoi((*it).c_str()));
    else HT_ERRORF("unknown bmz compressor argument: %s", (*it).c_str());
  }
  return Error::OK;
}

int
BlockCompressionCodecBmz::deflate(const DynamicBuffer &input,
                                  DynamicBuffer &output,
                                  BlockCompressionHeader &header,
                                  size_t reserve) {
  size_t inlen = input.fill();
  size_t headerlen = header.encoded_length();
  size_t outlen = bmz_pack_buflen(inlen);

  output.reserve(outlen + headerlen + reserve);
  m_workmem.reserve(bmz_pack_worklen(inlen, m_fp_len), true);

  HT_EXPECT(bmz_pack(input.buf, inlen, output.buf + headerlen, &outlen,
                     m_offset, m_fp_len, 0, m_workmem.buf) == BMZ_E_OK, 
            Error::BLOCK_COMPRESSOR_DEFLATE_ERROR);

  // in case of an incompressible block
  if (outlen >= inlen) {
    header.set_type(NONE);
    memcpy(output.buf + headerlen, input.buf, inlen);
    header.set_length(inlen);
    header.set_zlength(inlen);
  }
  else {
    header.set_type(BMZ);
    header.set_length(inlen);
    header.set_zlength(outlen);
  }
  header.set_checksum(fletcher32(output.buf + headerlen, header.get_zlength()));
  
  output.ptr = output.buf;
  header.encode(&output.ptr);
  output.ptr += header.get_zlength();

  return Error::OK;
}

int
BlockCompressionCodecBmz::inflate(const DynamicBuffer &input,
                                  DynamicBuffer &output,
                                  BlockCompressionHeader &header) {
  uint8_t *ip = input.buf;
  size_t remain = input.fill();

  HT_EXPECT(header.decode(&ip, &remain) == Error::OK,
            Error::BLOCK_COMPRESSOR_BAD_HEADER);
  HT_EXPECT(header.get_zlength() == remain,
            Error::BLOCK_COMPRESSOR_BAD_HEADER);
  HT_EXPECT(header.get_checksum() == fletcher32(ip, remain),
            Error::BLOCK_COMPRESSOR_CHECKSUM_MISMATCH);

  size_t outlen = header.get_length();

  if (header.get_type() == NONE)
    memcpy(output.buf, ip, outlen);
  else {
    output.reserve(outlen);
    m_workmem.reserve(bmz_unpack_worklen(outlen), true);

    HT_EXPECT(bmz_unpack(ip, header.get_zlength(), output.buf, &outlen,
                         m_workmem.buf) == BMZ_E_OK,
              Error::BLOCK_COMPRESSOR_INFLATE_ERROR);
    HT_EXPECT(outlen == header.get_length(),
              Error::BLOCK_COMPRESSOR_INFLATE_ERROR);
  }
  output.ptr = output.buf + outlen;

  return Error::OK;
}
