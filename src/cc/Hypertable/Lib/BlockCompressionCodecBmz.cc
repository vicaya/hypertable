/** -*- c++ -*-
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
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
#include "Common/Checksum.h"
#include "Common/Thread.h"
#include "Common/Logger.h"
#include "BlockCompressionCodecBmz.h"
#include "bmz/bmz.h"

using namespace Hypertable;
using namespace std;

BlockCompressionCodecBmz::BlockCompressionCodecBmz(const Args &args)
    : m_workmem(0) {
  HT_EXPECT(bmz_init() == BMZ_E_OK, Error::BLOCK_COMPRESSOR_INIT_ERROR);
  // defaults
  m_offset = 0;
  m_fp_len = 19;
  set_args(args);
}

BlockCompressionCodecBmz::~BlockCompressionCodecBmz() {
}

#define _NEXT_ARG(_code_) do { \
  ++it; \
  HT_EXPECT(it != arg_end, Error::BLOCK_COMPRESSOR_INVALID_ARG); \
  _code_; \
} while (0)

void
BlockCompressionCodecBmz::set_args(const Args &args) {
  Args::const_iterator it = args.begin(), arg_end = args.end();

  for (; it != arg_end; ++it) {
    if (*it == "--fp-len")
      _NEXT_ARG(m_fp_len = atoi((*it).c_str()));
    else if (*it == "--offset")
      _NEXT_ARG(m_offset = atoi((*it).c_str()));
    else HT_ERRORF("unknown bmz compressor argument: %s", (*it).c_str());
  }
}

void
BlockCompressionCodecBmz::deflate(const DynamicBuffer &input,
                                  DynamicBuffer &output,
                                  BlockCompressionHeader &header,
                                  size_t reserve) {
  size_t inlen = input.fill();
  size_t headerlen = header.length();
  size_t outlen = bmz_pack_buflen(inlen);

  output.reserve(outlen + headerlen + reserve);
  m_workmem.reserve(bmz_pack_worklen(inlen, m_fp_len), true);

  HT_EXPECT(bmz_pack(input.base, inlen, output.base + headerlen, &outlen,
                     m_offset, m_fp_len, 0, m_workmem.base) == BMZ_E_OK,
            Error::BLOCK_COMPRESSOR_DEFLATE_ERROR);

  // in case of an incompressible block
  if (outlen >= inlen) {
    header.set_compression_type(NONE);
    memcpy(output.base + headerlen, input.base, inlen);
    header.set_data_length(inlen);
    header.set_data_zlength(inlen);
  }
  else {
    header.set_compression_type(BMZ);
    header.set_data_length(inlen);
    header.set_data_zlength(outlen);
  }
  header.set_data_checksum(fletcher32(output.base + headerlen,
                                      header.get_data_zlength()));
  output.ptr = output.base;
  header.encode(&output.ptr);
  output.ptr += header.get_data_zlength();
}

void
BlockCompressionCodecBmz::inflate(const DynamicBuffer &input,
                                  DynamicBuffer &output,
                                  BlockCompressionHeader &header) {
  const uint8_t *ip = input.base;
  size_t remain = input.fill();

  header.decode(&ip, &remain);
  HT_EXPECT(header.get_data_zlength() == remain,
            Error::BLOCK_COMPRESSOR_BAD_HEADER);
  HT_EXPECT(header.get_data_checksum() == fletcher32(ip, remain),
            Error::BLOCK_COMPRESSOR_CHECKSUM_MISMATCH);

  size_t outlen = header.get_data_length();

  output.reserve(outlen);

  if (header.get_compression_type() == NONE)
    memcpy(output.base, ip, outlen);
  else {
    m_workmem.reserve(bmz_unpack_worklen(outlen), true);

    HT_EXPECT(bmz_unpack(ip, header.get_data_zlength(), output.base, &outlen,
                         m_workmem.base) == BMZ_E_OK,
              Error::BLOCK_COMPRESSOR_INFLATE_ERROR);
    HT_EXPECT(outlen == header.get_data_length(),
              Error::BLOCK_COMPRESSOR_INFLATE_ERROR);
  }
  output.ptr = output.base + outlen;
}
