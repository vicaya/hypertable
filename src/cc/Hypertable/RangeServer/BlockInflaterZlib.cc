/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#include <cassert>
#include <iostream>

extern "C" {
#include <zlib.h>
}

#include "Common/DynamicBuffer.h"
#include "Common/Logger.h"

#include "BlockInflaterZlib.h"
#include "Constants.h"

using namespace hypertable;


/**
 *
 */
BlockInflaterZlib::BlockInflaterZlib() : BlockInflater() {
  memset(&mStream, 0, sizeof(mStream));
  mStream.zalloc = Z_NULL;
  mStream.zfree = Z_NULL;
  mStream.opaque = Z_NULL;
  mStream.avail_in = 0;
  mStream.next_in = Z_NULL;
  int ret = inflateInit(&mStream);
  assert(ret == Z_OK);
}



/**
 *
 */
BlockInflaterZlib::~BlockInflaterZlib() {
  inflateEnd(&mStream);
}



/**
 *
 */
bool BlockInflaterZlib::inflate(uint8_t *zbuf, uint32_t zlen, const char magic[12], hypertable::DynamicBuffer &outbuf) {
  Constants::BlockHeaderT *header = (Constants::BlockHeaderT *)zbuf;
  uint8_t *checkptr = zbuf;
  uint16_t  checksum = 0;
  int ret;

  if (zlen < sizeof(Constants::BlockHeaderT)) {
    LOG_VA_ERROR("Bad compressed block only %ld bytes", zlen);
    goto abort;
  }
  else if (zlen != header->zlength + sizeof(Constants::BlockHeaderT)) {
    LOG_VA_ERROR("Bad compressed block size, header says %ld, but actual is %ld", header->zlength + sizeof(Constants::BlockHeaderT), zlen);
    goto abort;
  }

  if (memcmp(header->magic, magic, 12)) {
    ((char *)header->magic)[12] = 0;
    LOG_VA_ERROR("Bad magic string in compressed block \"%s\"", header->magic);
    goto abort;
  }

  /** validate checksum **/
  for (size_t i=0; i<sizeof(Constants::BlockHeaderT)-sizeof(int16_t); i++)
    checksum += checkptr[i];
  checkptr = (uint8_t *)&header[1];
  for (size_t i=0; i<header->zlength; i++)
    checksum += checkptr[i];
  if (checksum != header->checksum) {
    LOG_VA_ERROR("Compressed block checksum mismatch header=%d, computed=%d", header->checksum, checksum);
    goto abort;
  }

  mStream.avail_in = header->zlength;
  mStream.next_in = (uint8_t *)&header[1];

  outbuf.reserve(header->length);
  mStream.avail_out = header->length;
  mStream.next_out = outbuf.buf;

  ret = ::inflate(&mStream, Z_NO_FLUSH);
  if (ret != Z_STREAM_END) {
    LOG_VA_ERROR("Compressed block inflate error (return value = %d)", ret);
    goto abort;
  }

  if (mStream.avail_out != 0) {
    LOG_VA_ERROR("Compressed block inflate error, expected %d but only inflated to %d bytes", header->length, header->length-mStream.avail_out);
    goto abort;
  }

  outbuf.ptr = outbuf.buf + header->length;

  ::inflateReset(&mStream);  
  return true;

 abort:
  ::inflateReset(&mStream);
  outbuf.free();
  return false;
}


