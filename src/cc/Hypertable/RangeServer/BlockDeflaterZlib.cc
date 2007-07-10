/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cassert>
#include <iostream>

#include "Common/DynamicBuffer.h"
#include "Common/Logger.h"

#include "BlockDeflaterZlib.h"
#include "Constants.h"


/**
 *
 */
BlockDeflaterZlib::BlockDeflaterZlib() : BlockDeflater() {
  memset(&mStream, 0, sizeof(mStream));
  mStream.zalloc = Z_NULL;
  mStream.zfree = Z_NULL;
  mStream.opaque = Z_NULL;
  int ret = deflateInit(&mStream, Z_BEST_COMPRESSION);
  assert(ret == Z_OK);
}



/**
 *
 */
BlockDeflaterZlib::~BlockDeflaterZlib() {
  deflateEnd(&mStream);
}



/**
 *
 */
void BlockDeflaterZlib::deflate(hypertable::DynamicBuffer &inbuf, hypertable::DynamicBuffer &outbuf, const char magic[12], size_t reserve) {
  uint32_t avail_out = inbuf.fill() + 6 + (((inbuf.fill() / 16000) + 1 ) * 5);  // see http://www.zlib.net/zlib_tech.html

  outbuf.clear();
  outbuf.reserve( sizeof(Constants::BlockHeaderT) + avail_out + reserve );

  Constants::BlockHeaderT *header = (Constants::BlockHeaderT *)outbuf.buf;

  mStream.avail_in = inbuf.fill();
  mStream.next_in = inbuf.buf;

  mStream.avail_out = avail_out;
  mStream.next_out = outbuf.buf + sizeof(Constants::BlockHeaderT);

  int ret = ::deflate(&mStream, Z_FINISH); 
  assert(ret == Z_STREAM_END);

  memcpy(header->magic, magic, 12);
  header->zlength = (avail_out - mStream.avail_out);
  header->length = inbuf.fill();

  // compute checksum
  uint8_t *checkptr = (uint8_t *)header;
  header->checksum = 0;
  for (size_t i=0; i<sizeof(Constants::BlockHeaderT)-sizeof(int16_t); i++)
    header->checksum += checkptr[i];
  checkptr = outbuf.buf + sizeof(Constants::BlockHeaderT);
  for (size_t i=0; i<header->zlength; i++)
    header->checksum += checkptr[i];

  deflateReset(&mStream);

  outbuf.ptr = outbuf.buf + sizeof(Constants::BlockHeaderT) + header->zlength;
}


