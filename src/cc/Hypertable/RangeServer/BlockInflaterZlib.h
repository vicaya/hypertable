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
#ifndef HYPERTABLE_BLOCKINFLATERZLIB_H
#define HYPERTABLE_BLOCKINFLATERZLIB_H

extern "C" {
#include <zlib.h>
}

#include "BlockInflater.h"

using namespace hypertable;

namespace hypertable {

  class BlockInflaterZlib : public BlockInflater {
  public:
    BlockInflaterZlib();
    virtual ~BlockInflaterZlib();
    virtual bool inflate(uint8_t *zbuf, uint32_t zlen, const char magic[12], hypertable::DynamicBuffer &outbuf);

  private:
    z_stream  mStream;
  };
}

#endif // HYPERTABLE_BLOCKINFLATERZLIB_H
