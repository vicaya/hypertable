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
#ifndef HYPERTABLE_RANGESERVER_CONSTANTS_H
#define HYPERTABLE_RANGESERVER_CONSTANTS_H

extern "C" {
#include <stdint.h>
}

namespace hypertable {

  class Constants {

  public:
    static const char DATA_BLOCK_MAGIC[12];
    static const char INDEX_FIXED_BLOCK_MAGIC[12];
    static const char INDEX_VARIABLE_BLOCK_MAGIC[12];

    static const uint32_t DEFAULT_BLOCKSIZE = 65536;

    static const uint16_t COMPRESSION_TYPE_ZLIB = 1;

    typedef struct {
      char      magic[12];
      uint32_t  length;
      uint32_t  zlength;
      uint16_t  checksum;
    } __attribute__((packed)) BlockHeaderT;

  };

}

#endif // HYPERTABLE_RANGESERVER_CONSTANTS_H
