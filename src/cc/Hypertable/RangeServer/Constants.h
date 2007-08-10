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
