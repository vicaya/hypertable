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

#ifndef HYPERTABLE_HEADER_H
#define HYPERTABLE_HEADER_H

extern "C" {
#include <stdint.h>
}

namespace Hypertable {

  class Header {

  public:

    static const uint8_t VERSION = 1;

    static const uint8_t PROTOCOL_NONE                    = 0;
    static const uint8_t PROTOCOL_DFSBROKER               = 1;
    static const uint8_t PROTOCOL_HYPERSPACE              = 2;
    static const uint8_t PROTOCOL_HYPERTABLE_MASTER       = 3;
    static const uint8_t PROTOCOL_HYPERTABLE_RANGESERVER  = 4;
    static const uint8_t PROTOCOL_MAX                     = 5;

    static const uint8_t FLAGS_BIT_REQUEST          = 0x01;
    static const uint8_t FLAGS_BIT_IGNORE_RESPONSE  = 0x02;

    static const uint8_t FLAGS_MASK_REQUEST         = 0xFE;
    static const uint8_t FLAGS_MASK_IGNORE_RESPONSE = 0xFD;

    static const char *protocolStrings[PROTOCOL_MAX];

    typedef struct {
      uint8_t   version;
      uint8_t   protocol;
      uint8_t   flags;
      uint8_t   headerLen;
      uint32_t  id;
      uint32_t  gid;
      uint32_t  totalLen;
    } __attribute__((packed)) HeaderT;

  };

}

#endif // HYPERTABLE_HEADER_H
