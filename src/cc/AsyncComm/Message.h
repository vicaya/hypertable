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


#ifndef HYPERTABLE_MESSAGE_H
#define HYPERTABLE_MESSAGE_H

extern "C" {
#include <stdint.h>
}

namespace hypertable {

  class Message {

  public:

    static const uint8_t VERSION = 1;

    static const uint8_t PROTOCOL_NONE                  = 0;
    static const uint8_t PROTOCOL_HDFS                  = 1;
    static const uint8_t PROTOCOL_PFS                   = 2;
    static const uint8_t PROTOCOL_HYPERTABLE_MASTER       = 3;
    static const uint8_t PROTOCOL_HYPERTABLE_RANGESERVER = 4;
    static const uint8_t PROTOCOL_MAX                   = 5;

    static const uint8_t FLAGS_MASK_REQUEST  = 0x01;
    static const uint8_t FLAGS_MASK_RESPONSE = 0xFE;

    static const char *protocolStrings[PROTOCOL_MAX];

    typedef struct {
      uint8_t   version;
      uint8_t   protocol;
      uint8_t   flags;
      uint8_t   headerLen;
      uint32_t  id;
      uint32_t  totalLen;
    } __attribute__((packed)) HeaderT;

  };

}

#endif // HYPERTABLE_MESSAGE_H
