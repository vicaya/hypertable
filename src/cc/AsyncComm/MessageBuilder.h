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


#ifndef HYPERTABLE_MESSAGEBUILDER_H
#define HYPERTABLE_MESSAGEBUILDER_H

#include <iostream>
using namespace std;

extern "C" {
#include <stdint.h>
}

#include "Common/atomic.h"

#include "CommBuf.h"
#include "Message.h"

namespace hypertable {

  class MessageBuilder {

  public:

    static atomic_t msNextMessageId;

    MessageBuilder() : mId(0), mProtocol(0), mFlags(0) { return; }

    virtual ~MessageBuilder() { return; }

    virtual void Reset(uint8_t protocol, uint8_t flags=0) {
      mId = atomic_inc_return(&msNextMessageId);
      mFlags = flags;
      mProtocol = protocol;
    }

    virtual void LoadFromMessage(Message::HeaderT *header) = 0;

    virtual size_t HeaderLength() = 0;

    virtual void Encapsulate(CommBuf *cbuf) = 0;

    void SetFlags(uint8_t flags) { mFlags = flags; }

    void AddFlag(uint8_t flag) { mFlags |= flag; }

    void SetProtocol(uint8_t protocol) { mProtocol = protocol; }

  protected:
    uint32_t  mId;
    uint8_t   mProtocol;
    uint8_t   mFlags;
  };

}


#endif // HYPERTABLE_MESSAGEBUILDER_H
