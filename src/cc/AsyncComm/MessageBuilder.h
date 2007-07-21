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
