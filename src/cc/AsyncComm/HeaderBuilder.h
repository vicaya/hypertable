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

#ifndef HYPERTABLE_HEADERBUILDER_H
#define HYPERTABLE_HEADERBUILDER_H

#include <iostream>
using namespace std;

extern "C" {
#include <stdint.h>
}

#include "Common/atomic.h"

#include "Header.h"

namespace hypertable {

  class HeaderBuilder {

  public:

    static atomic_t msNextMessageId;

    HeaderBuilder() : mId(0), mGroupId(0), mTotalLen(0), mProtocol(0), mFlags(0) { return; }

    HeaderBuilder(uint8_t protocol) : mId(0), mGroupId(0), mTotalLen(0), mProtocol(protocol), mFlags(0) { return; }

    void Reset(uint8_t protocol, uint8_t flags=0) {
      mId = atomic_inc_return(&msNextMessageId);
      mGroupId = 0;
      mProtocol = protocol;
      mFlags = flags;
      mTotalLen = 0;
    }

    void LoadFromMessage(Header::HeaderT *header) {
      mId        = header->id;
      mGroupId   = header->gid;
      mProtocol  = header->protocol;
      mFlags     = header->flags;
      mTotalLen  = 0;
    }

    size_t HeaderLength() {
      return sizeof(Header::HeaderT);
    }

    void Encode(uint8_t **bufPtr) {
      Header::HeaderT *mheader;
      mheader = (Header::HeaderT *)*bufPtr;
      mheader->version = Header::VERSION;
      mheader->protocol = mProtocol;
      mheader->flags = mFlags;
      mheader->headerLen = sizeof(Header::HeaderT);
      mheader->id = mId;
      mheader->gid = mGroupId;
      mheader->totalLen = mTotalLen;
      (*bufPtr) += sizeof(Header::HeaderT);
    }


    void SetFlags(uint8_t flags) { mFlags = flags; }

    void AddFlag(uint8_t flag) { mFlags |= flag; }

    void SetProtocol(uint8_t protocol) { mProtocol = protocol; }

    void SetGroupId(uint32_t groupId) { mGroupId = groupId; }

    void SetTotalLen(uint32_t totalLen) { mTotalLen = totalLen; }

  protected:
    uint32_t  mId;
    uint32_t  mGroupId;
    uint32_t  mTotalLen;
    uint8_t   mProtocol;
    uint8_t   mFlags;
  };

}


#endif // HYPERTABLE_HEADERBUILDER_H
