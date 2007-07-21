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


#ifndef HYPERTABLE_MESSAGEBUILDERSIMPLE_H
#define HYPERTABLE_MESSAGEBUILDERSIMPLE_H

#include "MessageBuilder.h"

namespace hypertable {

  class MessageBuilderSimple : public MessageBuilder {

  public:

    MessageBuilderSimple() : MessageBuilder() { return; }

    virtual ~MessageBuilderSimple() { return; }

    virtual void Reset(uint8_t protocol, uint8_t flags=0) {
      MessageBuilder::Reset(protocol, flags);
    }

    virtual void LoadFromMessage(Message::HeaderT *header) {
      mId = header->id;
      mProtocol = header->protocol;
      mFlags = header->flags;
    }

    virtual size_t HeaderLength() { return sizeof(Message::HeaderT); }

    virtual void Encapsulate(CommBuf *cbuf) {
      Message::HeaderT *mheader;
      cbuf->data -= sizeof(Message::HeaderT);
      cbuf->dataLen += sizeof(Message::HeaderT);
      mheader = (Message::HeaderT *)cbuf->data;
      mheader->version = Message::VERSION;
      mheader->protocol = mProtocol;
      mheader->flags = mFlags;
      mheader->headerLen = sizeof(Message::HeaderT);
      mheader->id = mId;
      mheader->totalLen = cbuf->dataLen + cbuf->extLen;
    }

  };

}

#endif // HYPERTABLE_MESSAGEBUILDERSIMPLE_H
