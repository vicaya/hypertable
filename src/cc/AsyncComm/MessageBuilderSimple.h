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
