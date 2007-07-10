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


#ifndef HYPERTABLE_EVENT_H
#define HYPERTABLE_EVENT_H

#include <iostream>
#include <string>

extern "C" {
#include <stdint.h>
#include <netinet/in.h>
}

using namespace std;

#include "Message.h"

namespace hypertable {

  class Event {

  public:

    enum Type { CONNECTION_ESTABLISHED, DISCONNECT, MESSAGE, ERROR };

    Event(Type ct, struct sockaddr_in &a, int err=0, Message::HeaderT *h=0) 
      : type(ct), addr(a), error(err), header(h) {
      if (h != 0) {
	message = ((uint8_t *)header) + header->headerLen;
	messageLen = header->totalLen - header->headerLen;
      }
      else {
	message = 0;
	messageLen = 0;
      }
    }

    ~Event() {
      delete [] header;
    }

    Type                type;
    struct sockaddr_in  addr;
    int                 error;
    Message::HeaderT   *header;
    uint8_t            *message;
    size_t              messageLen;

    std::string toString();
    void Display() { cerr << toString() << endl; }
  };
}

#endif // HYPERTABLE_EVENT_H
