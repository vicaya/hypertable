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


#include <iostream>
using namespace std;

extern "C" {
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
}

#include "Common/Error.h"

#include "Event.h"

namespace hypertable {

  std::string Event::toString() {
    char cbuf[32];
    string dstr;

    dstr = "Event: type=";
    if (type == CONNECTION_ESTABLISHED)
      dstr += "CONNECTION_ESTABLISHED";
    else if (type == DISCONNECT)
      dstr += "DISCONNECT";
    else if (type == MESSAGE) {
      dstr += "MESSAGE protocol=";
      if (header->protocol < Message::PROTOCOL_MAX)
	dstr += Message::protocolStrings[header->protocol];
      else
	dstr += "unknown";
      dstr += " id=";
      sprintf(cbuf, "%u", header->id);
      dstr += cbuf;
      dstr += " headerLen=";
      sprintf(cbuf, "%u", header->headerLen);
      dstr += cbuf;
      dstr += " totalLen=";
      sprintf(cbuf, "%u", header->totalLen);
      dstr += cbuf;
    }
    else if (type == ERROR) {
      dstr += "ERROR";
    }
    else {
      sprintf(cbuf, "%d", (int)type);
      dstr += cbuf;
    }

    if (error != Error::OK)
      dstr += (std::string)" \"" + Error::GetText(error) + "\"";

    dstr += " from=";
    dstr += inet_ntoa(addr.sin_addr);
    sprintf(cbuf, ":%d", (int)ntohs(addr.sin_port));
    dstr += cbuf;

    return dstr;
  }

}


