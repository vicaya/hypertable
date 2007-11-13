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


#include <iostream>
using namespace std;

extern "C" {
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
}

#include "Common/Error.h"
#include "Common/StringExt.h"

#include "Event.h"

namespace Hypertable {

  std::string Event::toString() {
    string dstr;

    dstr = "Event: type=";
    if (type == CONNECTION_ESTABLISHED)
      dstr += "CONNECTION_ESTABLISHED";
    else if (type == DISCONNECT)
      dstr += "DISCONNECT";
    else if (type == MESSAGE) {
      dstr += "MESSAGE protocol=";
      if (header->protocol < Header::PROTOCOL_MAX)
	dstr += Header::protocolStrings[header->protocol];
      else
	dstr += "unknown";
      dstr += (std::string)" id=" + (int)header->id;
      dstr += (std::string)" gid=" + (int)header->gid;
      dstr += (std::string)" headerLen=" + (int)header->headerLen;
      dstr += (std::string)" totalLen=" + (int)header->totalLen;
    }
    else if (type == TIMER)
      dstr += "TIMER";
    else if (type == ERROR)
      dstr += "ERROR";
    else
      dstr += (int)type;

    if (error != Error::OK)
      dstr += (std::string)" \"" + Error::get_text(error) + "\"";

    if (type != TIMER) {
      dstr += " from=";
      dstr += (std::string)inet_ntoa(addr.sin_addr) + ":" + (int)ntohs(addr.sin_port);
    }

    return dstr;
  }

}


