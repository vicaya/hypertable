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

#include <cstdlib>
#include <cstring>

extern "C" {
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/utsname.h>
}

#include "InetAddr.h"
#include "StringExt.h"

using namespace Hypertable;

/**
 *
 */
bool InetAddr::initialize(struct sockaddr_in *addr, const char *host, uint16_t port) {
  memset(addr, 0, sizeof(struct sockaddr_in));
  {
    struct hostent *he = gethostbyname(host);
    if (he == 0) {
      std::string errMsg = (std::string)"gethostbyname(\"" + host + "\")";
      herror(errMsg.c_str());
      return false;
    }
    memcpy(&addr->sin_addr.s_addr, he->h_addr_list[0], sizeof(uint32_t));
  }
  addr->sin_family = AF_INET;
  addr->sin_port = htons(port);
  return true;
}

bool InetAddr::initialize(struct sockaddr_in *addr, const char *addrStr) {
  const char *colon = strchr(addrStr, ':');
  uint16_t port;
  std::string host;

  if (colon) {
    host = std::string(addrStr, colon-addrStr);
    port = (uint16_t)atoi(colon+1);
    return initialize(addr, host.c_str(), port);
  }
  
  return initialize(addr, "localhost", (uint16_t)atoi(addrStr));
}

/**
 *
 */
bool InetAddr::initialize(struct sockaddr_in *addr, uint32_t haddr, uint16_t port) {
  memset(addr, 0 , sizeof(sockaddr_in));
  addr->sin_family = AF_INET;
  addr->sin_addr.s_addr = htonl(haddr);
  addr->sin_port = htons(port);
  return true;
}

/**
 *
 */
const char *InetAddr::string_format(std::string &addrStr, struct sockaddr_in &addr) {
  addrStr = (std::string)inet_ntoa(addr.sin_addr) + ":" + ntohs(addr.sin_port);
  return addrStr.c_str();
}


/**
 *
 */
void InetAddr::get_hostname(std::string &hostname) {
  struct utsname u;
  uname(&u);
  hostname = u.nodename;
}
