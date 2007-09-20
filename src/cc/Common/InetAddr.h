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
#ifndef HYPERTABLE_INETADDR_H
#define HYPERTABLE_INETADDR_H

#include <string>

extern "C" {
#include <stdint.h>
#include <netinet/in.h>
}

namespace hypertable {

  class InetAddr {

  public:
    static bool Initialize(struct sockaddr_in *addr, const char *host, uint16_t port);
    static bool Initialize(struct sockaddr_in *addr, uint32_t haddr, uint16_t port);
    static const char *StringFormat(std::string &addrStr, struct sockaddr_in &addr);
  };

}

#endif // HYPERTABLE_INETADDR_H
