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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#ifndef SOCKADDRMAP_H
#define SOCKADDRMAP_H

#include "Common/InetAddr.h"
#include "Common/HashMap.h"

namespace Hypertable {

class SockAddrHash {
 public:
  size_t operator () (const sockaddr_in &addr) const {
    return (size_t)(addr.sin_addr.s_addr ^ addr.sin_port);
  }
};

struct SockAddrEqual {
  bool operator()(const sockaddr_in &addr1, const sockaddr_in &addr2) const {
    return (addr1.sin_addr.s_addr == addr2.sin_addr.s_addr)
           && (addr1.sin_port == addr2.sin_port);
  }
};

template<typename TypeT, typename addr=struct sockaddr_in>
class SockAddrMap : public hash_map<addr, TypeT, SockAddrHash, SockAddrEqual> {
};

} // namespace Hypertable

#endif // SOCKADDRMAP_H
