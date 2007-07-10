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


#ifndef SOCKADDRMAP_H
#define SOCKADDRMAP_H

#include <ext/hash_map>
using namespace std;

extern "C" {
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
}

class SockAddrHash {
 public:
  size_t operator () (struct sockaddr_in addr) const {
    return (size_t)(addr.sin_addr.s_addr ^ addr.sin_port);
  }
};
struct SockAddrEqual {
  bool operator()(struct sockaddr_in addr1, struct sockaddr_in addr2) const {
    return (addr1.sin_addr.s_addr == addr2.sin_addr.s_addr) && (addr1.sin_port == addr2.sin_port);
  }
};

template<typename _Tp, typename addr=struct sockaddr_in>
class SockAddrMapT : public __gnu_cxx::hash_map<addr, _Tp, SockAddrHash, SockAddrEqual> {
};

#endif // SOCKADDRMAP_H
