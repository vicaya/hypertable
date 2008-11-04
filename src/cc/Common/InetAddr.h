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
#ifndef HYPERTABLE_INETADDR_H
#define HYPERTABLE_INETADDR_H

extern "C" {
#include <netinet/in.h>
}

#include "Common/String.h"

namespace Hypertable {

  /**
   * High-level entry point to a service
   */
  struct Endpoint {
    Endpoint(const String &host, uint16_t port) : host(host), port(port) {}

    String host;
    uint16_t port;
  };

  std::ostream &operator<<(std::ostream &, const Endpoint &);

  /**
   * Encapsulate an internet address
   *
   * Note, deriving from sockaddr_in is just a path of least resistance
   * and should be changed to aggregation, if we decide to support ipv6
   */
  struct InetAddr : sockaddr_in {
    InetAddr();
    InetAddr(const String &host, uint16_t port);
    InetAddr(const String &endpoint);
    InetAddr(uint32_t ip32, uint16_t port);
    InetAddr(const sockaddr_in &addr) { operator=(addr); }

    InetAddr &operator=(const sockaddr_in &addr) {
      if (this != &addr)
        memcpy(this, &addr, sizeof(sockaddr_in));

      return *this;
    }

    String format(int sep = ':') { return InetAddr::format(*this, sep); }
    String hex(int sep = ':') { return InetAddr::hex(*this, sep); }

    // convenient/legacy static methods
    /** Initialize addr from host port */
    static bool initialize(sockaddr_in *addr, const char *host, uint16_t port);

    /** Initialize addr from an endpoint string (host:port) */
    static bool initialize(sockaddr_in *addr, const char *addr_str);

    /**
     * parse an endpoint string in (host:port) format
     *
     * @param endpoint - input
     * @param defport - default port
     * @return Endpoint tuple
     */
    static Endpoint parse_endpoint(const char *endpoint, int defport = 0);
    static Endpoint parse_endpoint(const String &endpoint, int defport = 0) {
      return parse_endpoint(endpoint.c_str(), defport);
    }

    /**
     * Parse an ipv4 address string in n.n.n.n or n format
     * @param ip - ipv4 string
     * @param port - port of the address
     * @param addr - result address
     * @param base - base of the integer representation (default: 0, handle 0x)
     * @return true on success
     */
    static bool parse_ipv4(const char *ip, uint16_t port, sockaddr_in &addr,
                           int base = 0);

    /** Initialize addr from an integer ip address and port */
    static bool initialize(sockaddr_in *addr, uint32_t haddr, uint16_t port);

    /** Format a socket address */
    static const char *string_format(String &addr_str, const sockaddr_in &addr);
    static String format(const sockaddr_in &addr, int sep = ':');
    static String hex(const sockaddr_in &addr, int sep = ':');
  };

  std::ostream &operator<<(std::ostream &, const sockaddr_in &);

} // namespace Hypertable

#endif // HYPERTABLE_INETADDR_H
