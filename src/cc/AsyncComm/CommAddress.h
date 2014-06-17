/** -*- c++ -*-
 * Copyright (C) 2010 Doug Judd (Hypertable, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2
 * of the License.
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

#ifndef HYPERTABLE_COMMADDRESS_H
#define HYPERTABLE_COMMADDRESS_H

#include <set>

#include "Common/HashMap.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/String.h"

namespace Hypertable {

  class CommAddress {
  public:

    enum AddressType { NONE=0, PROXY, INET };

    CommAddress() : m_type(NONE) { }
    CommAddress(const sockaddr_in iaddr) : m_type(INET) { inet=iaddr; }

    void set_proxy(const String &p) { proxy = p; m_type=PROXY; }
    void set_inet(sockaddr_in iaddr) { inet = iaddr; m_type=INET; }

    CommAddress &operator=(sockaddr_in iaddr) 
      { inet = iaddr; m_type=INET; return *this; }

    bool operator<(const CommAddress &other) const {
      if (m_type != other.type())
	return m_type < other.type();
      if (m_type == PROXY)
	return proxy < other.proxy;
      HT_ASSERT(m_type == INET);
      return inet < other.inet;
    }

    bool is_proxy() const { return m_type == PROXY; }
    bool is_inet() const { return m_type == INET; }
    bool is_set() const { return m_type == PROXY || m_type == INET; }
    
    void clear() { proxy=""; m_type=NONE; }

    String to_str() const;

    int32_t type() const { return m_type; }

    String proxy;
    InetAddr inet;

  private:
    int32_t m_type;
  };

  class CommAddressHash {
  public:
    size_t operator () (const CommAddress &addr) const {
      if (addr.is_inet())
	return (size_t)(addr.inet.sin_addr.s_addr ^ addr.inet.sin_port);
      else if (addr.is_proxy()) {
	__gnu_cxx::hash<const char *> cchash;
	return cchash(addr.proxy.c_str());
      }
      return 0;
    }
  };

  struct CommAddressEqual {
    bool operator()(const CommAddress &addr1, const CommAddress &addr2) const {
      if (addr1.type() == addr2.type() &&
	  ((addr1.is_proxy() && (addr1.proxy==addr2.proxy)) ||
	   (addr1.is_inet() && (addr1.inet==addr2.inet))))
	return true;
      return false;
    }
  };

  template<typename TypeT, typename addr=CommAddress>
  class CommAddressMap : public hash_map<addr, TypeT, CommAddressHash, CommAddressEqual> {
  };

  typedef std::set<CommAddress> CommAddressSet;

} // namespace Hypertable

#endif // HYPERTABLE_COMMADDRESS_H
