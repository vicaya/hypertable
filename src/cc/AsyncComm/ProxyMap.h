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

#ifndef HYPERTABLE_PROXYMAP_H
#define HYPERTABLE_PROXYMAP_H

#include "Common/InetAddr.h"
#include "Common/HashMap.h"
#include "Common/Mutex.h"
#include "Common/SockAddrMap.h"
#include "Common/String.h"

#include "CommBuf.h"

namespace Hypertable {

  typedef hash_map<String, InetAddr> ProxyMapT;

  class ProxyMap {

  public:
    
    void update_mapping(const String &proxy, const InetAddr &addr,
			ProxyMapT &invalidated_map, ProxyMapT &new_map);
    void update_mappings(String &mappings, ProxyMapT &invalidated_map,
			 ProxyMapT &new_map);
    bool get_mapping(const String &proxy, InetAddr &addr);

    String get_proxy(InetAddr &addr);
    
    void get_map(ProxyMapT &map) {
      map = m_map;
    }

    CommBuf *create_update_message();

  private:

    void invalidate(const String &proxy, const InetAddr &addr,
		    ProxyMapT &invalidated_mappings);

    Mutex     m_mutex;
    ProxyMapT m_map;
    SockAddrMap<String> m_reverse_map;
  };

}

#endif // HYPERTABLE_PROXYMAP_H
