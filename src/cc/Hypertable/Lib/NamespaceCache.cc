/** -*- c++ -*-
 * Copyright (C) 2010 Sanjit Jhala (Hypertable, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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

#include "Common/Compat.h"
#include "NamespaceCache.h"

using namespace Hypertable;
using namespace std;

NamespaceCache::NamespaceCache(PropertiesPtr &props, RangeLocatorPtr &range_locator,
    ConnectionManagerPtr &conn_manager, Hyperspace::SessionPtr &hyperspace,
    ApplicationQueuePtr &app_queue, NameIdMapperPtr &namemap, MasterClientPtr &master_client,
    TableCachePtr &table_cache, uint32_t default_timeout_ms)
  : m_props(props), m_range_locator(range_locator), m_comm(conn_manager->get_comm()),
    m_conn_manager(conn_manager), m_hyperspace(hyperspace), m_app_queue(app_queue),
    m_namemap(namemap), m_master_client(master_client), m_table_cache(table_cache),
    m_timeout_ms(default_timeout_ms) {

    HT_ASSERT(m_props && m_range_locator && conn_manager && m_hyperspace &&
              m_app_queue && m_namemap && m_master_client && m_table_cache);
}

NamespacePtr NamespaceCache::get(const String &name) {
  ScopedLock lock(m_mutex);
  String id;
  bool is_namespace = false;
  NamespacePtr ns;
  NamespaceMap::iterator it = m_namespace_map.find(name);

  if (it != m_namespace_map.end()) {
    return it->second;
  }

  if (!m_namemap->name_to_id(name, id, &is_namespace) || !is_namespace)
    return NULL;

  ns = new Namespace(name, id, m_props, m_conn_manager, m_hyperspace,
                     m_app_queue, m_namemap, m_master_client, m_range_locator,
                     m_table_cache, m_timeout_ms);
  m_namespace_map.insert(make_pair(name, ns));
  return ns;
}

bool NamespaceCache::remove(const String &name) {
  ScopedLock lock(m_mutex);
  bool found = false;
  NamespaceMap::iterator it = m_namespace_map.find(name);

  if (it != m_namespace_map.end()) {
    found = true;
    m_namespace_map.erase(it);
  }
  return found;
}
