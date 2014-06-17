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

#include "TableCache.h"

using namespace Hypertable;
using namespace std;

TableCache::TableCache(PropertiesPtr &props, RangeLocatorPtr &range_locator,
    ConnectionManagerPtr &conn_manager, Hyperspace::SessionPtr &hyperspace,
    ApplicationQueuePtr &app_queue, NameIdMapperPtr &namemap, uint32_t default_timeout_ms)
  : m_props(props), m_range_locator(range_locator), m_comm(conn_manager->get_comm()),
    m_conn_manager(conn_manager), m_hyperspace(hyperspace), m_app_queue(app_queue),
    m_namemap(namemap), m_timeout_ms(default_timeout_ms) {

  HT_ASSERT(m_props && m_range_locator && conn_manager && m_hyperspace &&
            m_app_queue && m_namemap);

}

TablePtr TableCache::get(const String &table_name, bool force) {
  ScopedLock lock(m_mutex);
  String id;

  TableMap::iterator it = m_table_map.find(table_name);
  if (it != m_table_map.end()) {
    if (force || it->second->need_refresh())
      it->second->refresh();

    return it->second;
  }

  TablePtr table = new Table(m_props, m_range_locator, m_conn_manager, m_hyperspace,
                             m_app_queue, m_namemap, table_name, m_timeout_ms);
  m_table_map.insert(make_pair(table_name, table));
  return table;
}

bool TableCache::get_schema_str(const String &table_name, String &schema, bool with_ids)
{
  ScopedLock lock(m_mutex);
  TableMap::const_iterator it = m_table_map.find(table_name);

  if (it == m_table_map.end())
    return false;
  it->second->schema()->render(schema, with_ids);
  return true;
}

bool TableCache::get_schema(const String &table_name, SchemaPtr &output_schema) {
  ScopedLock lock(m_mutex);
  TableMap::const_iterator it = m_table_map.find(table_name);

  if (it == m_table_map.end())
    return false;
  output_schema = new Schema(*(it->second->schema()));
  return true;
}

bool TableCache::remove(const String &table_name) {
  ScopedLock lock(m_mutex);
  bool found = false;
  TableMap::iterator it = m_table_map.find(table_name);

  if (it != m_table_map.end()) {
    found = true;
    m_table_map.erase(it);
  }
  return found;
}
