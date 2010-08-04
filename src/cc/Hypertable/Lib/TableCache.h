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

#ifndef HYPERTABLE_TABLECACHE_H
#define HYPERTABLE_TABLECACHE_H

#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"
#include "Common/String.h"
#include "AsyncComm/ApplicationQueue.h"
#include "Schema.h"
#include "RangeLocator.h"
#include "Types.h"
#include "Table.h"

namespace Hypertable {

  class TableCache : public ReferenceCount {
  public:

    TableCache(PropertiesPtr &, RangeLocatorPtr &, ConnectionManagerPtr &,
               Hyperspace::SessionPtr &, ApplicationQueuePtr &, NameIdMapperPtr &namemap,
               uint32_t default_timeout_ms);

    /**
     *
     */
    TablePtr get(const String &table_name, bool force);

    /**
     * @param table_name Name of table
     * @param output_schema string representation of Schema object
     * @param with_ids if true return CF ids
     * @return false if table_name is not in cache
     */
    bool get_schema_str(const String &table_name, String &output_schema, bool with_ids=false);

    /**
     *
     * @param table_name Name of table
     * @param output_schema Schema object
     * @return false if table_name is not in cache
     */
    bool get_schema(const String &table_name, SchemaPtr &output_schema);

    /**
     * @param table_name
     * @return false if entry is not in cache
     */
    bool remove(const String &table_name);

  private:
    typedef hash_map<String, TablePtr> TableMap;

    PropertiesPtr           m_props;
    RangeLocatorPtr         m_range_locator;
    Comm                   *m_comm;
    ConnectionManagerPtr    m_conn_manager;
    Hyperspace::SessionPtr  m_hyperspace;
    ApplicationQueuePtr     m_app_queue;
    NameIdMapperPtr         m_namemap;
    uint32_t                m_timeout_ms;
    Mutex                   m_mutex;
    TableMap                m_table_map;

  };

  typedef intrusive_ptr<TableCache> TableCachePtr;

} // namespace Hypertable


#endif // HYPERTABLE_TABLECACHE_H
