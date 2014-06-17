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

#ifndef HYPERTABLE_NAMESPACECACHE_H
#define HYPERTABLE_NAMESPACECACHE_H

#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"
#include "Common/String.h"
#include "AsyncComm/ApplicationQueue.h"
#include "TableCache.h"
#include "Schema.h"
#include "RangeLocator.h"
#include "Types.h"
#include "Namespace.h"

namespace Hypertable {

  class NamespaceCache : public ReferenceCount {
  public:

    NamespaceCache(PropertiesPtr &props, RangeLocatorPtr &range_locator,
                   ConnectionManagerPtr &conn_manager, Hyperspace::SessionPtr &hyperspace,
                   ApplicationQueuePtr &app_queue, NameIdMapperPtr &namemap,
                   MasterClientPtr &master_client, TableCachePtr &table_cache,
                   uint32_t default_timeout_ms);

    /**
     * @param name namespace name
     * @return NULL if the namespace doesn't exist, ow return a pointer to the Namespace
     */
    NamespacePtr get(const String &name);

    /**
     * @param name
     * @return false if entry is not in cache
     */
    bool remove(const String &name);

  private:
    typedef hash_map<String, NamespacePtr> NamespaceMap;

    PropertiesPtr           m_props;
    RangeLocatorPtr         m_range_locator;
    Comm                   *m_comm;
    ConnectionManagerPtr    m_conn_manager;
    Hyperspace::SessionPtr  m_hyperspace;
    ApplicationQueuePtr     m_app_queue;
    NameIdMapperPtr         m_namemap;
    MasterClientPtr         m_master_client;
    TableCachePtr           m_table_cache;
    uint32_t                m_timeout_ms;
    Mutex                   m_mutex;
    NamespaceMap            m_namespace_map;

  };

  typedef intrusive_ptr<NamespaceCache> NamespaceCachePtr;

} // namespace Hypertable


#endif // HYPERTABLE_
