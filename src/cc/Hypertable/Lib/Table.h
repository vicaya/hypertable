/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERTABLE_TABLE_H
#define HYPERTABLE_TABLE_H

#include "Common/ReferenceCount.h"
#include "Common/Mutex.h"

#include "AsyncComm/ApplicationQueue.h"

#include "NameIdMapper.h"
#include "Schema.h"
#include "RangeLocator.h"
#include "Types.h"

namespace Hyperspace {
  class Session;
}

namespace Hypertable {

  class ConnectionManager;
  class ResultCallback;
  class TableScannerAsync;
  class TableScanner;
  class TableMutator;

  /** Represents an open table.
   */
  class Table : public ReferenceCount {

  public:
    Table(PropertiesPtr &, ConnectionManagerPtr &, Hyperspace::SessionPtr &,
          NameIdMapperPtr &namemap, const String &name);
    Table(PropertiesPtr &, RangeLocatorPtr &, ConnectionManagerPtr &,
          Hyperspace::SessionPtr &, ApplicationQueuePtr &, NameIdMapperPtr &namemap,
          const String &name, uint32_t default_timeout_ms);
    virtual ~Table();

    /**
     * Creates a mutator on this table
     *
     * @param timeout_ms maximum time in milliseconds to allow
     *        mutator methods to execute before throwing an exception
     * @param flags mutator flags
     * @param flush_interval_ms time interval in milliseconds to flush
     *        data. 0 disables it.
     * @return newly constructed mutator object
     */
    TableMutator *create_mutator(uint32_t timeout_ms = 0,
                                 uint32_t flags = 0,
                                 uint32_t flush_interval_ms = 0);

    /**
     * Creates a synchronous scanner on this table
     *
     * @param scan_spec scan specification
     * @param timeout_ms maximum time in milliseconds to allow
     *        scanner methods to execute before throwing an exception
     * @param retry_table_not_found whether to retry upon errors caused by
     *        drop/create tables with the same name
     * @param scanner_queue_size max scanblocks the scanner can queue up
     * @return pointer to scanner object
     */
    TableScanner *create_scanner(const ScanSpec &scan_spec,
                                 uint32_t timeout_ms = 0,
                                 bool retry_table_not_found = false,
                                 size_t scanner_queue_size=0);
    /**
     * Creates an asynchronous scanner on this table
     *
     * @param scan_spec scan specification
     * @param timeout_ms maximum time in milliseconds to allow
     *        scanner methods to execute before throwing an exception
     * @param retry_table_not_found whether to retry upon errors caused by
     *        drop/create tables with the same name
     * @param cb callback to be notified when scan results arrive
     * @return pointer to scanner object
     */
    TableScannerAsync *create_scanner_async(ResultCallback *cb,
                                            const ScanSpec &scan_spec,
                                            uint32_t timeout_ms = 0,
                                            bool retry_table_not_found = false);

    void get_identifier(TableIdentifier *table_id_p) {
      memcpy(table_id_p, &m_table, sizeof(TableIdentifier));
    }

    String get_name() {
      ScopedLock lock(m_mutex);
      return m_name;
    }

    SchemaPtr schema() {
      ScopedLock lock(m_mutex);
      return m_schema;
    }

    /**
     * Refresh schema etc.
     */
    void refresh();

    /**
     * Get a copy of table identifier and schema atomically
     *
     * @param table_identifier reference of the table identifier copy
     * @param schema reference of the schema copy
     */
    void get(TableIdentifierManaged &table_identifier, SchemaPtr &schema);

    /**
     * Make a copy of table identifier and schema atomically also
     */
    void refresh(TableIdentifierManaged &table_identifier, SchemaPtr &schema);

    bool need_refresh() {
      ScopedLock lock(m_mutex);
      return m_stale;
    }

  private:
    void initialize();

    Mutex                  m_mutex;
    PropertiesPtr          m_props;
    Comm                  *m_comm;
    ConnectionManagerPtr   m_conn_manager;
    Hyperspace::SessionPtr m_hyperspace;
    SchemaPtr              m_schema;
    RangeLocatorPtr        m_range_locator;
    ApplicationQueuePtr    m_app_queue;
    NameIdMapperPtr        m_namemap;
    String                 m_name;
    TableIdentifierManaged m_table;
    int                    m_timeout_ms;
    bool                   m_stale;
    String                 m_toplevel_dir;
    size_t                 m_scanner_queue_size;
  };

  typedef intrusive_ptr<Table> TablePtr;

} // namesapce Hypertable

#endif // HYPERTABLE_TABLE_H
