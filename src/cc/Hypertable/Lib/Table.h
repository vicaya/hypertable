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

#include "TableMutator.h"
#include "Schema.h"
#include "RangeLocator.h"
#include "TableScanner.h"
#include "Types.h"

namespace Hyperspace {
  class Session;
}

namespace Hypertable {

  class ConnectionManager;

  /** Represents an open table.
   */
  class Table : public ReferenceCount {

  public:
    Table(PropertiesPtr &, ConnectionManagerPtr &, Hyperspace::SessionPtr &,
          const String &name);
    Table(RangeLocatorPtr &, ConnectionManagerPtr &, Hyperspace::SessionPtr &,
          const String &name, uint32_t default_timeout_millis);
    virtual ~Table();

    /**
     * Creates a mutator on this table
     *
     * @param timeout_millis maximum time in milliseconds to allow
     *        mutator methods to execute before throwing an exception
     * @return newly constructed mutator object
     */
    TableMutator *create_mutator(uint32_t timeout_millis=0);

    /**
     * Creates a scanner on this table
     *
     * @param scan_spec scan specification
     * @param timeout_millis maximum time in milliseconds to allow
     *        scanner methods to execute before throwing an exception
     * @return pointer to scanner object
     */
    TableScanner *create_scanner(const ScanSpec &scan_spec, uint32_t timeout_millis=0);

    void get_identifier(TableIdentifier *table_id_p) {
      memcpy(table_id_p, &m_table, sizeof(TableIdentifier));
    }

    const TableIdentifier &identifier() const { return m_table; }

  private:

    void initialize(const String &name);

    PropertiesPtr          m_props;
    Comm                  *m_comm;
    ConnectionManagerPtr   m_conn_manager;
    Hyperspace::SessionPtr m_hyperspace;
    SchemaPtr              m_schema;
    RangeLocatorPtr        m_range_locator;
    TableIdentifier        m_table;
    int                    m_timeout_millis;
  };

  typedef intrusive_ptr<Table> TablePtr;

} // namesapce Hypertable

#endif // HYPERTABLE_TABLE_H
