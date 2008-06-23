/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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

#include "Common/Properties.h"
#include "Common/ReferenceCount.h"

#include "Defaults.h"
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
    Table(PropertiesPtr &props_ptr, ConnectionManagerPtr &conn_manager_ptr, Hyperspace::SessionPtr &hyperspace_ptr, String name);
    Table(PropertiesPtr &props_ptr, Comm *comm, Hyperspace::SessionPtr &hyperspace_ptr, String name);
    virtual ~Table();

    /**
     * Creates a mutator on this table
     *
     * @param timeout maximum time in seconds to allow mutator methods to execute before throwing an exception
     * @return newly constructed mutator object
     */
    TableMutator *create_mutator(int timeout=0);

    /**
     * Creates a scanner on this table
     *
     * @param scan_spec scan specification
     * @param timeout maximum time in seconds to allow scanner methods to execute before throwing an exception
     * @return pointer to scanner object
     */
    TableScanner *create_scanner(ScanSpec &scan_spec, int timeout=0);

    void get_identifier(TableIdentifier *table_id_p) {
      memcpy(table_id_p, &m_table, sizeof(TableIdentifier));
    }

  private:

    void initialize(const String &name);

    PropertiesPtr          m_props_ptr;
    Comm                  *m_comm;
    ConnectionManagerPtr   m_conn_manager_ptr;
    Hyperspace::SessionPtr m_hyperspace_ptr;
    SchemaPtr              m_schema_ptr;
    RangeLocatorPtr        m_range_locator_ptr;
    TableIdentifier        m_table;
  };
  typedef boost::intrusive_ptr<Table> TablePtr;

}

#endif // HYPERTABLE_TABLE_H
