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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#ifndef HYPERTABLE_TABLE_H
#define HYPERTABLE_TABLE_H

#include "Common/Properties.h"
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
    Table(PropertiesPtr &props_ptr, ConnectionManagerPtr &conn_manager_ptr, Hyperspace::SessionPtr &hyperspace_ptr, std::string name);
    Table(PropertiesPtr &props_ptr, Comm *comm, Hyperspace::SessionPtr &hyperspace_ptr, std::string name);
    virtual ~Table();

    int create_mutator(TableMutatorPtr &mutator_ptr);
    int create_scanner(ScanSpecificationT &scan_spec, TableScannerPtr &scanner_ptr);

    void get_identifier(TableIdentifierT *table_id_p) {
      memcpy(table_id_p, &m_table, sizeof(TableIdentifierT));
    }

  private:

    void initialize(std::string &name);

    PropertiesPtr          m_props_ptr;
    Comm                  *m_comm;
    ConnectionManagerPtr   m_conn_manager_ptr;
    Hyperspace::SessionPtr m_hyperspace_ptr;
    SchemaPtr              m_schema_ptr;
    RangeLocatorPtr        m_range_locator_ptr;
    TableIdentifierT       m_table;
  };
  typedef boost::intrusive_ptr<Table> TablePtr;

}

#endif // HYPERTABLE_TABLE_H
