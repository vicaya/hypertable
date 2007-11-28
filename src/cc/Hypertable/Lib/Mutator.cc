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

#include "Mutator.h"


/**
 * 
 */
Mutator::Mutator(ConnectionManagerPtr &conn_manager_ptr, TableIdentifierT *table_identifier, SchemaPtr &schema_ptr, RangeLocatorPtr &range_locator_ptr) : m_conn_manager_ptr(conn_manager_ptr), m_schema_ptr(schema_ptr), m_range_locator_ptr(range_locator_ptr), m_range_server(conn_manager_ptr->get_comm(), 30), m_table_name(table_identifier->name) {
  // copy TableIdentifierT
  memcpy(&m_table_identifier, table_identifier, sizeof(TableIdentifierT));
  m_table_identifier.name = m_table_name.c_str();
}


#if 0
/**
 *
 */
void Mutator::set(CellKey &key, uint8_t *value, uint32_t valueLen) {

    void Set(CellKey &key, uint8_t *value, uint32_t valueLen);


  // Location Cache lookup

  // If failed, Lookup range and Load cache

  // Insert mod into per-range queue

  // Update memory used

}
#endif


/**
 *
 */
void Mutator::flush(MutationResultPtr &resultPtr) {

  // Sweep through the set of per-range queues, sending UPDATE requests to their range servers

  // Increment useCount variable in callback, once for each request that went out

}
