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

#include "KeySpec.h"
#include "MutatorScatterBuffer.h"


/**
 *
 */
MutatorScatterBuffer::MutatorScatterBuffer(ConnectionManagerPtr &conn_manager_ptr, TableIdentifierT *table_identifier, SchemaPtr &schema_ptr, RangeLocatorPtr &range_locator_ptr) : m_conn_manager_ptr(conn_manager_ptr), m_schema_ptr(schema_ptr), m_range_locator_ptr(range_locator_ptr), m_range_server(conn_manager_ptr->get_comm(), 30), m_table_name(table_identifier->name) {
  // copy TableIdentifierT
  memcpy(&m_table_identifier, table_identifier, sizeof(TableIdentifierT));
  m_table_identifier.name = m_table_name.c_str();
}


/**
 *
 */
void MutatorScatterBuffer::set(uint64_t timestamp, KeySpec &key, uint8_t *value, uint32_t value_len) {
}


int MutatorScatterBuffer::flush() {
  return false;
}


bool MutatorScatterBuffer::wait_for_completion() {
  return false;
}


