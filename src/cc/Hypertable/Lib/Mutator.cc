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

#include <cstring>

#include "Common/StringExt.h"

#include "Mutator.h"


/**
 * 
 */
Mutator::Mutator(ConnectionManagerPtr &conn_manager_ptr, TableIdentifierT *table_identifier, SchemaPtr &schema_ptr, RangeLocatorPtr &range_locator_ptr) : m_conn_manager_ptr(conn_manager_ptr), m_schema_ptr(schema_ptr), m_range_locator_ptr(range_locator_ptr), m_table_name(table_identifier->name), m_cur_memory_used(0) {
  // copy TableIdentifierT
  memcpy(&m_table_identifier, table_identifier, sizeof(TableIdentifierT));
  m_table_identifier.name = m_table_name.c_str();
}


/**
 * 
 */
void Mutator::set(uint64_t timestamp, KeySpec &key, uint8_t *value, uint32_t value_len) {
  int error;
  const char *row = (const char *)key.row;
  const char *column_qualifier = (const char *)key.column_qualifier;
  RangeLocationInfo range_info;

  /**
   * Sanity check the row key
   */
  if (key.row_len == 0)
    throw Exception(Error::BAD_KEY, "Invalid row key - cannot be zero length");

  if (row[key.row_len] != 0)
    throw Exception(Error::BAD_KEY, "Invalid row key - must be followed by a '\0' character");

  if (strlen(row) != key.row_len)
    throw Exception(Error::BAD_KEY, (std::string)"Invalid row key - '\0' character not allowed (offset=" + (uint32_t)strlen(row) + ")");

  if (row[0] == (char)0xff && row[1] == (char)0xff)
    throw Exception(Error::BAD_KEY, "Invalid row key - cannot start with character sequence 0xff 0xff");

  /**
   * Sanity check the column qualifier
   */
  if (key.column_qualifier_len > 0) {
    if (column_qualifier[key.column_qualifier_len] != 0)
      throw Exception(Error::BAD_KEY, "Invalid column qualifier - must be followed by a '\0' character");
    if (strlen(column_qualifier) != key.column_qualifier_len)
      throw Exception(Error::BAD_KEY, (std::string)"Invalid column qualifier - '\0' character not allowed (offset=" + (uint32_t)strlen(column_qualifier) + ")");
  }

  m_buffer_ptr->set(timestamp, key, value, value_len);

  m_cur_memory_used += 20 + key.row_len + key.column_qualifier_len + value_len;

  if (m_cur_memory_used > m_max_memory) {

    if (m_prev_buffer_ptr) {
      if (!m_prev_buffer_ptr->wait_for_completion()) {
	// Something went wrong ...
      }
    }

    if ((error = m_buffer_ptr->flush()) != Error::OK) {
      // something went wrong ...
    }

    m_prev_buffer_ptr = m_buffer_ptr;

    m_buffer_ptr = new MutatorScatterBuffer(m_conn_manager_ptr, &m_table_identifier, m_schema_ptr, m_range_locator_ptr);

  }

}



#if 0
    void delete_row(uint64_t timestamp, KeySpec &key);
    void delete_cell(uint64_t timestamp, KeySpec &key);
    void delete_qualified_cell(uint64_t timestamp, KeySpec &key);
#endif



/**
 *

void Mutator::flush(MutationResultPtr &resultPtr) {

  // Sweep through the set of per-range queues, sending UPDATE requests to their range servers

  // Increment useCount variable in callback, once for each request that went out

}
 */
