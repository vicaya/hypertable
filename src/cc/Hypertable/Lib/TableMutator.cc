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

#include <cstring>

extern "C" {
#include <poll.h>
}

#include <boost/algorithm/string.hpp>

#include "Common/StringExt.h"

#include "Defaults.h"
#include "Key.h"
#include "TableMutator.h"

namespace {
  const uint64_t DEFAULT_MAX_MEMORY = 20000000LL;
}


/**
 * 
 */
TableMutator::TableMutator(PropertiesPtr &props_ptr, Comm *comm, TableIdentifier *table_identifier, SchemaPtr &schema_ptr, RangeLocatorPtr &range_locator_ptr, int timeout) : m_props_ptr(props_ptr), m_comm(comm), m_schema_ptr(schema_ptr), m_range_locator_ptr(range_locator_ptr), m_table_identifier(table_identifier), m_memory_used(0), m_max_memory(DEFAULT_MAX_MEMORY), m_resends(0), m_timeout(timeout) {

  if (m_timeout == 0 ||
      (m_timeout = props_ptr->get_int("Hypertable.Client.Timeout", 0)) == 0 ||
      (m_timeout = props_ptr->get_int("Hypertable.Request.Timeout", 0)) == 0)
    m_timeout = HYPERTABLE_CLIENT_TIMEOUT;

  m_buffer_ptr = new TableMutatorScatterBuffer(props_ptr, m_comm, &m_table_identifier, m_schema_ptr, m_range_locator_ptr);
}


/**
 * 
 */
void TableMutator::set(uint64_t timestamp, KeySpec &key, const void *value, uint32_t value_len) {
  Timer timer(m_timeout);

  sanity_check_key(key);

  if (key.column_family == 0)
    throw Exception(Error::BAD_KEY, "Invalid key - column family not specified");

  {
    Key full_key;
    Schema::ColumnFamily *cf = m_schema_ptr->get_column_family(key.column_family);
    if (cf == 0)
      throw Exception(Error::BAD_KEY, (std::string)"Invalid key - bad column family '" + key.column_family + "'");
    full_key.row = (const char *)key.row;
    full_key.column_qualifier = (const char *)key.column_qualifier;
    full_key.column_family_code = (uint8_t)cf->id;
    full_key.timestamp = timestamp;
    
    m_buffer_ptr->set(full_key, value, value_len, timer);
  }

  m_memory_used += 20 + key.row_len + key.column_qualifier_len + value_len;

  if (m_buffer_ptr->full() || m_memory_used > m_max_memory) {

    timer.start();

    if (m_prev_buffer_ptr)
      wait_for_previous_buffer(timer);

    m_buffer_ptr->send();

    m_prev_buffer_ptr = m_buffer_ptr;

    m_buffer_ptr = new TableMutatorScatterBuffer(m_props_ptr, m_comm, &m_table_identifier, m_schema_ptr, m_range_locator_ptr);
    m_memory_used = 0;
  }

}



void TableMutator::set_delete(uint64_t timestamp, KeySpec &key) {
  Key full_key;
  Timer timer(m_timeout);

  sanity_check_key(key);

  if (key.column_family == 0) {
    full_key.row = (const char *)key.row;
    full_key.column_family_code = 0;
    full_key.column_qualifier = 0;
    full_key.timestamp = timestamp;    
  }
  else  {
    Schema::ColumnFamily *cf = m_schema_ptr->get_column_family(key.column_family);
    if (cf == 0)
      throw Exception(Error::BAD_KEY, (std::string)"Invalid key - bad column family '" + key.column_family + "'");
    full_key.row = (const char *)key.row;
    full_key.column_qualifier = (const char *)key.column_qualifier;
    full_key.column_family_code = (uint8_t)cf->id;
    full_key.timestamp = timestamp;
  }

  m_buffer_ptr->set_delete(full_key, timer);

  m_memory_used += 20 + key.row_len + key.column_qualifier_len;

  if (m_buffer_ptr->full() || m_memory_used > m_max_memory) {

    timer.start();

    if (m_prev_buffer_ptr)
      wait_for_previous_buffer(timer);

    m_buffer_ptr->send();

    m_prev_buffer_ptr = m_buffer_ptr;

    m_buffer_ptr = new TableMutatorScatterBuffer(m_props_ptr, m_comm, &m_table_identifier, m_schema_ptr, m_range_locator_ptr);
    m_memory_used = 0;
  }

}


void TableMutator::flush() {
  Timer timer(m_timeout, true);

  if (m_prev_buffer_ptr)
    wait_for_previous_buffer(timer);

  /**
   * If there are buffered updates, send them and wait for completion
   */
  if (m_memory_used > 0) {
    m_buffer_ptr->send();
    m_prev_buffer_ptr = m_buffer_ptr;
    wait_for_previous_buffer(timer);
  }

  m_buffer_ptr->reset();
  m_prev_buffer_ptr = 0;
}



void TableMutator::wait_for_previous_buffer(Timer &timer) {
  TableMutatorScatterBuffer *redo_buffer = 0;
  int wait_time = 1;

  while (!m_prev_buffer_ptr->wait_for_completion(timer)) {

#if 0
    if (error == Error::RANGESERVER_TIMESTAMP_ORDER_ERROR) {
      std::string table_name_upper = m_table_identifier.name;
      boost::to_upper(table_name_upper);
      throw Exception(error, (std::string)"Problem sending updates (table=" + table_name_upper.c_str() + ")");
    }
#endif

    if (timer.remaining() < wait_time)
      throw Exception(Error::REQUEST_TIMEOUT);

    // wait a bit
    poll(0, 0, wait_time*1000);
    wait_time += 2;

    /**
     * Make several attempts to create redo buffer
     */
    if ((redo_buffer = m_prev_buffer_ptr->create_redo_buffer(timer)) == 0)
      continue;

    m_resends += m_prev_buffer_ptr->get_resend_count();

    m_prev_buffer_ptr = redo_buffer;

    /**
     * Re-send failed sends
     */
    m_prev_buffer_ptr->send();
  }

}



void TableMutator::sanity_check_key(KeySpec &key) {
  const char *row = (const char *)key.row;
  const char *column_qualifier = (const char *)key.column_qualifier;

  /**
   * Sanity check the row key
   */
  if (key.row_len == 0)
    throw Exception(Error::BAD_KEY, "Invalid row key - cannot be zero length");

  if (row[key.row_len] != 0)
    throw Exception(Error::BAD_KEY, "Invalid row key - must be followed by a '\\0' character");

  if (strlen(row) != key.row_len)
    throw Exception(Error::BAD_KEY, (std::string)"Invalid row key - '\\0' character not allowed (offset=" + (uint32_t)strlen(row) + ")");

  if (row[0] == (char)0xff && row[1] == (char)0xff)
    throw Exception(Error::BAD_KEY, "Invalid row key - cannot start with character sequence 0xff 0xff");

  /**
   * Sanity check the column qualifier
   */
  if (key.column_qualifier_len > 0) {
    if (column_qualifier[key.column_qualifier_len] != 0)
      throw Exception(Error::BAD_KEY, "Invalid column qualifier - must be followed by a '\\0' character");
    if (strlen(column_qualifier) != key.column_qualifier_len)
      throw Exception(Error::BAD_KEY, (std::string)"Invalid column qualifier - '\\0' character not allowed (offset=" + (uint32_t)strlen(column_qualifier) + ")");
  }
}
