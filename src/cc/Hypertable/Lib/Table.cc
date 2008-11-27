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

#include "Common/Compat.h"
#include <cstring>

#include <boost/algorithm/string.hpp>

#include "Common/String.h"
#include "Common/DynamicBuffer.h"
#include "Common/Error.h"
#include "Common/Logger.h"

#include "Hyperspace/HandleCallback.h"
#include "Hyperspace/Session.h"

#include "Table.h"

using namespace Hypertable;
using namespace Hyperspace;


Table::Table(PropertiesPtr &props, ConnectionManagerPtr &conn_manager,
             Hyperspace::SessionPtr &hyperspace, const String &name)
  : m_props(props), m_comm(conn_manager->get_comm()),
    m_conn_manager(conn_manager), m_hyperspace(hyperspace) {

  HT_TRY("getting table timeout",
    m_timeout_millis = props->get_i32("Hypertable.Client.Timeout"));

  initialize(name);

  m_range_locator = new RangeLocator(props, m_conn_manager, m_hyperspace,
                                     m_timeout_millis);
}


Table::Table(RangeLocatorPtr &range_locator, ConnectionManagerPtr &conn_manager,
    Hyperspace::SessionPtr &hyperspace, const String &name, uint32_t timeout_millis)
  : m_comm(conn_manager->get_comm()), m_conn_manager(conn_manager),
    m_hyperspace(hyperspace), m_range_locator(range_locator),
    m_timeout_millis(timeout_millis) {

  initialize(name);
}


void Table::initialize(const String &name) {
  String tablefile = "/hypertable/tables/"; tablefile += name;
  DynamicBuffer value_buf(0);
  uint64_t handle;
  HandleCallbackPtr null_handle_callback;
  String errmsg;

  // TODO: issue 11
  /**
   * Open table file
   */
  try {
    handle = m_hyperspace->open(tablefile, OPEN_FLAG_READ,
                                null_handle_callback);
  }
  catch (Exception &e) {
    if (e.code() == Error::HYPERSPACE_BAD_PATHNAME)
      HT_THROW2(Error::TABLE_DOES_NOT_EXIST, e, "");
    HT_THROW2F(e.code(), e, "Unable to open Hyperspace table file '%s'",
               tablefile.c_str());
  }
  m_table.name = strdup(name.c_str());

  /**
   * Get table_id attribute
   */
  value_buf.clear();
  m_hyperspace->attr_get(handle, "table_id", value_buf);

  assert(value_buf.fill() == sizeof(int32_t));

  // TODO: fix me!
  memcpy(&m_table.id, value_buf.base, sizeof(int32_t));

  /**
   * Get schema attribute
   */
  value_buf.clear();
  m_hyperspace->attr_get(handle, "schema", value_buf);

  m_hyperspace->close(handle);

  m_schema = Schema::new_instance((const char *)value_buf.base,
      strlen((const char *)value_buf.base), true);

  if (!m_schema->is_valid()) {
    HT_ERRORF("Schema Parse Error: %s", m_schema->get_error_string());
    HT_THROW_(Error::BAD_SCHEMA);
  }

  m_table.generation = m_schema->get_generation();
}

Table::~Table() {
  free((void *)m_table.name);
}



TableMutator *Table::create_mutator(uint32_t timeout_millis) {
  return new TableMutator(m_comm, &m_table, m_schema, m_range_locator,
                          timeout_millis ? timeout_millis : m_timeout_millis);
}



TableScanner *Table::create_scanner(const ScanSpec &scan_spec, uint32_t timeout_millis) {
  return new TableScanner(m_comm, &m_table, m_schema, m_range_locator,
                          scan_spec, timeout_millis ? timeout_millis : m_timeout_millis);
}
