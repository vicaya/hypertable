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
#include "TableScanner.h"
#include "TableMutatorShared.h"

using namespace Hypertable;
using namespace Hyperspace;


Table::Table(PropertiesPtr &props, ConnectionManagerPtr &conn_manager,
             Hyperspace::SessionPtr &hyperspace, NameIdMapperPtr &namemap, const String &name)
  : m_props(props), m_comm(conn_manager->get_comm()),
    m_conn_manager(conn_manager), m_hyperspace(hyperspace), m_namemap(namemap), m_name(name),
    m_stale(true) {

  m_timeout_ms = props->get_i32("Hypertable.Request.Timeout");

  initialize();

  m_range_locator = new RangeLocator(props, m_conn_manager, m_hyperspace,
                                     m_timeout_ms);

  m_app_queue = new ApplicationQueue(props->get_i32("Hypertable.Client.Workers"));
}


Table::Table(PropertiesPtr &props, RangeLocatorPtr &range_locator,
    ConnectionManagerPtr &conn_manager, Hyperspace::SessionPtr &hyperspace,
    ApplicationQueuePtr &app_queue, NameIdMapperPtr &namemap, const String &name,
    uint32_t timeout_ms)
  : m_props(props), m_comm(conn_manager->get_comm()),
    m_conn_manager(conn_manager), m_hyperspace(hyperspace),
    m_range_locator(range_locator), m_app_queue(app_queue), m_namemap(namemap),
    m_name(name), m_timeout_ms(timeout_ms), m_stale(true) {

  initialize();
}


void Table::initialize() {
  String tablefile;
  DynamicBuffer value_buf(0);
  uint64_t handle;
  HandleCallbackPtr null_handle_callback;
  String errmsg;
  String table_id;

  m_toplevel_dir = m_props->get_str("Hypertable.Directory");
  boost::trim_if(m_toplevel_dir, boost::is_any_of("/"));
  m_toplevel_dir = String("/") + m_toplevel_dir;

  // Convert table name to ID string
  if (m_table.id == 0) {
    bool is_namespace;

    if (!m_namemap->name_to_id(m_name, table_id, &is_namespace) ||
        is_namespace)
      HT_THROW(Error::TABLE_NOT_FOUND, m_name);
    m_table.set_id(table_id);
  }
  else {
    bool is_namespace;
    String new_name;

    if (!m_namemap->id_to_name(m_table.id, new_name, &is_namespace) ||
        is_namespace)
      HT_THROWF(Error::TABLE_NOT_FOUND, "%s (%s)", table_id.c_str(), m_name.c_str());
    m_name = new_name;
  }

  tablefile = m_toplevel_dir + "/tables/" + m_table.id;

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
      HT_THROW2(Error::TABLE_NOT_FOUND, e, m_name);
    HT_THROW2F(e.code(), e, "Unable to open Hyperspace table file '%s'",
               tablefile.c_str());
  }

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
  m_stale = false;
}


void Table::refresh() {
  ScopedLock lock(m_mutex);
  HT_ASSERT(m_name != "");
  m_stale = true;
  initialize();
}


void Table::get(TableIdentifierManaged &ident_copy, SchemaPtr &schema_copy) {
  ScopedLock lock(m_mutex);
  ident_copy = m_table;
  schema_copy = m_schema;
}


void
Table::refresh(TableIdentifierManaged &ident_copy, SchemaPtr &schema_copy) {
  ScopedLock lock(m_mutex);
  HT_ASSERT(m_name != "");
  m_stale = true;
  initialize();
  ident_copy = m_table;
  schema_copy = m_schema;
}


Table::~Table() {
}


TableMutator *
Table::create_mutator(uint32_t timeout_ms, uint32_t flags,
                      uint32_t flush_interval_ms) {
  uint32_t timeout = timeout_ms ? timeout_ms : m_timeout_ms;

  if (flush_interval_ms) {
    return new TableMutatorShared(m_props, m_comm, this, m_range_locator,
                                  m_app_queue, timeout, flush_interval_ms, flags);
  }
  return new TableMutator(m_props, m_comm, this, m_range_locator, timeout, flags);
}


TableScanner *
Table::create_scanner(const ScanSpec &scan_spec, uint32_t timeout_ms,
                      bool retry_table_not_found) {
  return new TableScanner(m_comm, this, m_range_locator, scan_spec,
                          timeout_ms ? timeout_ms : m_timeout_ms,
                          retry_table_not_found);
}
