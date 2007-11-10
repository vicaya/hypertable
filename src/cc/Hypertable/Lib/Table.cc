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

#include "Common/DynamicBuffer.h"
#include "Common/Error.h"
#include "Common/Logger.h"

#include "Hyperspace/HandleCallback.h"
#include "Hyperspace/Session.h"

#include "Table.h"

using namespace hypertable;
using namespace Hyperspace;


/**
 * 
 */
Table::Table(ConnectionManagerPtr &conn_manager_ptr, Hyperspace::SessionPtr &hyperspace_ptr, std::string &name) : m_conn_manager_ptr(conn_manager_ptr), m_hyperspace_ptr(hyperspace_ptr) {
  int error;
  std::string tableFile = (std::string)"/hypertable/tables/" + name;
  DynamicBuffer schemaBuf(0);
  uint64_t handle;
  HandleCallbackPtr nullHandleCallback;
  std::string errMsg;

  /**
   * Open table file
   */
  if ((error = m_hyperspace_ptr->open(tableFile, OPEN_FLAG_READ, nullHandleCallback, &handle)) != Error::OK) {
    LOG_VA_ERROR("Unable to open Hyperspace table file '%s' - %s", tableFile.c_str(), Error::get_text(error));
    throw Exception(error);
  }

  /**
   * Get schema attribute
   */
  if ((error = m_hyperspace_ptr->attr_get(handle, "schema", schemaBuf)) != Error::OK) {
    LOG_VA_ERROR("Problem getting attribute 'schema' for table file '%s' - %s", tableFile.c_str(), Error::get_text(error));
    throw Exception(error);
  }

  m_hyperspace_ptr->close(handle);

  m_schema_ptr = Schema::new_instance((const char *)schemaBuf.buf, strlen((const char *)schemaBuf.buf), true);

  if (!m_schema_ptr->is_valid()) {
    LOG_VA_ERROR("Schema Parse Error: %s", m_schema_ptr->get_error_string());
    throw Exception(Error::BAD_SCHEMA);
  }

  m_range_locator_ptr = new RangeLocator(m_conn_manager_ptr, m_hyperspace_ptr);
}


int Table::create_mutator(MutatorPtr &mutator_ptr) {
  mutator_ptr = new Mutator(m_conn_manager_ptr, m_schema_ptr);
  return Error::OK;
}


int Table::create_scanner(ScanSpecificationT &scan_spec, TableScannerPtr &scanner_ptr) {
  scanner_ptr = new TableScanner(m_schema_ptr, m_range_locator_ptr, scan_spec);
  return Error::OK;
}
