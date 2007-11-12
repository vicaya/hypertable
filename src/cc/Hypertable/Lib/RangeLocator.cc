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
#include <cassert>
#include <cstring>

#include "Common/Error.h"

#include "Hyperspace/Session.h"

#include "Key.h"
#include "RootFileHandler.h"
#include "RangeLocator.h"
#include "ScanBlock.h"


/**
 * 
 */
RangeLocator::RangeLocator(ConnectionManagerPtr &connManagerPtr, Hyperspace::SessionPtr &hyperspacePtr) : m_conn_manager_ptr(connManagerPtr), m_hyperspace_ptr(hyperspacePtr), m_cache(1000), m_root_stale(true), m_range_server(connManagerPtr->get_comm(), 30) {
  int error;
  DynamicBuffer valueBuf(0);
  HandleCallbackPtr nullHandleCallback;
  uint64_t handle;
  Schema *schema = 0;
  
  m_root_handler_ptr = new RootFileHandler(this);

  if ((error = m_hyperspace_ptr->open("/hypertable/root", OPEN_FLAG_READ, m_root_handler_ptr, &m_root_file_handle)) != Error::OK) {
    LOG_VA_ERROR("Unable to open Hyperspace file '/hypertable/root' (%s)", Error::get_text(error));
    throw Exception(error);
  }

  if ((error = m_hyperspace_ptr->open("/hypertable/tables/METADATA", OPEN_FLAG_READ, nullHandleCallback, &handle)) != Error::OK) {
    LOG_VA_ERROR("Unable to open Hyperspace file '/hypertable/tables/METADATA' (%s)", Error::get_text(error));
    throw Exception(error);
  }

  if ((error = m_hyperspace_ptr->attr_get(handle, "schema", valueBuf)) != Error::OK) {
    LOG_ERROR("Problem getting 'schema' attribute from METADATA hyperspace file");
    throw Exception(error);
  }

  m_hyperspace_ptr->close(handle);

  schema = Schema::new_instance((const char *)valueBuf.buf, valueBuf.fill(), true);
  if (!schema->is_valid()) {
    delete schema;
    LOG_VA_ERROR("Schema Parse Error for table METADATA : %s", schema->get_error_string());
    throw Exception(Error::RANGESERVER_SCHEMA_PARSE_ERROR);
  }

  m_metadata_schema_ptr = schema;

  Schema::ColumnFamily *cf;

  if ((cf = schema->get_column_family("StartRow")) == 0) {
    LOG_ERROR("Unable to find column family 'StartRow' in METADATA schema");
    throw Exception(Error::BAD_SCHEMA);
  }
  m_startrow_cid = cf->id;

  if ((cf = schema->get_column_family("Location")) == 0) {
    LOG_ERROR("Unable to find column family 'Location' in METADATA schema");
    throw Exception(Error::BAD_SCHEMA);
  }
  m_location_cid = cf->id;

}

RangeLocator::~RangeLocator() {
  m_hyperspace_ptr->close(m_root_file_handle);
}



/** Determines the location of the range server containing the given
 * row key of the given table.
 *
 * @param table_id table ID of the table containing row key
 * @param row_key row key to lookup
 * @param location_ptr address of return pointer to server location string
 * @param Error::OK on success or error code on failure
 */
int RangeLocator::find(TableIdentifierT *table, const char *row_key, const char **location_ptr) {
  RangeT range;
  ScanSpecificationT scan_spec;
  ScanBlock scan_block;
  int error;
  Key keyComps;

  if (m_root_stale) {
    if ((error = read_root_location()) != Error::OK)
      return error;
  }

  if (m_cache.lookup(table->id, row_key, location_ptr))
    return Error::OK;

  if (table->id == 0) {

    assert(strcmp(row_key, "0:"));

    range.startRow = 0;
    range.endRow = "1:";

    scan_spec.rowLimit = 0;
    scan_spec.cellLimit = 1;
    scan_spec.columns.push_back("StartRow");
    scan_spec.columns.push_back("Location");
    scan_spec.startRow = row_key;
    scan_spec.startRowInclusive = true;
    scan_spec.endRow = row_key;
    scan_spec.endRowInclusive = true;
    // scan_spec.interval = ????;

    if ((error = m_range_server.create_scanner(m_root_addr, *table, range, scan_spec, scan_block)) != Error::OK)
      return error;

#if 0

    ScanResult::VectorT &resultVec = result.get_vector();

    if (resultVec.size() != 2) {
      LOG_VA_ERROR("METADATA lookup of '%s' yielded %d results, 2 were expected", row_key, resultVec.size());
      return Error::INVALID_METADATA;
    }

    for (size_t i=0; i<resultVec.size(); i++) {
      if (!keyComps.load(resultVec[i].first)) {
	LOG_VA_ERROR("METADATA lookup for '%s' returned bad key", row_key);
	return Error::INVALID_METADATA;
      }
      
      if (keyComps.columnFamily == m_startrow_cid) {
	/** do something **/	
      }
      else if (keyComps.columnFamily == m_location_cid) {
	/** do something **/	
      }
      else {
	LOG_VA_ERROR("METADATA lookup for '%s' returned incorrect column (id=%d)", row_key, keyComps.columnFamily);
	return Error::INVALID_METADATA;
      }
    }

#endif

  }
  else {
    /** do something **/
  }

  return Error::OK;
}


/**
 * 
 */
int RangeLocator::read_root_location() {
  int error;
  DynamicBuffer value(0);
  std::string addrStr;
  char *ptr;

  if ((error = m_hyperspace_ptr->attr_get(m_root_file_handle, "location", value)) != Error::OK) {
    LOG_VA_ERROR("Problem reading 'location' attribute of Hyperspace file /hypertable/root - %s", Error::get_text(error));
    return error;
  }

  m_cache.insert(0, 0, "1:", (const char *)value.buf);

  if ((ptr = strchr((const char *)value.buf, '_')) != 0)
    *ptr = 0;

  if (!InetAddr::initialize(&m_root_addr, (const char *)value.buf)) {
    LOG_ERROR("Unable to extract address from /hypertable/root Hyperspace file");
    return Error::BAD_ROOT_LOCATION;
  }

  m_conn_manager_ptr->add(m_root_addr, 30, "Root RangeServer");

  if (!m_conn_manager_ptr->wait_for_connection(m_root_addr, 20)) {
    LOG_VA_ERROR("Timeout (20s) waiting for root RangeServer connection - %s", (const char *)value.buf);
  }

  m_root_stale = false;

  return Error::OK;
}
