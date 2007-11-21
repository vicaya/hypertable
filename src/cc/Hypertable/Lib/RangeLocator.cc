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

namespace {
  const uint32_t METADATA_READAHEAD_COUNT = 10;
  const char root_end_row[5] = { '0', ':', 0xFF, 0xFF, 0 };
  const char *METADATA_ROOT_END_ROW = (const char *)root_end_row;
}


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

  m_metadata_table.name = "METADATA";
  m_metadata_table.id = 0;
  m_metadata_table.generation = schema->get_generation();

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



int RangeLocator::find(TableIdentifierT *table, const char *row_key, RangeLocationInfo *range_loc_info_p) {
  RangeT range;
  ScanSpecificationT meta_scan_spec;
  ScanBlock scan_block;
  int error;
  Key keyComps;
  const ByteString32T *key;
  const ByteString32T *value;
  RangeLocationInfo range_loc_info;
  std::string start_row;
  std::string end_row;
  char buf[16];
  struct sockaddr_in addr;
  TableIdentifierT *table_ptr;

  if (m_root_stale) {
    if ((error = read_root_location()) != Error::OK)
      return error;
  }

  if (m_cache.lookup(table->id, row_key, range_loc_info_p))
    return Error::OK;

  range.startRow = 0;
  range.endRow = METADATA_ROOT_END_ROW;
  memcpy(&addr, &m_root_addr, sizeof(struct sockaddr_in));
  table_ptr = &m_metadata_table;

  for (size_t level=0; level<2; level++) {

    /** Construct the start row and end row of METADATA range that we're interested in */
    if (level == 0) {
      if (table->id == 0) {
	start_row = build_metadata_start_row_key(buf, "%d:", 0, row_key);
	end_row   = build_metadata_end_row_key(buf, "%d:", 0);
      }
      else {
	start_row = build_metadata_start_row_key(buf, "0:%d:", table->id, row_key);
	end_row   = build_metadata_end_row_key(buf, "0:%d:", table->id);
      }
    }
    else {
      assert(table->id != 0);
      start_row = build_metadata_start_row_key(buf, "%d:", table->id, row_key);
      end_row   = build_metadata_end_row_key(buf, "%d:", table->id);
    }

    /** Do a cache lookup for the start row to see if we already have range info for that row */
    if (m_cache.lookup(0, start_row.c_str(), range_loc_info_p)) {
      if (table->id == 0 || level == 1)
	return Error::OK;
      range.startRow = range_loc_info_p->start_row.c_str();
      range.endRow   = range_loc_info_p->end_row.c_str();
      if (!LocationCache::location_to_addr(range_loc_info_p->location.c_str(), addr)) {
	LOG_VA_ERROR("Invalid location found in METADATA entry for row '%s' - %s", start_row.c_str(), range_loc_info_p->location.c_str());
	return Error::INVALID_METADATA;
      }
      continue;
    }

    meta_scan_spec.rowLimit = METADATA_READAHEAD_COUNT;
    meta_scan_spec.max_versions = 1;
    meta_scan_spec.columns.push_back("StartRow");
    meta_scan_spec.columns.push_back("Location");
    meta_scan_spec.startRow = start_row.c_str();
    meta_scan_spec.startRowInclusive = true;
    meta_scan_spec.endRow = end_row.c_str();
    meta_scan_spec.endRowInclusive = true;
    // meta_scan_spec.interval = ????;

    if ((error = m_range_server.create_scanner(addr, *table, range, meta_scan_spec, scan_block)) != Error::OK)
      return error;

    range_loc_info.start_row = "";
    range_loc_info.end_row = "";
    range_loc_info.location = "";

    bool got_start_row = false;
    bool got_end_row = false;
    bool got_location = false;

    while (scan_block.next(key, value)) {

      if (!keyComps.load(key)) {
	LOG_VA_ERROR("METADATA lookup for '%s' returned bad key", (const char *)key->data);
	// TODO: delete scanner 
	return Error::INVALID_METADATA;
      }

      if (got_end_row && strcmp(keyComps.rowKey, range_loc_info.end_row.c_str())) {
	if (got_start_row && got_location)
	  m_cache.insert(0, range_loc_info);
	else {
	  LOG_VA_ERROR("Incomplete METADATA record found in root tablet under row key '%s'", range_loc_info.end_row.c_str());
	}
	range_loc_info.start_row = "";
	range_loc_info.end_row = "";
	range_loc_info.location = "";
	got_start_row = false;
	got_end_row = false;
	got_location = false;
      }

      if (!got_end_row) {
	range_loc_info.end_row = keyComps.rowKey;
	got_end_row = true;
      }

      if (keyComps.columnFamily == m_startrow_cid) {
	range_loc_info.start_row = std::string((const char *)value->data, value->len);
	got_start_row = true;
      }
      else if (keyComps.columnFamily == m_location_cid) {
	range_loc_info.location = std::string((const char *)value->data, value->len);
	got_location = true;
      }
      else {
	LOG_VA_ERROR("METADATA lookup on row '%s' returned incorrect column (id=%d)", (const char *)key->data, keyComps.columnFamily);
      }
    }

    if (got_start_row && got_end_row && got_location)
      m_cache.insert(0, range_loc_info);
    else if (got_end_row) {
      LOG_VA_ERROR("Incomplete METADATA record found in root tablet under row key '%s'", range_loc_info.end_row.c_str());
    }

    if (!scan_block.eos()) {
      // TODO: m_range_server.destroy_scanner(addr, scan_block.get_scanner_id(), 0);
    }

    if (!m_cache.lookup(0, start_row.c_str(), range_loc_info_p))
      return Error::METADATA_NOT_FOUND;

    if (table->id == 0 || level == 1)
      return Error::OK;

    range.startRow = range_loc_info_p->start_row.c_str();
    range.endRow   = range_loc_info_p->end_row.c_str();
    if (!LocationCache::location_to_addr(range_loc_info_p->location.c_str(), addr)) {
      LOG_VA_ERROR("Invalid location found in METADATA entry for row '%s' - %s", start_row.c_str(), range_loc_info_p->location.c_str());
      return Error::INVALID_METADATA;
    }
  }

  return Error::METADATA_NOT_FOUND;
}


/**
 * 
 */
int RangeLocator::read_root_location() {
  int error;
  DynamicBuffer value(0);
  std::string addrStr;
  char *ptr;
  RangeLocationInfo range_loc_info;
  char buf[8];

  if ((error = m_hyperspace_ptr->attr_get(m_root_file_handle, "location", value)) != Error::OK) {
    LOG_VA_ERROR("Problem reading 'location' attribute of Hyperspace file /hypertable/root - %s", Error::get_text(error));
    return error;
  }

  range_loc_info.start_row  = "0:";
  range_loc_info.end_row    = build_metadata_end_row_key(buf, "%d:", 0);
  range_loc_info.location   = (const char *)value.buf;
  
  m_cache.insert(0, range_loc_info, true);

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


/**
 * 
 */
const char *RangeLocator::build_metadata_start_row_key(char *buf, const char *format, uint32_t table_id, const char *row_key) {
  sprintf(buf, format, table_id);
  if (row_key)
    strcat(buf, row_key);
  return buf;
}

/**
 * 
 */
const char *RangeLocator::build_metadata_end_row_key(char *buf, const char *format, uint32_t table_id) {
  char *ptr;
  sprintf(buf, format, table_id);
  ptr = buf + strlen(buf);
  *ptr++ = 0xff;
  *ptr++ = 0xff;
  *ptr = 0;
  return buf;
}


