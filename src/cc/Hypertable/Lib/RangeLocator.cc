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
#include <cstdlib>
#include <cstring>

extern "C" {
#include <limits.h>
#include <poll.h>
}

#include "Common/Error.h"

#include "Hyperspace/Session.h"

#include "Key.h"
#include "RootFileHandler.h"
#include "RangeLocator.h"
#include "ScanBlock.h"

namespace {
  const uint32_t METADATA_READAHEAD_COUNT = 10;
}


/**
 * 
 */
RangeLocator::RangeLocator(ConnectionManagerPtr &connManagerPtr, Hyperspace::SessionPtr &hyperspacePtr) : m_conn_manager_ptr(connManagerPtr), m_hyperspace_ptr(hyperspacePtr), m_cache(1000), m_root_stale(true), m_range_server(connManagerPtr->get_comm(), 300) {
  initialize();
}


/**
 * 
 */
RangeLocator::RangeLocator(Comm *comm, Hyperspace::SessionPtr &hyperspacePtr) : m_conn_manager_ptr(0), m_hyperspace_ptr(hyperspacePtr), m_cache(1000), m_root_stale(true), m_range_server(comm, 30) {
  initialize();
}


void RangeLocator::initialize() {
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



int RangeLocator::find(TableIdentifierT *table, const char *row_key, RangeLocationInfo *range_loc_info_p, int timeout) {
  int error;
  float wait_time = 1.0;
  float total_wait_time = 0.0;

  error = find(table, row_key, range_loc_info_p, true);

  // retry loop
  while (error != Error::OK && total_wait_time < (float)timeout) {
    // wait a bit
    poll(0, 0, (int)(wait_time*1000.0));
    total_wait_time += wait_time;
    wait_time *= 1.5;
    // try again, the hard way
    error = find(table, row_key, range_loc_info_p, true);
  }

  return error;
}

namespace {
  class MetaKeyBuilder {
  public:
    MetaKeyBuilder() : start(0), end(0) { return; }
    void build_keys(const char *format, uint32_t table_id, const char *row_key) {
      char *ptr;
      if (row_key) {
	start = new char [ 16 + strlen(row_key) + 1 ];
	sprintf(start, format, table_id);
	strcat(start, row_key);
      }
      else {
	start = new char [ 16 ];
	sprintf(start, format, table_id);
      }
      end = new char [ 16 ];
      sprintf(end, format, table_id);
      ptr = end + strlen(end);
      *ptr++ = (char)0xff;
      *ptr++ = (char)0xff;
      *ptr = 0;
      
    }
    ~MetaKeyBuilder() {
      delete [] start;
      delete [] end;
    }
    char *start;
    char *end;
  };
}


int RangeLocator::find(TableIdentifierT *table, const char *row_key, RangeLocationInfo *range_loc_info_p, bool hard) {
  RangeT range;
  ScanSpecificationT meta_scan_spec;
  ScanBlock scan_block;
  int error;
  Key keyComps;
  RangeLocationInfo range_loc_info;
  std::string start_row;
  std::string end_row;
  struct sockaddr_in addr;
  bool inclusive = (row_key == 0 || *row_key == 0) ? true : false;

  if (m_root_stale) {
    if ((error = read_root_location()) != Error::OK)
      return error;
  }

  if (!hard && m_cache.lookup(table->id, row_key, range_loc_info_p))
    return Error::OK;

  /**
   * If key is on root METADATA range, return root range information
   */
  if (table->id == 0 && (row_key == 0 || strcmp(row_key, Key::END_ROOT_ROW) < 0)) {
    range_loc_info_p->start_row = "";
    range_loc_info_p->end_row = Key::END_ROOT_ROW;
    range_loc_info_p->location = m_root_range_info.location;
    return Error::OK;
  }

  /** at this point, we didn't find it so we need to do a METADATA lookup **/

  range.startRow = 0;
  range.endRow = Key::END_ROOT_ROW;
  memcpy(&addr, &m_root_addr, sizeof(struct sockaddr_in));

  MetaKeyBuilder meta_keys;
  char *meta_key_ptr;

  if (table->id == 0)
    meta_keys.build_keys("%d:", 0, row_key);
  else
    meta_keys.build_keys("0:%d:", table->id, row_key);


  /**
   * Find second level METADATA range from root
   */
  meta_key_ptr = meta_keys.start+2;
  if (hard || !m_cache.lookup(0, meta_key_ptr, range_loc_info_p, inclusive)) {
    meta_scan_spec.rowLimit = METADATA_READAHEAD_COUNT;
    meta_scan_spec.max_versions = 1;
    meta_scan_spec.columns.clear();
    meta_scan_spec.columns.push_back("StartRow");
    meta_scan_spec.columns.push_back("Location");
    meta_scan_spec.startRow = meta_keys.start;
    meta_scan_spec.startRowInclusive = true;
    meta_scan_spec.endRow = 0;
    meta_scan_spec.endRowInclusive = false;
    // meta_scan_spec.interval = ????;

    if ((error = m_range_server.create_scanner(addr, m_metadata_table, range, meta_scan_spec, scan_block)) != Error::OK)
      return error;

    if ((error = process_metadata_scanblock(scan_block)) != Error::OK) {
      // TODO: m_range_server.destroy_scanner(addr, scan_block.get_scanner_id(), 0);
      return error;
    }

    if (!scan_block.eos()) {
      // TODO: m_range_server.destroy_scanner(addr, scan_block.get_scanner_id(), 0);
    }

    if (!m_cache.lookup(0, meta_key_ptr, range_loc_info_p, inclusive)) {
      LOG_VA_ERROR("Unable to find metadata for row '%s'", meta_keys.start);
      return Error::METADATA_NOT_FOUND;
    }
  }

  if (table->id == 0)
    return Error::OK;

  /**
   * Find actual range from second-level METADATA range
   */

  range.startRow = range_loc_info_p->start_row.c_str();
  range.endRow   = range_loc_info_p->end_row.c_str();

  if (!LocationCache::location_to_addr(range_loc_info_p->location.c_str(), addr)) {
    LOG_VA_ERROR("Invalid location found in METADATA entry for row '%s' - %s", start_row.c_str(), range_loc_info_p->location.c_str());
    return Error::INVALID_METADATA;
  }

  meta_scan_spec.rowLimit = METADATA_READAHEAD_COUNT;
  meta_scan_spec.max_versions = 1;
  meta_scan_spec.columns.clear();
  meta_scan_spec.columns.push_back("StartRow");
  meta_scan_spec.columns.push_back("Location");
  meta_scan_spec.startRow = meta_keys.start+2;
  meta_scan_spec.startRowInclusive = true;
  meta_scan_spec.endRow = meta_keys.end+2;
  meta_scan_spec.endRowInclusive = true;
  // meta_scan_spec.interval = ????;

  if (m_conn_manager_ptr)
    m_conn_manager_ptr->wait_for_connection(addr, 12);

  if ((error = m_range_server.create_scanner(addr, m_metadata_table, range, meta_scan_spec, scan_block)) != Error::OK)
    return error;

  if ((error = process_metadata_scanblock(scan_block)) != Error::OK) {
    // TODO: m_range_server.destroy_scanner(addr, scan_block.get_scanner_id(), 0);
    return error;
  }

  if (!scan_block.eos()) {
    // TODO: m_range_server.destroy_scanner(addr, scan_block.get_scanner_id(), 0);
  }

  if (row_key == 0)
    row_key = "";

  if (!m_cache.lookup(table->id, row_key, range_loc_info_p, inclusive)) {
    LOG_VA_ERROR("RangeLocator failed to find metadata for table '%s' row '%s'", table->name, row_key);
    return Error::METADATA_NOT_FOUND;
  }

  return Error::OK;
}


/**
 *
 */
int RangeLocator::process_metadata_scanblock(ScanBlock &scan_block) {
  RangeLocationInfo range_loc_info;
  const ByteString32T *key;
  const ByteString32T *value;
  Key keyComps;
  const char *stripped_key;
  uint32_t table_id = 0;
  struct sockaddr_in addr;
  
  range_loc_info.start_row = "";
  range_loc_info.end_row = "";
  range_loc_info.location = "";

  bool got_start_row = false;
  bool got_end_row = false;
  bool got_location = false;

  while (scan_block.next(key, value)) {

    if (!keyComps.load(key)) {
      LOG_VA_ERROR("METADATA lookup for '%s' returned bad key", (const char *)key->data);
      return Error::INVALID_METADATA;
    }

    if ((stripped_key = strchr(keyComps.row, ':')) == 0) {
      LOG_VA_ERROR("Bad row key found in METADATA - '%s'", keyComps.row);
      return Error::INVALID_METADATA;
    }
    stripped_key++;

    if (got_end_row) {
      if (strcmp(stripped_key, range_loc_info.end_row.c_str())) {
	if (got_start_row && got_location) {

	  /**
	   * Add this location (address) to the connection manager
	   */
	  if (!LocationCache::location_to_addr(range_loc_info.location.c_str(), addr)) {
	    LOG_VA_ERROR("Invalid location found in METADATA entry for row '%s' - %s", range_loc_info.end_row.c_str(), range_loc_info.location.c_str());
	    return Error::INVALID_METADATA;
	  }
	  if (m_conn_manager_ptr)
	    m_conn_manager_ptr->add(addr, 300, "RangeServer");

	  m_cache.insert(table_id, range_loc_info);
	  //cout << "(1) cache insert table=" << table_id << " start=" << range_loc_info.start_row << " end=" << range_loc_info.end_row << " loc=" << range_loc_info.location << endl;
	}
	else {
	  LOG_VA_ERROR("Incomplete METADATA record found in root tablet under row key '%s'", range_loc_info.end_row.c_str());
	  //DUMP_CORE;
	}
	range_loc_info.start_row = "";
	range_loc_info.end_row = "";
	range_loc_info.location = "";
	got_start_row = false;
	got_end_row = false;
	got_location = false;
      }
    }
    else {
      table_id = (uint32_t)strtol(keyComps.row, 0, 10);
      range_loc_info.end_row = stripped_key;
      got_end_row = true;
    }

    if (keyComps.column_family_code == m_startrow_cid) {
      //cout << "TS=" << keyComps.timestamp << endl;
      range_loc_info.start_row = std::string((const char *)value->data, value->len);
      got_start_row = true;
    }
    else if (keyComps.column_family_code == m_location_cid) {
      range_loc_info.location = std::string((const char *)value->data, value->len);
      got_location = true;
    }
    else {
      LOG_VA_ERROR("METADATA lookup on row '%s' returned incorrect column (id=%d)", (const char *)key->data, keyComps.column_family_code);
    }
  }

  if (got_start_row && got_end_row && got_location) {
    m_cache.insert(table_id, range_loc_info);
    //cout << "(2) cache insert table=" << table_id << " start=" << range_loc_info.start_row << " end=" << range_loc_info.end_row << " loc=" << range_loc_info.location << endl;
  }
  else if (got_end_row) {
    LOG_VA_ERROR("Incomplete METADATA record found in root tablet under row key '%s'", range_loc_info.end_row.c_str());
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

  if ((error = m_hyperspace_ptr->attr_get(m_root_file_handle, "location", value)) != Error::OK) {
    LOG_VA_ERROR("Problem reading 'location' attribute of Hyperspace file /hypertable/root - %s", Error::get_text(error));
    return error;
  }

  m_root_range_info.start_row  = "";
  m_root_range_info.end_row    = Key::END_ROOT_ROW;
  m_root_range_info.location   = (const char *)value.buf;
  
  m_cache.insert(0, m_root_range_info, true);

  if (!LocationCache::location_to_addr((const char *)value.buf, m_root_addr)) {
    LOG_ERROR("Bad format of 'location' attribute of /hypertable/root Hyperspace file");
    return Error::BAD_ROOT_LOCATION;    
  }

  if (m_conn_manager_ptr) {

    m_conn_manager_ptr->add(m_root_addr, 8, "Root RangeServer");

    if (!m_conn_manager_ptr->wait_for_connection(m_root_addr, 20)) {
      std::string addr_str;
      LOG_VA_ERROR("Timeout (20s) waiting for root RangeServer connection - %s",
		   InetAddr::string_format(addr_str, m_root_addr));
    }
  }

  m_root_stale = false;

  return Error::OK;
}
