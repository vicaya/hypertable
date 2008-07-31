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

#include "Common/Compat.h"
#include <cassert>
#include <cstdlib>
#include <cstring>

extern "C" {
#include <limits.h>
#include <poll.h>
}

#include "Common/Error.h"

#include "Hyperspace/Session.h"

#include "Defaults.h"
#include "Key.h"
#include "RootFileHandler.h"
#include "RangeLocator.h"
#include "ScanBlock.h"
#include "ScanSpec.h"

using namespace Hypertable;

namespace {
  const uint32_t METADATA_READAHEAD_COUNT = 10;
  const uint32_t MAX_ERROR_QUEUE_LENGTH = 4;

  class MetaKeyBuilder {
  public:
    MetaKeyBuilder() : start(0), end(0) { return; }
    void
    build_keys(const char *format, uint32_t table_id, const char *row_key) {
      char *ptr;
      if (row_key) {
        start = new char [16 + strlen(row_key) + 1];
        sprintf(start, format, table_id);
        strcat(start, row_key);
      }
      else {
        start = new char [16];
        sprintf(start, format, table_id);
      }
      end = new char [16];
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


/**
 *
 */
RangeLocator::RangeLocator(PropertiesPtr &props_ptr,
                           ConnectionManagerPtr &conn_mgr,
                           Hyperspace::SessionPtr &hyperspace)
    : m_conn_manager_ptr(conn_mgr), m_hyperspace_ptr(hyperspace),
      m_root_stale(true), m_range_server(conn_mgr->get_comm(),
      HYPERTABLE_RANGESERVER_CLIENT_TIMEOUT) {
  time_t client_timeout;

  if ((client_timeout = props_ptr->get_int("Hypertable.Request.Timeout", 0)) != 0)
    m_range_server.set_default_timeout(client_timeout);

  int cache_size = props_ptr->get_int("Hypertable.LocationCache.MaxEntries", HYPERTABLE_LOCATIONCACHE_MAXENTRIES);
  m_cache_ptr = new LocationCache(cache_size);

  initialize();
}


/**
 *
 */
RangeLocator::RangeLocator(PropertiesPtr &props_ptr, Comm *comm,
                           Hyperspace::SessionPtr &hyperspace)
    : m_conn_manager_ptr(0), m_hyperspace_ptr(hyperspace), m_root_stale(true),
      m_range_server(comm, HYPERTABLE_RANGESERVER_CLIENT_TIMEOUT) {
  time_t client_timeout;

  if ((client_timeout = props_ptr->get_int("Hypertable.Request.Timeout", 0)) != 0)
    m_range_server.set_default_timeout(client_timeout);

  int cache_size = props_ptr->get_int("Hypertable.LocationCache.MaxEntries", HYPERTABLE_LOCATIONCACHE_MAXENTRIES);
  m_cache_ptr = new LocationCache(cache_size);

  initialize();
}


void RangeLocator::initialize() {
  int error;
  DynamicBuffer valbuf(0);
  HandleCallbackPtr null_handle_callback;
  uint64_t handle;
  Schema *schema = 0;

  m_root_handler_ptr = new RootFileHandler(this);

  if ((error = m_hyperspace_ptr->open("/hypertable/root", OPEN_FLAG_READ, m_root_handler_ptr, &m_root_file_handle)) != Error::OK) {
    HT_ERRORF("Unable to open Hyperspace file '/hypertable/root' (%s)", Error::get_text(error));
    HT_THROW(error, "");
  }

  if ((error = m_hyperspace_ptr->open("/hypertable/tables/METADATA", OPEN_FLAG_READ, null_handle_callback, &handle)) != Error::OK) {
    HT_ERRORF("Unable to open Hyperspace file '/hypertable/tables/METADATA' (%s)", Error::get_text(error));
    HT_THROW(error, "");
  }

  if ((error = m_hyperspace_ptr->attr_get(handle, "schema", valbuf)) != Error::OK) {
    HT_ERROR("Problem getting 'schema' attribute from METADATA hyperspace file");
    HT_THROW(error, "");
  }

  m_hyperspace_ptr->close(handle);

  schema = Schema::new_instance((const char *)valbuf.base, valbuf.fill(), true);
  if (!schema->is_valid()) {
    delete schema;
    HT_ERRORF("Schema Parse Error for table METADATA : %s", schema->get_error_string());
    HT_THROW(Error::RANGESERVER_SCHEMA_PARSE_ERROR, "");
  }

  m_metadata_schema_ptr = schema;

  m_metadata_table.name = "METADATA";
  m_metadata_table.id = 0;
  m_metadata_table.generation = schema->get_generation();

  Schema::ColumnFamily *cf;

  if ((cf = schema->get_column_family("StartRow")) == 0) {
    HT_ERROR("Unable to find column family 'StartRow' in METADATA schema");
    HT_THROW(Error::BAD_SCHEMA, "");
  }
  m_startrow_cid = cf->id;

  if ((cf = schema->get_column_family("Location")) == 0) {
    HT_ERROR("Unable to find column family 'Location' in METADATA schema");
    HT_THROW(Error::BAD_SCHEMA, "");
  }
  m_location_cid = cf->id;
}


RangeLocator::~RangeLocator() {
  m_hyperspace_ptr->close(m_root_file_handle);
}



void
RangeLocator::find_loop(TableIdentifier *table, const char *row_key,
    RangeLocationInfo *rane_loc_infop, Timer &timer, bool hard) {
  int error;
  double wait_time = 1.0;
  double total_wait_time = 0.0;

  error = find(table, row_key, rane_loc_infop, timer, hard);

  if (error == Error::TABLE_DOES_NOT_EXIST) {
    boost::mutex::scoped_lock lock(m_mutex);
    m_last_errors.clear();
    HT_THROW(error, (std::string)"Table '" + table->name + "' is being dropped.");
  }

  while (error != Error::OK) {

    // check for timer expiration
    if (timer.remaining() < wait_time) {
      boost::mutex::scoped_lock lock(m_mutex);
      for (std::deque<std::string>::iterator iter = m_last_errors.begin(); iter != m_last_errors.end(); iter++)
        HT_ERRORF("%s", (*iter).c_str());
      m_last_errors.clear();
      HT_THROW(Error::REQUEST_TIMEOUT, (String)"Locating range for row = '" + row_key + "'");
    }

    // wait a bit
    poll(0, 0, (int)(wait_time*1000.0));
    total_wait_time += wait_time;
    wait_time *= 1.5;

    // try again
    if ((error = find(table, row_key, rane_loc_infop, timer, true)) == Error::TABLE_DOES_NOT_EXIST) {
      boost::mutex::scoped_lock lock(m_mutex);
      m_last_errors.clear();
      HT_THROW(error, (std::string)"Table '" + table->name + "' is being dropped.");
    }
  }

  {
    boost::mutex::scoped_lock lock(m_mutex);
    m_last_errors.clear();
  }
}



int
RangeLocator::find(TableIdentifier *table, const char *row_key,
    RangeLocationInfo *rane_loc_infop, Timer &timer, bool hard) {
  RangeSpec range;
  ScanSpec meta_scan_spec;
  ScanBlock scan_block;
  int error;
  Key key;
  RangeLocationInfo range_loc_info;
  std::string start_row;
  std::string end_row;
  struct sockaddr_in addr;
  RowInterval ri;
  bool inclusive = (row_key == 0 || *row_key == 0) ? true : false;

  if (m_root_stale) {
    if ((error = read_root_location(timer)) != Error::OK)
      return error;
  }

  if (!hard && m_cache_ptr->lookup(table->id, row_key, rane_loc_infop))
    return Error::OK;

  /**
   * If key is on root METADATA range, return root range information
   */
  if (table->id == 0 && (row_key == 0 || strcmp(row_key, Key::END_ROOT_ROW) < 0)) {
    rane_loc_infop->start_row = "";
    rane_loc_infop->end_row = Key::END_ROOT_ROW;
    rane_loc_infop->location = m_root_range_info.location;
    return Error::OK;
  }

  /** at this point, we didn't find it so we need to do a METADATA lookup **/

  range.start_row = 0;
  range.end_row = Key::END_ROOT_ROW;
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
  if (hard ||
      !m_cache_ptr->lookup(0, meta_key_ptr, rane_loc_infop, inclusive)) {

    meta_scan_spec.row_limit = METADATA_READAHEAD_COUNT;
    meta_scan_spec.max_versions = 1;
    meta_scan_spec.columns.push_back("StartRow");
    meta_scan_spec.columns.push_back("Location");

    ri.start = meta_keys.start;
    ri.start_inclusive = true;
    ri.end = 0;
    ri.end_inclusive = false;
    meta_scan_spec.row_intervals.push_back(ri);

    meta_scan_spec.return_deletes = false;
    // meta_scan_spec.interval = ????;

    try {
      m_range_server.set_timeout((time_t)(timer.remaining() + 0.5));
      m_range_server.create_scanner(addr, m_metadata_table, range, meta_scan_spec, scan_block);
    }
    catch (Exception &e) {
      return e.code();
    }

    if ((error = process_metadata_scanblock(scan_block)) != Error::OK) {
      m_range_server.destroy_scanner(addr, scan_block.get_scanner_id(), 0);
      return error;
    }

    if (!scan_block.eos()) {
      m_range_server.destroy_scanner(addr, scan_block.get_scanner_id(), 0);
    }

    if (!m_cache_ptr->lookup(0, meta_key_ptr, rane_loc_infop, inclusive)) {
      HT_ERRORF("Unable to find metadata for row '%s' row_key=%s",
                meta_keys.start, row_key);
      return Error::METADATA_NOT_FOUND;
    }
  }

  if (table->id == 0)
    return Error::OK;

  /**
   * Find actual range from second-level METADATA range
   */

  range.start_row = rane_loc_infop->start_row.c_str();
  range.end_row   = rane_loc_infop->end_row.c_str();

  if (!LocationCache::location_to_addr(
      rane_loc_infop->location.c_str(), addr)) {
    HT_ERRORF("Invalid location found in METADATA entry for row '%s' - %s",
              start_row.c_str(), rane_loc_infop->location.c_str());
    return Error::INVALID_METADATA;
  }

  meta_scan_spec.clear();

  meta_scan_spec.row_limit = METADATA_READAHEAD_COUNT;
  meta_scan_spec.max_versions = 1;
  meta_scan_spec.columns.push_back("StartRow");
  meta_scan_spec.columns.push_back("Location");

  ri.start = meta_keys.start+2;
  ri.start_inclusive = true;
  ri.end = meta_keys.end+2;;
  ri.end_inclusive = true;
  meta_scan_spec.row_intervals.push_back(ri);

  // meta_scan_spec.interval = ????;

  if (m_conn_manager_ptr &&
      !m_conn_manager_ptr->wait_for_connection(addr, (time_t)(timer.remaining() + 0.5))) {
    if (timer.expired())
      HT_THROW(Error::REQUEST_TIMEOUT, "");
  }

  try {
    m_range_server.set_timeout((time_t)(timer.remaining() + 0.5));
    m_range_server.create_scanner(addr, m_metadata_table, range,
                                  meta_scan_spec, scan_block);
  }
  catch (Exception &e) {
    return e.code();
  }

  if ((error = process_metadata_scanblock(scan_block)) != Error::OK) {
    m_range_server.destroy_scanner(addr, scan_block.get_scanner_id(), 0);
    return error;
  }

  if (!scan_block.eos()) {
    m_range_server.destroy_scanner(addr, scan_block.get_scanner_id(), 0);
  }

  if (row_key == 0)
    row_key = "";

  if (!m_cache_ptr->lookup(table->id, row_key, rane_loc_infop, inclusive)) {
    boost::mutex::scoped_lock lock(m_mutex);
    m_last_errors.push_back((std::string)"RangeLocator failed to find metadata for table '" + table->name + "' row '" + row_key + "'");
    while (m_last_errors.size() > MAX_ERROR_QUEUE_LENGTH)
      m_last_errors.pop_front();
    return Error::METADATA_NOT_FOUND;
  }

  return Error::OK;
}


/**
 *
 */
int RangeLocator::process_metadata_scanblock(ScanBlock &scan_block) {
  RangeLocationInfo range_loc_info;
  ByteString bskey;
  ByteString value;
  Key key;
  const char *stripped_key;
  uint32_t table_id = 0;
  struct sockaddr_in addr;

  range_loc_info.start_row = "";
  range_loc_info.end_row = "";
  range_loc_info.location = "";

  bool got_start_row = false;
  bool got_end_row = false;
  bool got_location = false;

  while (scan_block.next(bskey, value)) {

    if (!key.load(bskey)) {
      HT_ERRORF("METADATA lookup for '%s' returned bad key", bskey.str());
      return Error::INVALID_METADATA;
    }

    if ((stripped_key = strchr(key.row, ':')) == 0) {
      HT_ERRORF("Bad row key found in METADATA - '%s'", key.row);
      return Error::INVALID_METADATA;
    }
    stripped_key++;

    if (got_end_row) {
      if (strcmp(stripped_key, range_loc_info.end_row.c_str())) {
        if (got_start_row && got_location) {

          /**
           * Add this location (address) to the connection manager
           */
          if (!LocationCache::location_to_addr(
              range_loc_info.location.c_str(), addr)) {
            HT_ERRORF("Invalid location found in METADATA entry for row '%s' "
                      "- %s", range_loc_info.end_row.c_str(),
                      range_loc_info.location.c_str());
            return Error::INVALID_METADATA;
          }
          if (m_conn_manager_ptr)
            m_conn_manager_ptr->add(addr, 300, "RangeServer");

          m_cache_ptr->insert(table_id, range_loc_info);
          //cout << "(1) cache insert table=" << table_id << " start=" << range_loc_info.start_row << " end=" << range_loc_info.end_row << " loc=" << range_loc_info.location << endl;
        }
        else {
          boost::mutex::scoped_lock lock(m_mutex);
          m_last_errors.push_back(format("Incomplete METADATA record found in "
              "root range under row key '%s'", range_loc_info.end_row.c_str()));
          while (m_last_errors.size() > MAX_ERROR_QUEUE_LENGTH)
            m_last_errors.pop_front();
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
      table_id = (uint32_t)strtol(key.row, 0, 10);
      range_loc_info.end_row = stripped_key;
      got_end_row = true;
    }

    if (key.column_family_code == m_startrow_cid) {
      const uint8_t *str;
      size_t len = value.decode_length(&str);
      //cout << "TS=" << key.timestamp << endl;
      range_loc_info.start_row = std::string((const char *)str, len);
      got_start_row = true;
    }
    else if (key.column_family_code == m_location_cid) {
      const uint8_t *str;
      size_t len = value.decode_length(&str);
      range_loc_info.location = std::string((const char *)str, len);
      if (range_loc_info.location == "!")
        return Error::TABLE_DOES_NOT_EXIST;
      got_location = true;
    }
    else {
      HT_ERRORF("METADATA lookup on row '%s' returned incorrect column (id=%d)",
                bskey.str(), key.column_family_code);
    }
  }

  if (got_start_row && got_end_row && got_location) {

    /**
     * Add this location (address) to the connection manager
     */
    if (!LocationCache::location_to_addr(
        range_loc_info.location.c_str(), addr)) {
      HT_ERRORF("Invalid location found in METADATA entry for row '%s' - %s",
          range_loc_info.end_row.c_str(), range_loc_info.location.c_str());
      return Error::INVALID_METADATA;
    }
    if (m_conn_manager_ptr)
      m_conn_manager_ptr->add(addr, 300, "RangeServer");

    m_cache_ptr->insert(table_id, range_loc_info);

    //cout << "(2) cache insert table=" << table_id << " start=" << range_loc_info.start_row << " end=" << range_loc_info.end_row << " loc=" << range_loc_info.location << endl;
  }
  else if (got_end_row) {
    HT_ERRORF("Incomplete METADATA record found in root tablet under row key "
              "'%s'", range_loc_info.end_row.c_str());
  }

  return Error::OK;
}




/**
 *
 */
int RangeLocator::read_root_location(Timer &timer) {
  int error;
  DynamicBuffer value(0);
  std::string addr_str;

  if ((error = m_hyperspace_ptr->attr_get(m_root_file_handle, "Location", value)) != Error::OK) {
    HT_ERRORF("Problem reading 'Location' attribute of Hyperspace file /hypertable/root - %s", Error::get_text(error));
    return error;
  }

  m_root_range_info.start_row  = "";
  m_root_range_info.end_row    = Key::END_ROOT_ROW;
  m_root_range_info.location   = (const char *)value.base;

  m_cache_ptr->insert(0, m_root_range_info, true);

  if (!LocationCache::location_to_addr((const char *)value.base, m_root_addr)) {
    HT_ERROR("Bad format of 'Location' attribute of /hypertable/root Hyperspace file");
    return Error::BAD_ROOT_LOCATION;
  }

  if (m_conn_manager_ptr) {

    m_conn_manager_ptr->add(m_root_addr, 8, "Root RangeServer");

    if (!m_conn_manager_ptr->wait_for_connection(m_root_addr, (time_t)(timer.remaining() + 0.5))) {
      std::string addr_str;
      HT_ERRORF("Timeout (20s) waiting for root RangeServer connection - %s",
                   InetAddr::string_format(addr_str, m_root_addr));
    }
  }

  m_root_stale = false;

  return Error::OK;
}
