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

// convenient local macros to record errors for debugging
#define SAVE_ERR(_code_, _msg_) \
  do { \
    ScopedLock lock(m_mutex); \
    m_last_errors.push_back(HT_EXCEPTION(_code_, _msg_)); \
    while (m_last_errors.size() > MAX_ERROR_QUEUE_LENGTH) \
      m_last_errors.pop_front(); \
  } while (false)

#define SAVE_ERR2(_code_, _ex_, _msg_) \
  do { \
    ScopedLock lock(m_mutex); \
    m_last_errors.push_back(HT_EXCEPTION2(_code_, _ex_, _msg_)); \
    while (m_last_errors.size() > MAX_ERROR_QUEUE_LENGTH) \
      m_last_errors.pop_front(); \
  } while (false)

using namespace Hypertable;

namespace {
  const uint32_t METADATA_READAHEAD_COUNT = 10;
  const uint32_t MAX_ERROR_QUEUE_LENGTH = 4;
  const uint32_t METADATA_RETRY_INTERVAL = 30000;
  const uint32_t ROOT_METADATA_RETRY_INTERVAL = 3000;

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


RangeLocator::RangeLocator(PropertiesPtr &cfg, ConnectionManagerPtr &conn_mgr,
    Hyperspace::SessionPtr &hyperspace, uint32_t timeout_ms)
  : m_conn_manager(conn_mgr), m_hyperspace(hyperspace),
    m_root_stale(true), m_range_server(conn_mgr->get_comm(), timeout_ms),
    m_hyperspace_init(false), m_hyperspace_connected(true), m_timeout_ms(timeout_ms) {

  int cache_size = cfg->get_i64("Hypertable.LocationCache.MaxEntries");

  m_cache = new LocationCache(cache_size);
  // register hyperspace session callback
  m_hyperspace_session_callback.m_rangelocator = this;
  m_hyperspace->add_callback(&m_hyperspace_session_callback);
  // no need to serialize access in ctor
  initialize();
}

void RangeLocator::hyperspace_disconnected()
{
  ScopedLock lock(m_hyperspace_mutex);
  m_hyperspace_init = false;
  m_hyperspace_connected = false;
}

void RangeLocator::hyperspace_reconnected()
{
  ScopedLock lock(m_hyperspace_mutex);
  HT_ASSERT(!m_hyperspace_init);
  m_hyperspace_connected = true;
}

/**
 * Assumes access is serialized via m_hyperspace_mutex
 */
void RangeLocator::initialize() {
  DynamicBuffer valbuf(0);
  HandleCallbackPtr null_handle_callback;
  uint64_t handle;
  Timer timer(m_timeout_ms, true);

  if (m_hyperspace_init)
    return;
  HT_ASSERT(m_hyperspace_connected);

  m_root_handler = new RootFileHandler(this);

  m_root_file_handle = m_hyperspace->open("/hypertable/root",
                                          OPEN_FLAG_READ, m_root_handler);

  while (true) {
    try {
      handle = m_hyperspace->open("/hypertable/tables/METADATA",
                                  OPEN_FLAG_READ, null_handle_callback);
      break;
    }
    catch (Exception &e) {
      if (timer.expired())
        HT_THROW2(Error::HYPERSPACE_FILE_NOT_FOUND, e,
                  "/hypertable/tables/METADATA");
      poll(0, 0, 3000);
    }
  }

  m_hyperspace->attr_get(handle, "schema", valbuf);

  m_hyperspace->close(handle);

  SchemaPtr schema = Schema::new_instance((char *)valbuf.base, valbuf.fill(),
                                          true);
  if (!schema->is_valid()) {
    HT_ERRORF("Schema Parse Error for table METADATA : %s",
              schema->get_error_string());
    HT_THROW_(Error::RANGESERVER_SCHEMA_PARSE_ERROR);
  }

  m_metadata_schema = schema;

  m_metadata_table.name = "METADATA";
  m_metadata_table.id = 0;
  m_metadata_table.generation = schema->get_generation();

  Schema::ColumnFamily *cf;

  if ((cf = schema->get_column_family("StartRow")) == 0) {
    HT_ERROR("Unable to find column family 'StartRow' in METADATA schema");
    HT_THROW_(Error::BAD_SCHEMA);
  }
  m_startrow_cid = cf->id;

  if ((cf = schema->get_column_family("Location")) == 0) {
    HT_ERROR("Unable to find column family 'Location' in METADATA schema");
    HT_THROW_(Error::BAD_SCHEMA);
  }
  m_location_cid = cf->id;
  m_hyperspace_init = true;
}


RangeLocator::~RangeLocator() {
  m_hyperspace->close(m_root_file_handle);
  m_hyperspace->remove_callback(&m_hyperspace_session_callback);
}


void
RangeLocator::find_loop(const TableIdentifier *table, const char *row_key,
    RangeLocationInfo *rane_loc_infop, Timer &timer, bool hard) {
  int error;
  uint32_t wait_time = 1000;
  uint32_t total_wait_time = 0;

  error = find(table, row_key, rane_loc_infop, timer, hard);

  if (error == Error::TABLE_NOT_FOUND) {
    ScopedLock lock(m_mutex);
    clear_error_history();
    HT_THROWF(error, "Table '%s' is (being) dropped", table->name);
  }

  while (error != Error::OK) {

    // check for timer expiration
    if (timer.remaining() < wait_time) {
      dump_error_history();
      HT_THROWF(Error::REQUEST_TIMEOUT, "Locating range for row='%s'", row_key);
    }

    // wait a bit
    poll(0, 0, (int)wait_time);
    total_wait_time += wait_time;
    wait_time = (wait_time * 3) / 2;

    // try again
    if ((error = find(table, row_key, rane_loc_infop, timer, true))
        == Error::TABLE_NOT_FOUND) {
      ScopedLock lock(m_mutex);
      clear_error_history();
      HT_THROWF(error, "Table '%s' is (being) dropped", table->name);
    }
  }

  clear_error_history();
}


int
RangeLocator::find(const TableIdentifier *table, const char *row_key,
    RangeLocationInfo *rane_loc_infop, Timer &timer, bool hard) {
  RangeSpec range;
  ScanSpec meta_scan_spec;
  ScanBlock scan_block;
  int error;
  Key key;
  RangeLocationInfo range_loc_info;
  String start_row;
  String end_row;
  struct sockaddr_in addr;
  RowInterval ri;
  bool inclusive = (row_key == 0 || *row_key == 0) ? true : false;

  if (m_root_stale) {
    if ((error = read_root_location(timer)) != Error::OK)
      return error;
  }

  if (!hard && m_cache->lookup(table->id, row_key, rane_loc_infop))
    return Error::OK;

  /**
   * If key is on root METADATA range, return root range information
   */
  if (table->id == 0 && (row_key == 0
      || strcmp(row_key, Key::END_ROOT_ROW) < 0)) {
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
  char *meta_key;

  if (table->id == 0)
    meta_keys.build_keys("%d:", 0, row_key);
  else
    meta_keys.build_keys("0:%d:", table->id, row_key);

  /**
   * Find second level METADATA range from root
   */
  meta_key = meta_keys.start+2;
  if (hard ||
      !m_cache->lookup(0, meta_key, rane_loc_infop, inclusive)) {

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
      m_range_server.set_timeout(timer.remaining());
      m_range_server.create_scanner(addr, m_metadata_table, range,
                                    meta_scan_spec, scan_block);
    }
    catch (Exception &e) {
      if (e.code() == Error::RANGESERVER_RANGE_NOT_FOUND)
        m_cache->invalidate(0, meta_keys.start);
      SAVE_ERR2(e.code(), e, format("Problem creating scanner for start row "
                "'%s' on METADATA[..??]", meta_keys.start));
      return e.code();
    }

    if ((error = process_metadata_scanblock(scan_block)) != Error::OK) {
      m_range_server.destroy_scanner(addr, scan_block.get_scanner_id(), 0);
      return error;
    }

    if (!scan_block.eos()) {
      m_range_server.destroy_scanner(addr, scan_block.get_scanner_id(), 0);
    }

    if (!m_cache->lookup(0, meta_key, rane_loc_infop, inclusive)) {
      String err_msg = format("Unable to find metadata for row '%s' row_key=%s",
                              meta_keys.start, row_key);
      HT_ERRORF("%s", err_msg.c_str());
      SAVE_ERR(Error::METADATA_NOT_FOUND, err_msg);
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
    String err_msg = format("Invalid location found in METADATA entry for row "
        "'%s' - %s", start_row.c_str(), rane_loc_infop->location.c_str());
    HT_ERRORF("%s", err_msg.c_str());
    SAVE_ERR(Error::INVALID_METADATA, err_msg);
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

  if (m_conn_manager &&
      !m_conn_manager->wait_for_connection(addr, timer.remaining())) {
    if (timer.expired())
      HT_THROW_(Error::REQUEST_TIMEOUT);
  }

  try {
    m_range_server.set_timeout(timer.remaining());
    m_range_server.create_scanner(addr, m_metadata_table, range,
                                  meta_scan_spec, scan_block);
  }
  catch (Exception &e) {
    if (e.code() == Error::RANGESERVER_RANGE_NOT_FOUND)
      m_cache->invalidate(0, meta_keys.start+2);
    SAVE_ERR2(e.code(), e, format("Problem creating scanner on second-level "
              "METADATA (start row = %s)", ri.start));
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

  if (!m_cache->lookup(table->id, row_key, rane_loc_infop, inclusive)) {
    SAVE_ERR(Error::METADATA_NOT_FOUND, (String)"RangeLocator failed to find "
             "metadata for table '" + table->name + "' row '" + row_key + "'");
    return Error::METADATA_NOT_FOUND;
  }

  return Error::OK;
}


int RangeLocator::process_metadata_scanblock(ScanBlock &scan_block) {
  RangeLocationInfo range_loc_info;
  SerializedKey serkey;
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

  while (scan_block.next(serkey, value)) {

    if (!key.load(serkey)) {
      String err_msg = format("METADATA lookup for '%s' returned bad key",
                              serkey.str() + 1);
      HT_ERRORF("%s", err_msg.c_str());
      SAVE_ERR(Error::INVALID_METADATA, err_msg);
      return Error::INVALID_METADATA;
    }

    if ((stripped_key = strchr(key.row, ':')) == 0) {
      String err_msg = format("Bad row key found in METADATA - '%s'", key.row);
      HT_ERRORF("%s", err_msg.c_str());
      SAVE_ERR(Error::INVALID_METADATA, err_msg);
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
            String err_msg = format("Invalid location found in METADATA entry "
                "for row '%s' - %s", range_loc_info.end_row.c_str(),
                range_loc_info.location.c_str());
            SAVE_ERR(Error::INVALID_METADATA, err_msg);
            HT_ERRORF("%s", err_msg.c_str());
            return Error::INVALID_METADATA;
          }
          if (m_conn_manager)
            m_conn_manager->add(addr, METADATA_RETRY_INTERVAL, "RangeServer");

          m_cache->insert(table_id, range_loc_info);
          /*
          HT_DEBUG_OUT << "(1) cache insert table=" << table_id << " start="
              << range_loc_info.start_row << " end=" << range_loc_info.end_row
              << " loc=" << range_loc_info.location << HT_END;
          */
        }
        else {
          SAVE_ERR(Error::INVALID_METADATA, format("Incomplete METADATA record "
                   "found under row key '%s' (got_location=%s)", range_loc_info
                   .end_row.c_str(), got_location ? "true" : "false"));
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
      range_loc_info.start_row = String((const char *)str, len);
      got_start_row = true;
    }
    else if (key.column_family_code == m_location_cid) {
      const uint8_t *str;
      size_t len = value.decode_length(&str);
      range_loc_info.location = String((const char *)str, len);
      if (range_loc_info.location == "!")
        return Error::TABLE_NOT_FOUND;
      got_location = true;
    }
    else {
      HT_ERRORF("METADATA lookup on row '%s' returned incorrect column (id=%d)",
                serkey.row(), key.column_family_code);
    }
  }

  if (got_start_row && got_end_row && got_location) {

    /**
     * Add this location (address) to the connection manager
     */
    if (!LocationCache::location_to_addr(
        range_loc_info.location.c_str(), addr)) {
      String err_msg = format("Invalid location found in METADATA entry for "
          "row '%s' - %s", range_loc_info.end_row.c_str(),
          range_loc_info.location.c_str());
      SAVE_ERR(Error::INVALID_METADATA, err_msg);
      HT_ERRORF("%s", err_msg.c_str());
      return Error::INVALID_METADATA;
    }
    if (m_conn_manager)
      m_conn_manager->add(addr, METADATA_RETRY_INTERVAL, "RangeServer");

    m_cache->insert(table_id, range_loc_info);

    /*
    HT_DEBUG_OUT << "(2) cache insert table=" << table_id << " start="
        << range_loc_info.start_row << " end=" << range_loc_info.end_row
        << " loc=" << range_loc_info.location << HT_END;
    */
  }
  else if (got_end_row) {
    SAVE_ERR(Error::INVALID_METADATA, format("Incomplete METADATA record found "
             "under row key '%s' (got_location=%s)", range_loc_info
             .end_row.c_str(), got_location ? "true" : "false"));
  }

  return Error::OK;
}


int RangeLocator::read_root_location(Timer &timer) {
  DynamicBuffer value(0);
  String addr_str;

  {
    ScopedLock lock(m_hyperspace_mutex);
    if (m_hyperspace_init)
      m_hyperspace->attr_get(m_root_file_handle, "Location", value);
    else if (m_hyperspace_connected) {
      initialize();
      m_hyperspace->attr_get(m_root_file_handle, "Location", value);
      }
    else
      HT_THROW(Error::CONNECT_ERROR_HYPERSPACE, "RangeLocator not connected to Hyperspace");
  }

  m_root_range_info.start_row  = "";
  m_root_range_info.end_row    = Key::END_ROOT_ROW;
  m_root_range_info.location   = (const char *)value.base;

  m_cache->insert(0, m_root_range_info, true);

  if (!LocationCache::location_to_addr((const char *)value.base, m_root_addr)) {
    HT_ERROR("Bad format of 'Location' attribute of "
             "/hypertable/root Hyperspace file");
    SAVE_ERR(Error::BAD_ROOT_LOCATION, "Bad format of 'Location' attribute"
                  " of /hypertable/root Hyperspace file");
    return Error::BAD_ROOT_LOCATION;
  }

  if (m_conn_manager) {
    uint32_t after_remaining, remaining = timer.remaining();

    m_conn_manager->add(m_root_addr, ROOT_METADATA_RETRY_INTERVAL,
                        "Root RangeServer");

    if (!m_conn_manager->wait_for_connection(m_root_addr, remaining)) {
      after_remaining = timer.remaining();
      HT_ERRORF("Timeout (%u millis) waiting for root RangeServer connection "
                "- %s", remaining - after_remaining,
                m_root_addr.format().c_str());
      return Error::REQUEST_TIMEOUT;
    }
  }

  m_root_stale = false;

  return Error::OK;
}

void RangeLocatorHyperspaceSessionCallback::disconnected() {
  m_rangelocator->hyperspace_disconnected();
}

void RangeLocatorHyperspaceSessionCallback::reconnected() {
  m_rangelocator->hyperspace_reconnected();
}


