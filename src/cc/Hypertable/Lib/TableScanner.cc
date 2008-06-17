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
#include <vector>

#include "Common/String.h"
#include "Defaults.h"
#include "Key.h"
#include "TableScanner.h"

using namespace Hypertable;

namespace {
  const char end_row_key[2] = { (char)0xff, (char)0xff };
}

/**
 * TODO: Asynchronously destroy dangling scanners on EOS
 */

/**
 */
TableScanner::TableScanner(PropertiesPtr &props_ptr, Comm *comm, TableIdentifier *table_identifier, SchemaPtr &schema_ptr, RangeLocatorPtr &range_locator_ptr, ScanSpec &scan_spec, int timeout) : m_comm(comm), m_schema_ptr(schema_ptr), m_range_locator_ptr(range_locator_ptr), m_range_server(comm, HYPERTABLE_CLIENT_TIMEOUT), m_table_identifier(table_identifier), m_started(false), m_eos(false), m_readahead(true), m_fetch_outstanding(false), m_rows_seen(0), m_timeout(timeout) {
  char *str;

  if (m_timeout == 0 ||
      (m_timeout = props_ptr->get_int("Hypertable.Client.Timeout", 0)) == 0 ||
      (m_timeout = props_ptr->get_int("Hypertable.Request.Timeout", 0)) == 0)
    m_timeout = HYPERTABLE_CLIENT_TIMEOUT;

  m_range_locator_ptr->get_location_cache(m_cache_ptr);

  m_range_server.set_default_timeout(m_timeout);

  m_scan_spec.row_limit = scan_spec.row_limit;
  m_scan_spec.max_versions = scan_spec.max_versions;

  // deep copy columns vector
  for (size_t i=0; i<scan_spec.columns.size(); i++) {
    str = new char [strlen(scan_spec.columns[i]) + 1];
    strcpy(str, scan_spec.columns[i]);
    m_scan_spec.columns.push_back(str);
  }

  // deep copy start row
  if (scan_spec.start_row) {
    str = new char [strlen(scan_spec.start_row) + 1];
    strcpy(str, scan_spec.start_row);
    m_scan_spec.start_row = str;
  }
  else
    m_scan_spec.start_row = 0;
  m_scan_spec.start_row_inclusive = scan_spec.start_row_inclusive;

  // deep copy end row
  if (scan_spec.end_row) {
    str = new char [strlen(scan_spec.end_row) + 1];
    strcpy(str, scan_spec.end_row);
    m_scan_spec.end_row = str;
  }
  else
    m_scan_spec.end_row = 0;
  m_scan_spec.end_row_inclusive = scan_spec.end_row_inclusive;

  if (m_scan_spec.row_limit == 1 ||
      ((m_scan_spec.start_row && m_scan_spec.end_row) && !strcmp(m_scan_spec.start_row, m_scan_spec.end_row)))
    m_readahead = false;

  memcpy(&m_scan_spec.interval, &scan_spec.interval, sizeof(m_scan_spec.interval));

  m_scan_spec.return_deletes = scan_spec.return_deletes;

}


/**
 *
 */
TableScanner::~TableScanner() {
  for (size_t i=0; i<m_scan_spec.columns.size(); i++)
    delete [] m_scan_spec.columns[i];
  delete [] m_scan_spec.start_row;
  delete [] m_scan_spec.end_row;

  // if there is an outstanding fetch, wait for it to come back or timeout
  if (m_fetch_outstanding)
    m_sync_handler.wait_for_reply(m_event_ptr);
}



bool TableScanner::next(Cell &cell) {
  int error;
  ByteString bskey, value;
  Key key;
  Timer timer(m_timeout);

  if (m_eos)
    return false;

  if (!m_started)
    find_range_and_start_scan(m_scan_spec.start_row, timer);

  while (!m_scanblock.more()) {
    if (m_scanblock.eos()) {
      if (!strcmp(m_range_info.end_row.c_str(), Key::END_ROW_MARKER) ||
          (m_scan_spec.end_row && (strcmp(m_scan_spec.end_row, m_range_info.end_row.c_str()) <= 0))) {
        m_range_server.destroy_scanner(m_cur_addr, m_scanblock.get_scanner_id(), 0);
        m_eos = true;
        return false;
      }
      String next_row = m_range_info.end_row;
      next_row.append(1,1);  // construct row key in next range
      find_range_and_start_scan(next_row.c_str(), timer);
    }
    else {
      if (m_fetch_outstanding) {
        if (!m_sync_handler.wait_for_reply(m_event_ptr)) {
          m_fetch_outstanding = false;
          HT_ERRORF("RangeServer 'fetch scanblock' error : %s", Protocol::string_format_message(m_event_ptr).c_str());
          HT_THROW((int)Protocol::response_code(m_event_ptr), "");
        }
        else {
          error = m_scanblock.load(m_event_ptr);
          if (m_readahead && !m_scanblock.eos()) {
            m_range_server.fetch_scanblock(m_cur_addr, m_scanblock.get_scanner_id(), &m_sync_handler);
            m_fetch_outstanding = true;
          }
          else
            m_fetch_outstanding = false;
        }
      }
      else {
        timer.start();
        m_range_server.set_timeout((time_t)(timer.remaining() + 0.5));
        m_range_server.fetch_scanblock(m_cur_addr, m_scanblock.get_scanner_id(), m_scanblock);
      }

    }
  }

  if (m_scanblock.next(bskey, value)) {
    Schema::ColumnFamily *cf;
    if (!key.load(bskey))
      HT_THROW(Error::BAD_KEY, "");

    // check for end row
    if (m_scan_spec.end_row) {
      if (m_scan_spec.end_row_inclusive) {
        if (strcmp(key.row, m_scan_spec.end_row) > 0) {
          m_range_server.destroy_scanner(m_cur_addr, m_scanblock.get_scanner_id(), 0);
          m_eos = true;
          return false;
        }
      }
      else {
        if (strcmp(key.row, m_scan_spec.end_row) >= 0) {
          m_range_server.destroy_scanner(m_cur_addr, m_scanblock.get_scanner_id(), 0);
          m_eos = true;
          return false;
        }
      }
    }
    else if (!strcmp(key.row, end_row_key)) {
      m_range_server.destroy_scanner(m_cur_addr, m_scanblock.get_scanner_id(), 0);
      m_eos = true;
      return false;
    }

    // check for row change and row limit
    if (strcmp(m_cur_row.c_str(), key.row)) {
      m_rows_seen++;
      m_cur_row = key.row;
      if (m_scan_spec.row_limit > 0 && m_rows_seen > m_scan_spec.row_limit) {
        m_range_server.destroy_scanner(m_cur_addr, m_scanblock.get_scanner_id(), 0);
        m_eos = true;
        return false;
      }
    }

    cell.row_key = key.row;
    cell.column_qualifier = key.column_qualifier;
    if ((cf = m_schema_ptr->get_column_family(key.column_family_code)) == 0) {
      // LOG ERROR ...
      //HT_THROW(Error::BAD_KEY, "");
      cell.column_family = 0;
    }
    else
      cell.column_family = cf->name.c_str();
    cell.timestamp = key.timestamp;
    cell.value_len = value.decode_length(&cell.value);
    cell.flag = key.flag;
    return true;
  }

  HT_ERROR("No end marker found at end of table.");

  m_range_server.destroy_scanner(m_cur_addr, m_scanblock.get_scanner_id(), 0);
  m_eos = true;
  return false;
}



void TableScanner::find_range_and_start_scan(const char *row_key, Timer &timer) {
  RangeSpec  range;
  DynamicBuffer dbuf(0);

  timer.start();

  if (!m_cache_ptr->lookup(m_table_identifier.id, row_key, &m_range_info))
    m_range_locator_ptr->find_loop(&m_table_identifier, row_key, &m_range_info, timer, false);

  m_started = true;

  dbuf.ensure(m_range_info.start_row.length() + m_range_info.end_row.length() + 2);
  range.start_row = (const char *)dbuf.add_unchecked(m_range_info.start_row.c_str(), m_range_info.start_row.length()+1);
  range.end_row   = (const char *)dbuf.add_unchecked(m_range_info.end_row.c_str(), m_range_info.end_row.length()+1);
  if (!LocationCache::location_to_addr(m_range_info.location.c_str(), m_cur_addr)) {
    HT_ERRORF("Invalid location found in METADATA entry range [%s..%s] - %s",
                 range.start_row, range.end_row, m_range_info.location.c_str());
    HT_THROW(Error::INVALID_METADATA, "");
  }

  try {
    m_range_server.set_timeout((time_t)(timer.remaining() + 0.5));
    m_range_server.create_scanner(m_cur_addr, m_table_identifier, range, m_scan_spec, m_scanblock);
  }
  catch (Exception &e) {

    // try again, the hard way
    m_range_locator_ptr->find_loop(&m_table_identifier, row_key, &m_range_info, timer, true);

    // reset the range information
    dbuf.ensure(m_range_info.start_row.length() + m_range_info.end_row.length() + 2);
    range.start_row = (const char *)dbuf.add_unchecked(m_range_info.start_row.c_str(), m_range_info.start_row.length()+1);
    range.end_row   = (const char *)dbuf.add_unchecked(m_range_info.end_row.c_str(), m_range_info.end_row.length()+1);
    if (!LocationCache::location_to_addr(m_range_info.location.c_str(), m_cur_addr)) {
      HT_ERRORF("Invalid location found in METADATA entry range [%s..%s] - %s",
                   range.start_row, range.end_row, m_range_info.location.c_str());
      HT_THROW(Error::INVALID_METADATA, "");
    }

    // create the scanner
    try {
      m_range_server.set_timeout((time_t)(timer.remaining() + 0.5));
      m_range_server.create_scanner(m_cur_addr, m_table_identifier, range, m_scan_spec, m_scanblock);
    }
    catch (Exception &e) {
      HT_ERRORF("%s", e.what());
      HT_THROW(e.code(), String("Problem creating scanner on ") + m_table_identifier.name + "[" + range.start_row + ".." + range.end_row + "]");
    }
  }

  // maybe kick off readahead
  if (m_readahead && !m_scanblock.eos()) {
    m_range_server.fetch_scanblock(m_cur_addr, m_scanblock.get_scanner_id(), &m_sync_handler);
    m_fetch_outstanding = true;
  }
  else
    m_fetch_outstanding = false;
}
