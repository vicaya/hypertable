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
#include <vector>

#include "Common/Error.h"
#include "Common/String.h"

#include "Defaults.h"
#include "Key.h"
#include "IntervalScanner.h"

extern "C" {
#include <poll.h>
}

using namespace Hypertable;


/**
 * TODO: Asynchronously destroy dangling scanners on EOS
 */

/**
 */
IntervalScanner::IntervalScanner(PropertiesPtr &props_ptr, Comm *comm,
    const TableIdentifier *table_identifier, SchemaPtr &schema_ptr,
    RangeLocatorPtr &range_locator_ptr, const ScanSpec &scan_spec, int timeout)
  : m_comm(comm), m_schema_ptr(schema_ptr),
    m_range_locator_ptr(range_locator_ptr),
    m_range_server(comm, HYPERTABLE_CLIENT_TIMEOUT),
    m_table_identifier(*table_identifier), m_started(false),
    m_eos(false), m_readahead(true), m_fetch_outstanding(false),
    m_end_inclusive(false), m_rows_seen(0), m_timeout(timeout) {
  const char *start_row, *end_row;

  if (!scan_spec.row_intervals.empty() && !scan_spec.cell_intervals.empty())
    HT_THROW(Error::BAD_SCAN_SPEC,
             "ROW predicates and CELL predicates can't be combined");

  if (m_timeout == 0 ||
      (m_timeout = props_ptr->get_int("Hypertable.Client.Timeout", 0)) == 0 ||
      (m_timeout = props_ptr->get_int("Hypertable.Request.Timeout", 0)) == 0)
    m_timeout = HYPERTABLE_CLIENT_TIMEOUT;

  m_range_locator_ptr->get_location_cache(m_cache_ptr);
  m_range_server.set_default_timeout(m_timeout);

  m_scan_spec_builder.set_row_limit(scan_spec.row_limit);
  m_scan_spec_builder.set_max_versions(scan_spec.max_versions);

  for (size_t i=0; i<scan_spec.columns.size(); i++) {
    if (m_schema_ptr->get_column_family(scan_spec.columns[i]) == 0)
      HT_THROW(Error::RANGESERVER_INVALID_COLUMNFAMILY, scan_spec.columns[i]);
    m_scan_spec_builder.add_column(scan_spec.columns[i]);
  }

  HT_ASSERT(scan_spec.row_intervals.size() <= 1);

  if (!scan_spec.row_intervals.empty()) {
    start_row = (scan_spec.row_intervals[0].start == 0) ? ""
        : scan_spec.row_intervals[0].start;
    end_row = (scan_spec.row_intervals[0].end == 0) ? Key::END_ROW_MARKER
        : scan_spec.row_intervals[0].end;
    int cmpval = strcmp(start_row, end_row);
    if (cmpval > 0)
      HT_THROW(Error::BAD_SCAN_SPEC, "start_row > end_row");
    if (cmpval == 0 && !scan_spec.row_intervals[0].start_inclusive
        && !scan_spec.row_intervals[0].end_inclusive)
      HT_THROW(Error::BAD_SCAN_SPEC, "empty row interval");
    m_start_row = start_row;
    m_end_row = end_row;
    m_end_inclusive = scan_spec.row_intervals[0].end_inclusive;
    m_scan_spec_builder.add_row_interval(start_row,
        scan_spec.row_intervals[0].start_inclusive, end_row,
        scan_spec.row_intervals[0].end_inclusive);
  }
  else if (!scan_spec.cell_intervals.empty()) {
    start_row = (scan_spec.cell_intervals[0].start_row == 0) ? ""
        : scan_spec.cell_intervals[0].start_row;
    if (scan_spec.cell_intervals[0].start_column == 0)
      HT_THROW(Error::BAD_SCAN_SPEC,
               "Bad cell interval (start_column == NULL)");
    end_row = (scan_spec.cell_intervals[0].end_row == 0) ? Key::END_ROW_MARKER
        : scan_spec.cell_intervals[0].end_row;
    if (scan_spec.cell_intervals[0].end_column == 0)
      HT_THROW(Error::BAD_SCAN_SPEC,
               "Bad cell interval (end_column == NULL)");
    int cmpval = strcmp(start_row, end_row);
    if (cmpval > 0)
      HT_THROW(Error::BAD_SCAN_SPEC, "start_row > end_row");
    if (cmpval == 0) {
      int cmpval = strcmp(scan_spec.cell_intervals[0].start_column,
                          scan_spec.cell_intervals[0].end_column);
      if (cmpval > 0)
        HT_THROW(Error::BAD_SCAN_SPEC, "start_column > end_column");
      if (cmpval == 0 && !scan_spec.cell_intervals[0].start_inclusive
          && !scan_spec.cell_intervals[0].end_inclusive)
        HT_THROW(Error::BAD_SCAN_SPEC, "empty cell interval");
    }
    m_scan_spec_builder.add_cell_interval(start_row,
        scan_spec.cell_intervals[0].start_column,
        scan_spec.cell_intervals[0].start_inclusive,
        end_row, scan_spec.cell_intervals[0].end_column,
        scan_spec.cell_intervals[0].end_inclusive);
    m_start_row = scan_spec.cell_intervals[0].start_row;
    m_end_row = scan_spec.cell_intervals[0].end_row;
    m_end_inclusive = true;
  }
  else {
    m_start_row = "";
    m_end_row = Key::END_ROW_MARKER;
    m_scan_spec_builder.add_row_interval("", false, Key::END_ROW_MARKER, false);
  }

  if (m_scan_spec_builder.get().row_limit == 1
      || m_start_row.compare(m_end_row) == 0)
    m_readahead = false;

  m_scan_spec_builder.set_time_interval(scan_spec.time_interval.first,
                                        scan_spec.time_interval.second);

  m_scan_spec_builder.set_return_deletes(scan_spec.return_deletes);
}


/**
 *
 */
IntervalScanner::~IntervalScanner() {

  // if there is an outstanding fetch, wait for it to come back or timeout
  if (m_fetch_outstanding)
    m_sync_handler.wait_for_reply(m_event_ptr);
}



bool IntervalScanner::next(Cell &cell) {
  int error;
  SerializedKey serkey;
  ByteString value;
  Key key;
  Timer timer(m_timeout);

  if (m_eos)
    return false;

  if (!m_started)
    find_range_and_start_scan(m_start_row.c_str(), timer);

  while (!m_scanblock.more()) {
    if (m_scanblock.eos()) {
      if (!strcmp(m_range_info.end_row.c_str(), Key::END_ROW_MARKER) ||
          m_end_row.compare(m_range_info.end_row) <= 0) {
        m_range_server.destroy_scanner(m_cur_addr,
                                       m_scanblock.get_scanner_id(), 0);
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
          HT_ERRORF("RangeServer 'fetch scanblock' error : %s",
                    Protocol::string_format_message(m_event_ptr).c_str());
          HT_THROW((int)Protocol::response_code(m_event_ptr), "");
        }
        else {
          error = m_scanblock.load(m_event_ptr);
          if (m_readahead && !m_scanblock.eos()) {
            m_range_server.fetch_scanblock(m_cur_addr,
                m_scanblock.get_scanner_id(), &m_sync_handler);
            m_fetch_outstanding = true;
          }
          else
            m_fetch_outstanding = false;
        }
      }
      else {
        timer.start();
        m_range_server.set_timeout((time_t)(timer.remaining() + 0.5));
        m_range_server.fetch_scanblock(m_cur_addr,
            m_scanblock.get_scanner_id(), m_scanblock);
      }

    }
  }

  if (m_scanblock.next(serkey, value)) {
    Schema::ColumnFamily *cf;
    if (!key.load(serkey))
      HT_THROW(Error::BAD_KEY, "");

    // check for end row

    if (!strcmp(key.row, Key::END_ROW_MARKER)) {
      m_range_server.destroy_scanner(m_cur_addr,
                                     m_scanblock.get_scanner_id(), 0);
      m_eos = true;
      return false;
    }

    if (m_end_inclusive) {
      if (strcmp(key.row, m_end_row.c_str()) > 0) {
        m_range_server.destroy_scanner(m_cur_addr,
                                       m_scanblock.get_scanner_id(), 0);
        m_eos = true;
        return false;
      }
    }
    else {
      if (strcmp(key.row, m_end_row.c_str()) >= 0) {
        m_range_server.destroy_scanner(m_cur_addr,
                                       m_scanblock.get_scanner_id(), 0);
        m_eos = true;
        return false;
      }
    }

    // check for row change and row limit
    if (strcmp(m_cur_row.c_str(), key.row)) {
      m_rows_seen++;
      m_cur_row = key.row;
      if (m_scan_spec_builder.get().row_limit > 0
          && m_rows_seen > m_scan_spec_builder.get().row_limit) {
        m_range_server.destroy_scanner(m_cur_addr,
                                       m_scanblock.get_scanner_id(), 0);
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



void
IntervalScanner::find_range_and_start_scan(const char *row_key, Timer &timer) {
  RangeSpec  range;
  DynamicBuffer dbuf(0);

  timer.start();

 try_again:

  try {
    if (!m_cache_ptr->lookup(m_table_identifier.id, row_key, &m_range_info))
      m_range_locator_ptr->find_loop(&m_table_identifier, row_key,
                                     &m_range_info, timer, false);
  }
  catch (Exception &e) {
    if (e.code() == Error::REQUEST_TIMEOUT)
      HT_THROW2(e.code(), e, e.what());
    poll(0, 0, 1000);
    if (timer.expired())
      HT_THROW(Error::REQUEST_TIMEOUT, "");
    goto try_again;
  }

  m_started = true;

  while (true) {
    dbuf.ensure(m_range_info.start_row.length()
                + m_range_info.end_row.length() + 2);
    range.start_row = (char *)dbuf.add_unchecked(m_range_info.start_row.c_str(),
        m_range_info.start_row.length()+1);
    range.end_row   = (char *)dbuf.add_unchecked(m_range_info.end_row.c_str(),
        m_range_info.end_row.length()+1);
    if (!LocationCache::location_to_addr(m_range_info.location.c_str(),
        m_cur_addr)) {
      HT_ERRORF("Invalid location found in METADATA entry range [%s..%s] - %s",
                range.start_row, range.end_row, m_range_info.location.c_str());
      HT_THROW(Error::INVALID_METADATA, "");
    }

    try {
      m_range_server.set_timeout((time_t)(timer.remaining() + 0.5));
      m_range_server.create_scanner(m_cur_addr, m_table_identifier, range,
                                    m_scan_spec_builder.get(), m_scanblock);
    }
    catch (Exception &e) {
      double remaining = timer.remaining();

      if (e.code() != Error::REQUEST_TIMEOUT
          && e.code() != Error::RANGESERVER_RANGE_NOT_FOUND
          || remaining <= 3.0) {
        HT_ERRORF("%s - %s", e.what(), Error::get_text(e.code()));
        HT_THROW(e.code(), String("Problem creating scanner on ")
                 + m_table_identifier.name + "[" + range.start_row + ".."
                 + range.end_row + "]");
      }

      // wait a few seconds
      poll(0, 0, 3000);

      // try again, the hard way
      m_range_locator_ptr->find_loop(&m_table_identifier, row_key,
                                     &m_range_info, timer, true);
      continue;
    }
    break;
  }

  // maybe kick off readahead
  if (m_readahead && !m_scanblock.eos()) {
    m_range_server.fetch_scanblock(m_cur_addr, m_scanblock.get_scanner_id(),
                                   &m_sync_handler);
    m_fetch_outstanding = true;
  }
  else
    m_fetch_outstanding = false;
}
