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
#include <vector>

#include "Common/Error.h"
#include "Common/String.h"

#include "Key.h"
#include "IntervalScanner.h"
#include "Table.h"

extern "C" {
#include <poll.h>
}

using namespace Hypertable;

/**
 * TODO: Asynchronously destroy dangling scanners on EOS
 */
IntervalScanner::IntervalScanner(Comm *comm, Table *table,
    RangeLocatorPtr &range_locator, const ScanSpec &scan_spec,
    uint32_t timeout_ms, bool retry_table_not_found)
  : m_comm(comm), m_table(table), m_range_locator(range_locator),
    m_loc_cache(range_locator->location_cache()),
    m_range_server(comm, timeout_ms), m_eos(false), m_readahead(true),
    m_fetch_outstanding(false), m_create_scanner_outstanding(false),
    m_end_inclusive(false), m_rows_seen(0), m_timeout_ms(timeout_ms),
    m_retry_table_not_found(retry_table_not_found), m_bytes_scanned(0) {

  HT_ASSERT(m_timeout_ms);

  Timer timer(timeout_ms);
  table->get(m_table_identifier, m_schema);
  init(scan_spec, timer);
}


void IntervalScanner::init(const ScanSpec &scan_spec, Timer &timer) {
  const char *start_row, *end_row;
  String family, qualifier;
  bool has_qualifier, is_regexp;

  if (!scan_spec.row_intervals.empty() && !scan_spec.cell_intervals.empty())
    HT_THROW(Error::BAD_SCAN_SPEC,
             "ROW predicates and CELL predicates can't be combined");

  m_range_server.set_default_timeout(m_timeout_ms);
  m_rowset.clear();

  m_scan_spec_builder.clear();
  m_scan_spec_builder.set_row_limit(scan_spec.row_limit);
  m_scan_spec_builder.set_cell_limit(scan_spec.cell_limit);
  m_scan_spec_builder.set_max_versions(scan_spec.max_versions);
  m_scan_spec_builder.set_row_regexp(scan_spec.row_regexp);
  m_scan_spec_builder.set_value_regexp(scan_spec.value_regexp);

  for (size_t i=0; i<scan_spec.columns.size(); i++) {
    ScanSpec::parse_column(scan_spec.columns[i], family, qualifier, &has_qualifier, &is_regexp);
    if (m_schema->get_column_family(family.c_str()) == 0)
      HT_THROW(Error::RANGESERVER_INVALID_COLUMNFAMILY,
      (String)"Table= " + m_table->get_name() + " , Column family=" + scan_spec.columns[i]);
    m_scan_spec_builder.add_column(scan_spec.columns[i]);
  }

  HT_ASSERT(scan_spec.row_intervals.size() <= 1 || scan_spec.scan_and_filter_rows);

  if (!scan_spec.row_intervals.empty()) {
    if (!scan_spec.scan_and_filter_rows) {
      start_row = (scan_spec.row_intervals[0].start == 0) ? ""
          : scan_spec.row_intervals[0].start;
      if (scan_spec.row_intervals[0].end == 0 ||
          scan_spec.row_intervals[0].end[0] == 0)
        end_row = Key::END_ROW_MARKER;
      else
        end_row = scan_spec.row_intervals[0].end;
      int cmpval = strcmp(start_row, end_row);
      if (cmpval > 0)
        HT_THROW(Error::BAD_SCAN_SPEC, format("start_row (%s) > end_row (%s)", start_row, end_row));
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
    else {
      m_scan_spec_builder.set_scan_and_filter_rows(true);
      // order and filter duplicated rows
      CstrRowSet rowset;
      foreach (const RowInterval& ri, scan_spec.row_intervals)
        rowset.insert(ri.start); // ri.start always equals to ri.end
      // setup ordered row intervals and rowset
      m_scan_spec_builder.reserve_rows(rowset.size());
      foreach (const char* r, rowset) {
        m_scan_spec_builder.add_row_interval(r, true, "", true); // end is set to "" in order to safe space
        m_rowset.insert(m_scan_spec_builder.get().row_intervals.back().start); // Cstr's must be taken from m_scan_spec_builder and not from scan_spec
      }
      m_start_row = *m_rowset.begin();
      m_end_row = *m_rowset.rbegin();
      m_end_inclusive = true;
    }
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

  // start scan asynchronously (can trigger table not found exceptions)
  m_create_scanner_row = m_start_row;
  find_range_and_start_scan(m_create_scanner_row.c_str(), timer);
}


IntervalScanner::~IntervalScanner() {

  // if there is an outstanding fetch, wait for it to come back or timeout
  if (m_fetch_outstanding)
    m_fetch_scanblock_handler.wait_for_reply(m_event);

  // if there is an outstanding create_scanner, wait for it to come back or timeout
  if (m_create_scanner_outstanding)
    m_create_scanner_handler.wait_for_reply(m_event);
}


void IntervalScanner::request_next_scanblock(CommAddress addr) {
  assert(m_readahead);
  if (!m_scanblock.eos() && !m_fetch_outstanding) {
    m_range_server.fetch_scanblock(addr, m_scanblock.get_scanner_id(),
                                   &m_fetch_scanblock_handler);
    m_fetch_outstanding = true;
  }
}


int IntervalScanner::fetch_create_scanner_result(Timer &timer) {
  int error = Error::OK;
  assert(m_create_scanner_outstanding);

  if (!m_create_scanner_handler.wait_for_reply(m_event)) {
    m_create_scanner_outstanding = false;
    poll(0, 0, 1000);
    // find and start scan synchronously
    find_range_and_start_scan(m_create_scanner_row.c_str(), timer, true);
  }
  else {
    m_range_info = m_next_range_info;
    m_create_scanner_outstanding = false;
    error = m_scanblock.load(m_event);
    if (m_readahead)
      request_next_scanblock(m_range_info.addr);
  }

  if (m_readahead && m_end_row.compare(m_range_info.end_row) > 0) {
    m_create_scanner_row = m_range_info.end_row;
    // if rowset scan update start row for next range
    if (!m_rowset.empty())
      if (m_create_scanner_row.compare(*m_rowset.begin()) < 0)
        m_create_scanner_row = *m_rowset.begin();

    m_create_scanner_row.append(1,1);  // construct row key in next range
    find_range_and_start_scan(m_create_scanner_row.c_str(), timer);
  }

  return error;
}

bool IntervalScanner::next(Cell &cell) {
  int error;
  SerializedKey serkey;
  ByteString value;
  Key key;
  Timer timer(m_timeout_ms);

  if (m_eos)
    return false;

  if (!m_scanblock.more() && m_scanblock.eos() && m_create_scanner_outstanding)
    fetch_create_scanner_result(timer);

  while (!m_scanblock.more()) {
    if (m_scanblock.eos()) {
      if (!strcmp(m_range_info.end_row.c_str(), Key::END_ROW_MARKER) ||
          m_end_row.compare(m_range_info.end_row) <= 0) {
        m_eos = true;
        return false;
      }
      if (!m_create_scanner_outstanding) {
        m_create_scanner_row = m_range_info.end_row;
        // if rowset scan update start row for next range
        if (!m_rowset.empty())
          if (m_create_scanner_row.compare(*m_rowset.begin()) < 0)
            m_create_scanner_row = *m_rowset.begin();

        m_create_scanner_row.append(1,1);  // construct row key in next range
        find_range_and_start_scan(m_create_scanner_row.c_str(), timer);
      }
      fetch_create_scanner_result(timer);
    }
    else {
      if (m_fetch_outstanding) {
        if (!m_fetch_scanblock_handler.wait_for_reply(m_event)) {
          m_fetch_outstanding = false;
          HT_ERRORF("fetch scanblock : %s - %s",
                    Error::get_text((int)Protocol::response_code(m_event)),
                    Protocol::string_format_message(m_event).c_str());
          HT_THROW((int)Protocol::response_code(m_event), "");
        }
        else {
          m_fetch_outstanding = false;
          error = m_scanblock.load(m_event);
          request_next_scanblock(m_range_info.addr);
        }
      }
      else {
        timer.start();
        m_range_server.fetch_scanblock(m_range_info.addr,
                   m_scanblock.get_scanner_id(), m_scanblock, timer);
      }

    }
  }

  if (m_scanblock.next(serkey, value)) {
    Schema::ColumnFamily *cf;
    if (!key.load(serkey))
      HT_THROW(Error::BAD_KEY, "");

    // check for end row

    if (!strcmp(key.row, Key::END_ROW_MARKER)) {
      if (!m_scanblock.eos())
        m_range_server.destroy_scanner(m_range_info.addr,
                                       m_scanblock.get_scanner_id(), 0);
      m_eos = true;
      return false;
    }

    if (m_end_inclusive) {
      if (strcmp(key.row, m_end_row.c_str()) > 0) {
        if (!m_scanblock.eos())
          m_range_server.destroy_scanner(m_range_info.addr,
                                         m_scanblock.get_scanner_id(), 0);
        m_eos = true;
        return false;
      }
    }
    else {
      if (strcmp(key.row, m_end_row.c_str()) >= 0) {
        if (!m_scanblock.eos())
          m_range_server.destroy_scanner(m_range_info.addr,
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
        if (!m_scanblock.eos())
          m_range_server.destroy_scanner(m_range_info.addr,
                                         m_scanblock.get_scanner_id(), 0);
        m_eos = true;
        return false;
      }
    }

    cell.row_key = key.row;
    cell.column_qualifier = key.column_qualifier;
    if ((cf = m_schema->get_column_family(key.column_family_code)) == 0) {
      if (key.flag != FLAG_DELETE_ROW)
	HT_THROWF(Error::BAD_KEY, "Unexpected column family code %d",
		  (int)key.column_family_code);
      cell.column_family = "";
    }
    else
      cell.column_family = cf->name.c_str();

    cell.timestamp = key.timestamp;
    cell.revision = key.revision;
    cell.value_len = value.decode_length(&cell.value);
    cell.flag = key.flag;
    m_bytes_scanned += key.length + cell.value_len;

    // if rowset scan remove scanned row
    while (!m_rowset.empty() && strcmp(*m_rowset.begin(), key.row) < 0)
      m_rowset.erase(m_rowset.begin());
    return true;
  }

  HT_ERROR("No end marker found at end of table.");

  if (!m_scanblock.eos())
    m_range_server.destroy_scanner(m_range_info.addr, m_scanblock.get_scanner_id(), 0);

  m_eos = true;
  return false;
}

void
IntervalScanner::find_range_and_start_scan(const char *row_key, Timer &timer, bool synchronous)
{
  RangeSpec  range;
  DynamicBuffer dbuf(0);

  // if rowset scan adjust row intervals
  if (!m_rowset.empty()) {
    RowIntervals& row_intervals = m_scan_spec_builder.get().row_intervals;
    while (row_intervals.size() && strcmp(row_intervals.front().start, row_key) < 0)
      row_intervals.erase(row_intervals.begin());
  }

  timer.start();

 try_again:

  try {
    if (!m_loc_cache->lookup(m_table_identifier.id, row_key, &m_next_range_info))
      m_range_locator->find_loop(&m_table_identifier, row_key,
                                 &m_next_range_info, timer, false);
  }
  catch (Exception &e) {
    if (e.code() == Error::REQUEST_TIMEOUT)
      HT_THROW2(e.code(), e, e.what());
    poll(0, 0, 1000);
    if (timer.expired())
      HT_THROW(Error::REQUEST_TIMEOUT, "");
    goto try_again;
  }

  while (true) {
    dbuf.ensure(m_next_range_info.start_row.length()
                + m_next_range_info.end_row.length() + 2);
    range.start_row = (char *)dbuf.add_unchecked(m_next_range_info.start_row.c_str(),
        m_next_range_info.start_row.length()+1);
    range.end_row   = (char *)dbuf.add_unchecked(m_next_range_info.end_row.c_str(),
        m_next_range_info.end_row.length()+1);

    try {

      //HT_ASSERT(!m_create_scanner_outstanding);

      if (synchronous) {
        int num_retries = 0;
        do {
          try {
            if (num_retries++ > 1)    // only pause from the 2nd retry and on
              poll(0, 0, 3000);
            m_range_server.create_scanner(m_next_range_info.addr, m_table_identifier, range,
                                          m_scan_spec_builder.get(), m_scanblock, timer);
            m_range_info = m_next_range_info;
            m_create_scanner_outstanding = false;
            break;
          }
          catch (Exception &e) {
            if (e.code() == Error::RANGESERVER_GENERATION_MISMATCH
                || (m_retry_table_not_found && timer.remaining() > 0
                    && (e.code() == Error::TABLE_NOT_FOUND
                        || e.code() == Error::RANGESERVER_TABLE_NOT_FOUND))) {
              m_table->refresh(m_table_identifier, m_schema);
              HT_DEBUG_OUT << "Table refresh no. "<< num_retries << HT_END;
              continue;
            }
            HT_THROW2(e.code(), e, e.what());
          }
        } while (true);
      }
      else {
        // create scanner asynchronously
        m_range_server.create_scanner(m_next_range_info.addr, m_table_identifier, range,
                                      m_scan_spec_builder.get(), &m_create_scanner_handler,
                                      timer);
        m_create_scanner_outstanding = true;
      }
    }
    catch (Exception &e) {
      String msg = format("Problem creating scanner on %s[%s..%s]",
                          m_table_identifier.id, range.start_row,
                          range.end_row);
      m_create_scanner_outstanding = false;
      if (e.code() == Error::RANGESERVER_GENERATION_MISMATCH
          || e.code() == Error::RANGESERVER_TABLE_NOT_FOUND) {
        HT_WARN_OUT << e << HT_END;
        HT_THROW2(e.code(), e, msg);
      }
      else if ((e.code() != Error::REQUEST_TIMEOUT
                && e.code() != Error::RANGESERVER_RANGE_NOT_FOUND
                && e.code() != Error::COMM_NOT_CONNECTED
                && e.code() != Error::COMM_BROKEN_CONNECTION)) {
        HT_ERROR_OUT << e << HT_END;
        HT_THROW2(e.code(), e, msg);
      }
      else if (timer.remaining() <= 1000) {
        uint32_t duration  = timer.duration();
        HT_ERRORF("Scanner creation request will time out. Initial timer "
                  "duration %d", (int)duration);
        HT_THROW2(Error::REQUEST_TIMEOUT, e, msg + format(". Unable to "
                  "complete request within %d ms", (int)duration));
      }

      poll(0, 0, 1000);

      // try again, the hard way
      m_range_locator->find_loop(&m_table_identifier, row_key,
                                 &m_next_range_info, timer, true);
      continue;
    }
    break;
  }

  // maybe kick off readahead
  if (synchronous && m_readahead)
    request_next_scanblock(m_range_info.addr);

}
