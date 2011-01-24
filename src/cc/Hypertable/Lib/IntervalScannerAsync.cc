/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
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
#include "IntervalScannerAsync.h"
#include "Table.h"

extern "C" {
#include <poll.h>
}

using namespace Hypertable;

IntervalScannerAsync::IntervalScannerAsync(Comm *comm, ApplicationQueuePtr &app_queue,
    Table *table, RangeLocatorPtr &range_locator, const ScanSpec &scan_spec,
    uint32_t timeout_ms, bool retry_table_not_found, bool current,
    TableScannerAsync *scanner, int id)
  : m_comm(comm), m_table(table), m_range_locator(range_locator),
    m_loc_cache(range_locator->location_cache()),
    m_range_server(comm, timeout_ms), m_eos(false),
    m_fetch_outstanding(false), m_create_outstanding(false),
    m_end_inclusive(false), m_rows_seen(0), m_timeout_ms(timeout_ms),
    m_retry_table_not_found(retry_table_not_found), m_current(current), m_bytes_scanned(0),
    m_handler(app_queue, scanner, id),
    m_scanner(scanner), m_id(id), m_timer(timeout_ms), m_cur_scanner_finished(false),
    m_cur_scanner_id(0) {

  HT_ASSERT(m_timeout_ms);

  table->get(m_table_identifier, m_schema);
  init(scan_spec);
}


void IntervalScannerAsync::init(const ScanSpec &scan_spec) {
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
      CstrSet rowset;
      foreach (const RowInterval& ri, scan_spec.row_intervals)
        rowset.insert(ri.start); // ri.start always equals to ri.end
      // setup ordered row intervals and rowset
      m_scan_spec_builder.reserve_rows(rowset.size());
      foreach (const char* r, rowset) {
        // end is set to "" in order to safe space
        m_scan_spec_builder.add_row_interval(r, true, "", true);
        // Cstr's must be taken from m_scan_spec_builder and not from scan_spec
        m_rowset.insert(m_scan_spec_builder.get().row_intervals.back().start);
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
  m_scan_spec_builder.set_time_interval(scan_spec.time_interval.first,
                                        scan_spec.time_interval.second);

  m_scan_spec_builder.set_return_deletes(scan_spec.return_deletes);

  // start scan asynchronously (can trigger table not found exceptions)
  m_create_scanner_row = m_start_row;
  find_range_and_start_scan(m_create_scanner_row.c_str());
}

IntervalScannerAsync::~IntervalScannerAsync() {
  HT_ASSERT(!m_fetch_outstanding && !m_create_outstanding);
}

// caller is responsible for state of m_timer
void IntervalScannerAsync::find_range_and_start_scan(const char *row_key, bool hard) {

  RangeSpec  range;
  DynamicBuffer dbuf(0);

  m_timer.start();

  // if rowset scan adjust row intervals
  if (!m_rowset.empty()) {
    RowIntervals& row_intervals = m_scan_spec_builder.get().row_intervals;
    while (row_intervals.size() && strcmp(row_intervals.front().start, row_key) < 0)
      row_intervals.erase(row_intervals.begin());
  }

 try_again:

  try {
    if (hard) {
      m_range_locator->find_loop(&m_table_identifier, row_key,
                                 &m_next_range_info, m_timer, true);
    }
    else if (!m_loc_cache->lookup(m_table_identifier.id, row_key, &m_next_range_info))
      m_range_locator->find_loop(&m_table_identifier, row_key,
                                 &m_next_range_info, m_timer, false);
  }
  catch (Exception &e) {
    if (e.code() == Error::REQUEST_TIMEOUT)
      HT_THROW2(e.code(), e, e.what());
    poll(0, 0, 1000);
    if (m_timer.expired())
      HT_THROW(Error::REQUEST_TIMEOUT, "");
    goto try_again;
  }

  while (true) {
    set_range_spec(dbuf, range);
    try {
      //HT_ASSERT(!m_outstanding);
      // create scanner asynchronously
      m_create_outstanding = true;
      m_range_server.create_scanner(m_next_range_info.addr, m_table_identifier, range,
          m_scan_spec_builder.get(), &m_handler, m_timer);
    }
    catch (Exception &e) {
      String msg = format("Problem creating scanner on %s[%s..%s]",
                          m_table_identifier.id, range.start_row,
                          range.end_row);
      m_create_outstanding = false;
      if ((e.code() != Error::REQUEST_TIMEOUT
           && e.code() != Error::COMM_NOT_CONNECTED
           && e.code() != Error::COMM_BROKEN_CONNECTION)) {
        HT_ERROR_OUT << e << HT_END;
        HT_THROW2(e.code(), e, msg);
      }
      else if (m_timer.remaining() <= 1000) {
        uint32_t duration  = m_timer.duration();
        HT_ERRORF("Scanner creation request will time out. Initial timer "
                  "duration %d", (int)duration);
        HT_THROW2(Error::REQUEST_TIMEOUT, e, msg + format(". Unable to "
                  "complete request within %d ms", (int)duration));
      }

      poll(0, 0, 1000);

      // try again, the hard way
      m_range_locator->find_loop(&m_table_identifier, row_key,
                                 &m_next_range_info, m_timer, true);
      continue;
    }
    break;
  }
}

void IntervalScannerAsync::abort() {
  HT_ASSERT(m_fetch_outstanding ^ m_create_outstanding);
  m_fetch_outstanding = m_create_outstanding = false;
}

bool IntervalScannerAsync::retry(bool refresh, bool hard) {
  HT_ASSERT(m_fetch_outstanding ^ m_create_outstanding);
  uint32_t wait_time = 3000;

  // RangeServer has already destroyed scanner so we can't refresh
  if (m_fetch_outstanding && refresh) {
    m_fetch_outstanding = false;
    HT_ERROR_OUT << "Table schema can't be refreshed after scanner creation" << HT_END;
    return false;
  }
  // no point refreshing, since the wait will cause a timeout
  if (m_timer.remaining() < wait_time) {
    m_create_outstanding = false;
    uint32_t duration  = m_timer.duration();
    HT_ERRORF("Scanner creation request will time out. Initial timer "
              "duration %d", (int)duration);
    return false;
  }
  try {
    if (refresh)
      m_table->refresh(m_table_identifier, m_schema);
    poll(0, 0, wait_time);

    find_range_and_start_scan(m_create_scanner_row.c_str(), hard);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    m_create_outstanding = m_fetch_outstanding = false;
    return false;
  }
  return true;
}

void IntervalScannerAsync::set_range_spec(DynamicBuffer &dbuf, RangeSpec &range) {
  dbuf.ensure(m_next_range_info.start_row.length()
              + m_next_range_info.end_row.length() + 2);
  range.start_row = (char *)dbuf.add_unchecked(m_next_range_info.start_row.c_str(),
      m_next_range_info.start_row.length()+1);
  range.end_row   = (char *)dbuf.add_unchecked(m_next_range_info.end_row.c_str(),
      m_next_range_info.end_row.length()+1);
}

bool IntervalScannerAsync::handle_result(EventPtr &event, ScanCellsPtr &cells,
    bool &show_results) {
  HT_ASSERT(m_fetch_outstanding ^ m_create_outstanding);
  if (m_create_outstanding)
    m_range_info = m_next_range_info;

  m_fetch_outstanding = m_create_outstanding = false;
  show_results = m_current;

  m_timer.stop();
  if (m_current) {
    load_result(event, cells);
    if (m_eos)
      m_current = false;
    else
      // fetch more data for this scan
      do_readahead();
  }
  // store this event so we can load cells later when this scanner becomes current
  else
    m_event = event;

  return m_eos;
}

void IntervalScannerAsync::load_result(EventPtr &event, ScanCellsPtr &cells) {

  cells = new ScanCells;
  m_eos = cells->load(event, m_schema, m_end_row, m_end_inclusive,
                      m_scan_spec_builder.get().row_limit, &m_rows_seen, m_cur_row,
                      m_rowset, &m_cur_scanner_id, &m_cur_scanner_finished,
                      &m_bytes_scanned);

  if (m_eos && !m_cur_scanner_finished)
    m_range_server.destroy_scanner(m_range_info.addr, m_cur_scanner_id, 0);

  return;
}

bool IntervalScannerAsync::set_current(ScanCellsPtr &cells, bool &show_results) {
  m_current = true;
  show_results = false;

  if (m_create_outstanding)
    return false;

  show_results = true;
  HT_ASSERT(!m_fetch_outstanding);
  load_result(m_event, cells);
  if (!m_eos) {
    do_readahead();
  }
  return m_eos;
}

void IntervalScannerAsync::do_readahead() {

  HT_ASSERT(!m_create_outstanding && !m_fetch_outstanding);
  if (m_eos)
    return;

  // if the current scanner is finished
  if (m_cur_scanner_finished) {
    // if this range ends with END_ROW_MARKER OR the end row of scan is beyond the range end
    if (!strcmp(m_range_info.end_row.c_str(), Key::END_ROW_MARKER) ||
        m_end_row.compare(m_range_info.end_row) <= 0) {
      // this interval scanner is finished
      m_eos = true;
    }
    // else this scanner is done, kick off the next scanner
    else {
      m_create_scanner_row = m_range_info.end_row;
      // if rowset scan update start row for next range
      if (!m_rowset.empty())
        if (m_create_scanner_row.compare(*m_rowset.begin()) < 0)
          m_create_scanner_row = *m_rowset.begin();

      m_create_scanner_row.append(1,1);  // construct row key in next range
      find_range_and_start_scan(m_create_scanner_row.c_str());
    }
  }
  // this scanblock is done, but the scanner still has more scanblocks
  else {
    m_fetch_outstanding = true;
    // request next scanblock and block
    m_timer.start();
    m_range_server.fetch_scanblock(m_range_info.addr, m_cur_scanner_id,
                                   &m_handler, m_timer);
  }
  return;
}
