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
#include <vector>

#include "Common/Error.h"
#include "Common/String.h"

#include "Table.h"
#include "TableScannerAsync.h"

extern "C" {
#include <poll.h>
}

using namespace Hypertable;


/**
 *
 */
TableScannerAsync::TableScannerAsync(Comm *comm, ApplicationQueuePtr &app_queue, Table *table,
    RangeLocatorPtr &range_locator, const ScanSpec &scan_spec,
    uint32_t timeout_ms, bool retry_table_not_found, ResultCallback *cb)
  : m_bytes_scanned(0), m_cb(cb), m_current_scanner(0),
    m_outstanding(0), m_error(Error::OK), m_table(table), m_scan_spec(scan_spec) {

  ScopedLock lock(m_mutex);

  HT_ASSERT(timeout_ms);
  int scanner_id = 0;
  IntervalScannerAsyncPtr ri_scanner;
  ScanSpec interval_scan_spec;
  Timer timer(timeout_ms);
  bool current_set = false;

  m_table_name = table->get_name();
  m_cb->increment_outstanding();
  m_cb->register_scanner(this);

  try {
    if (scan_spec.row_intervals.empty()) {
      if (scan_spec.cell_intervals.empty()) {
        m_outstanding++;
        ri_scanner = new IntervalScannerAsync(comm, app_queue, table, range_locator, scan_spec,
            timeout_ms, retry_table_not_found, !current_set, this, scanner_id++);

        current_set = true;
        m_interval_scanners.push_back(ri_scanner);
      }
      else {
        for (size_t i=0; i<scan_spec.cell_intervals.size(); i++) {
          scan_spec.base_copy(interval_scan_spec);
          interval_scan_spec.cell_intervals.push_back(
              scan_spec.cell_intervals[i]);
          m_outstanding++;
          ri_scanner = new IntervalScannerAsync(comm, app_queue, table, range_locator,
              interval_scan_spec, timeout_ms, retry_table_not_found, !current_set, this,
              scanner_id++);
          current_set = true;
          m_interval_scanners.push_back(ri_scanner);
        }
      }
    }
    else if (scan_spec.scan_and_filter_rows) {
      ScanSpec rowset_scan_spec;
      scan_spec.base_copy(rowset_scan_spec);
      rowset_scan_spec.row_intervals.reserve(scan_spec.row_intervals.size());
      foreach (const RowInterval& ri, scan_spec.row_intervals) {
        if (ri.start != ri.end && strcmp(ri.start, ri.end) != 0) {
          scan_spec.base_copy(interval_scan_spec);
          interval_scan_spec.row_intervals.push_back(ri);
          m_outstanding++;
          ri_scanner = new IntervalScannerAsync(comm, app_queue, table, range_locator,
              interval_scan_spec, timeout_ms, retry_table_not_found, !current_set,
              this, scanner_id++);
          current_set = true;
          m_interval_scanners.push_back(ri_scanner);
        }
        else
          rowset_scan_spec.row_intervals.push_back(ri);
      }
      if (rowset_scan_spec.row_intervals.size()) {
       m_outstanding++;
       ri_scanner = new IntervalScannerAsync(comm, app_queue, table, range_locator,
            rowset_scan_spec, timeout_ms, retry_table_not_found, !current_set,
            this, scanner_id++);
        current_set = true;
        m_interval_scanners.push_back(ri_scanner);
      }
    }
    else {
      for (size_t i=0; i<scan_spec.row_intervals.size(); i++) {
        scan_spec.base_copy(interval_scan_spec);
        interval_scan_spec.row_intervals.push_back(scan_spec.row_intervals[i]);
        m_outstanding++;
        ri_scanner = new IntervalScannerAsync(comm, app_queue, table, range_locator,
            interval_scan_spec, timeout_ms, retry_table_not_found, !current_set,
            this, scanner_id++);
        current_set = true;
        m_interval_scanners.push_back(ri_scanner);
      }
    }
  }
  catch (Exception &e) {
    m_error = e.code();
    m_outstanding--;
    cb->decrement_outstanding();
    throw;
  }
}

TableScannerAsync::~TableScannerAsync() {
  wait_for_completion();
}

void TableScannerAsync::handle_error(int scanner_id, int error, const String &error_msg) {
  ScopedLock lock(m_mutex);
  bool abort = false;

  if (m_error != Error::OK)
    return;
  switch(error) {
    case (Error::TABLE_NOT_FOUND):
    case (Error::RANGESERVER_TABLE_NOT_FOUND):
      if (m_retry_table_not_found)
        abort = !(m_interval_scanners[scanner_id]->retry(true, true));
      else {
        m_interval_scanners[scanner_id]->abort();
        abort = true;
      }
      break;
    case (Error::RANGESERVER_GENERATION_MISMATCH):
      abort = !(m_interval_scanners[scanner_id]->retry(true, false));
      break;

    case(Error::REQUEST_TIMEOUT):
      handle_timeout(scanner_id, error_msg);
      return;

    case(Error::RANGESERVER_RANGE_NOT_FOUND):
    case(Error::COMM_NOT_CONNECTED):
    case(Error::COMM_BROKEN_CONNECTION):
      abort = !(m_interval_scanners[scanner_id]->retry(false, true));
      break;

    default:
      Exception e(error, error_msg);
      HT_ERROR_OUT <<  e << HT_END;
      m_interval_scanners[scanner_id]->abort();
      abort = true;
  }

  if (abort) {
    Exception e(error, error_msg);
    m_error = error;
    m_error_msg = error_msg;
    HT_ERROR_OUT << e << HT_END;

    maybe_callback_error(true, true);
  }
}

void TableScannerAsync::handle_timeout(int scanner_id, const String &error_msg) {
  ScopedLock lock(m_mutex);
  if (m_error != Error::OK)
    return;
  HT_ERROR_OUT << "Unable to complete scan request within " << m_timeout_ms
               << " - " << error_msg << HT_END;
  m_error = Error::REQUEST_TIMEOUT;
  maybe_callback_error(true, true);
}

void TableScannerAsync::handle_result(int scanner_id, EventPtr &event) {
  ScopedLock lock(m_mutex);
  ScanCellsPtr cells;

  // If the scan already encountered an error don't bother calling into callback anymore
  if (m_error != Error::OK) {
    maybe_callback_error(true, false);
    return;
  }

  bool next;
  bool do_callback;
  int current_scanner = scanner_id;

  try {
    // send results to interval scanner
    next = m_interval_scanners[scanner_id]->handle_result(event, cells, do_callback);
    // if error then remember it and we are done with this interval scanner
    maybe_callback_ok(next, do_callback, cells);

    while (next && m_outstanding && !m_error) {
      current_scanner++;
      m_current_scanner = current_scanner;
      next = m_interval_scanners[current_scanner]->set_current(cells, do_callback);
      HT_ASSERT(do_callback || !next);
      maybe_callback_ok(next, do_callback, cells);
    }
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    m_error = e.code();
    m_error_msg = e.what();
    maybe_callback_error(true, true);
    throw;
  }
}

void TableScannerAsync::maybe_callback_error(bool next, bool do_callback) {
  bool eos = false;
  // ok to update m_outstanding since caller has locked mutex
  if (next) {
    m_outstanding--;
  }

  if (m_outstanding == 0)
    eos = true;

  if (do_callback)
    m_cb->scan_error(this, m_error, m_error_msg, eos);

  if (eos) {
    m_cb->deregister_scanner(this);
    m_cb->decrement_outstanding();
    m_cond.notify_all();
  }
}

void TableScannerAsync::maybe_callback_ok(bool next, bool do_callback, ScanCellsPtr &cells) {
  bool eos = false;
  // ok to update m_outstanding since caller has locked mutex
  if (next)
    m_outstanding--;


  if (m_outstanding == 0) {
    eos = true;
  }

  if (do_callback) {
    if (eos)
      cells->set_eos();
    m_cb->scan_ok(this, cells);
  }

  if (m_outstanding==0) {
    m_cb->deregister_scanner(this);
    m_cb->decrement_outstanding();
    m_cond.notify_all();
  }
}

void TableScannerAsync::wait_for_completion() {
  ScopedLock lock(m_mutex);
  while (m_outstanding!=0)
    m_cond.wait(lock);
  return;
}

String TableScannerAsync::get_table_name() const {
  return m_table->get_name();
}


