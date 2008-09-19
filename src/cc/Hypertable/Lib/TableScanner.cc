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
#include "TableScanner.h"

extern "C" {
#include <poll.h>
}

using namespace Hypertable;

/**
 * TODO: Asynchronously destroy dangling scanners on EOS
 */

/**
 */
TableScanner::TableScanner(PropertiesPtr &props_ptr, Comm *comm,
    const TableIdentifier *table_identifier, SchemaPtr &schema_ptr,
    RangeLocatorPtr &range_locator_ptr, const ScanSpec &scan_spec, int timeout)
  : m_eos(false), m_scanneri(0), m_rows_seen(0) {

  IntervalScannerPtr ri_scanner_ptr;
  ScanSpec interval_scan_spec;

  if (timeout == 0 ||
      (timeout = props_ptr->get_int("Hypertable.Client.Timeout", 0)) == 0 ||
      (timeout = props_ptr->get_int("Hypertable.Request.Timeout", 0)) == 0)
    timeout = HYPERTABLE_CLIENT_TIMEOUT;

  Timer timer(timeout);

  if (scan_spec.row_intervals.empty()) {
    if (scan_spec.cell_intervals.empty()) {
      ri_scanner_ptr = new IntervalScanner(props_ptr, comm, table_identifier,
          schema_ptr, range_locator_ptr, scan_spec, timeout);
      m_interval_scanners.push_back(ri_scanner_ptr);
    }
    else {
      for (size_t i=0; i<scan_spec.cell_intervals.size(); i++) {
        scan_spec.base_copy(interval_scan_spec);
        interval_scan_spec.cell_intervals.push_back(
            scan_spec.cell_intervals[i]);
        ri_scanner_ptr = new IntervalScanner(props_ptr, comm, table_identifier,
            schema_ptr, range_locator_ptr, interval_scan_spec, timeout);
        m_interval_scanners.push_back(ri_scanner_ptr);
        ri_scanner_ptr->find_range_and_start_scan(
            scan_spec.cell_intervals[i].start_row, timer);
      }
    }
  }
  else {
    for (size_t i=0; i<scan_spec.row_intervals.size(); i++) {
      scan_spec.base_copy(interval_scan_spec);
      interval_scan_spec.row_intervals.push_back(scan_spec.row_intervals[i]);
      ri_scanner_ptr = new IntervalScanner(props_ptr, comm, table_identifier,
          schema_ptr, range_locator_ptr, interval_scan_spec, timeout);
      m_interval_scanners.push_back(ri_scanner_ptr);
      ri_scanner_ptr->find_range_and_start_scan(
          scan_spec.row_intervals[i].start, timer);
    }
  }

}



bool TableScanner::next(Cell &cell) {

  if (m_eos)
    return false;

 try_again:

  if (m_interval_scanners[m_scanneri]->next(cell))
    return true;

  m_rows_seen += m_interval_scanners[m_scanneri]->get_rows_seen();

  m_scanneri++;

  if (m_scanneri < m_interval_scanners.size()) {
    m_interval_scanners[m_scanneri]->set_rows_seen(m_rows_seen);
    goto try_again;
  }

  m_eos = true;
  return false;
}

void Hypertable::copy(TableScanner &scanner, CellsBuilder &b) {
  Cell cell;

  while (scanner.next(cell))
    b.add(cell);
}
