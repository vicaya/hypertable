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
#include "Table.h"
#include "TableScanner.h"

extern "C" {
#include <poll.h>
}

using namespace Hypertable;


/**
 * TODO: Asynchronously destroy dangling scanners on EOS
 */
TableScanner::TableScanner(Comm *comm, Table *table,
    RangeLocatorPtr &range_locator, const ScanSpec &scan_spec,
    uint32_t timeout_ms, bool retry_table_not_found)
  : m_eos(false), m_scanneri(0), m_rows_seen(0), m_bytes_scanned(0) {

  HT_ASSERT(timeout_ms);

  IntervalScannerPtr ri_scanner;
  ScanSpec interval_scan_spec;
  Timer timer(timeout_ms);

  if (scan_spec.row_intervals.empty()) {
    if (scan_spec.cell_intervals.empty()) {
      ri_scanner = new IntervalScanner(comm, table, range_locator, scan_spec,
                                       timeout_ms, retry_table_not_found);
      m_interval_scanners.push_back(ri_scanner);
    }
    else {
      for (size_t i=0; i<scan_spec.cell_intervals.size(); i++) {
        scan_spec.base_copy(interval_scan_spec);
        interval_scan_spec.cell_intervals.push_back(
            scan_spec.cell_intervals[i]);
        ri_scanner = new IntervalScanner(comm, table, range_locator,
            interval_scan_spec, timeout_ms, retry_table_not_found);
        m_interval_scanners.push_back(ri_scanner);
      }
    }
  }
  else {
    for (size_t i=0; i<scan_spec.row_intervals.size(); i++) {
      scan_spec.base_copy(interval_scan_spec);
      interval_scan_spec.row_intervals.push_back(scan_spec.row_intervals[i]);
      ri_scanner = new IntervalScanner(comm, table, range_locator,
          interval_scan_spec, timeout_ms, retry_table_not_found);
      m_interval_scanners.push_back(ri_scanner);
    }
  }
}


bool TableScanner::next(Cell &cell) {

  if (m_eos)
    return false;

  if (m_ungot.row_key) {
    cell = m_ungot;
    m_ungot.row_key = 0;
    return true;
  }

  do {
    if (m_interval_scanners[m_scanneri]->next(cell))
      return true;

    m_rows_seen += m_interval_scanners[m_scanneri]->get_rows_seen();
    m_bytes_scanned += m_interval_scanners[m_scanneri]->bytes_scanned();

    m_scanneri++;

    if (m_scanneri < m_interval_scanners.size())
      m_interval_scanners[m_scanneri]->set_rows_seen(m_rows_seen);
    else
      break;
  } while (true);

  m_eos = true;
  return false;
}


void TableScanner::unget(const Cell &cell) {
  if (m_ungot.row_key)
    HT_THROW_(Error::DOUBLE_UNGET);

  m_ungot = cell;
}


void Hypertable::copy(TableScanner &scanner, CellsBuilder &b) {
  Cell cell;

  while (scanner.next(cell))
    b.add(cell);
}
