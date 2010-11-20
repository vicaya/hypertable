/** -*- c++ -*-
 * Copyright (C) 2010 Doug Judd (Hypertable, Inc.)
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
#include "Common/Random.h"
#include "Common/String.h"

#include "Table.h"
#include "TableDumper.h"

extern "C" {
#include <poll.h>
}

using namespace Hypertable;


/**
 */
TableDumper::TableDumper(NamespacePtr &ns, const String &name,
			 ScanSpec &scan_spec, size_t target_node_count)
  : m_scan_spec(scan_spec), m_eod(false), m_rows_seen(0), m_bytes_scanned(0) {
  TableScannerPtr scanner;
  RowInterval ri;

  ns->get_table_splits(name, m_splits);

  // Create random m_ordering
  m_ordering.reserve(m_splits.size());
  for (size_t i=0; i<m_splits.size(); ++i)
    m_ordering.push_back(i);
  uint32_t tmp, n;
  for (size_t base=0,nleft=m_ordering.size(); base<m_ordering.size(); ++base,--nleft) {
    n = Random::number32() % nleft;
    if (n > 0) {
      tmp = m_ordering[base];
      m_ordering[base] = m_ordering[n];
      m_ordering[n] = tmp;
    }
  }

  m_table = ns->open_table(name);

  for (m_next=0; m_next<target_node_count && m_next < m_ordering.size(); m_next++) {
    m_scan_spec.row_intervals.clear();
    ri.start = m_splits[m_ordering[m_next]].start_row;
    ri.start_inclusive = false;
    ri.end = m_splits[m_ordering[m_next]].end_row;
    ri.end_inclusive = true;
    m_scan_spec.row_intervals.push_back(ri);
    scanner = m_table->create_scanner(m_scan_spec);
    m_scanners.push_back( scanner );
  }

  m_scanner_iter = m_scanners.begin();
  if (m_scanner_iter == m_scanners.end())
    m_eod = true;

}


bool TableDumper::next(Cell &cell) {

  if (m_eod)
    return false;

  do {

    if ((*m_scanner_iter)->next(cell)) {
      if (++m_scanner_iter == m_scanners.end())
	m_scanner_iter = m_scanners.begin();
      return true;
    }

    m_bytes_scanned += (*m_scanner_iter)->bytes_scanned();

    // add another scanner
    if (m_next < m_ordering.size()) {
      TableScannerPtr scanner;
      ScanSpec tmp_scan_spec;
      RowInterval ri;
      tmp_scan_spec.columns = m_scan_spec.columns;
      tmp_scan_spec.max_versions = m_scan_spec.max_versions;
      tmp_scan_spec.time_interval = m_scan_spec.time_interval;
      ri.start = m_splits[m_ordering[m_next]].start_row;
      ri.start_inclusive = false;
      ri.end = m_splits[m_ordering[m_next]].end_row;
      ri.end_inclusive = true;
      tmp_scan_spec.row_intervals.push_back(ri);
      scanner = m_table->create_scanner(tmp_scan_spec);
      m_scanners.push_back( scanner );
      m_next++;
    }

    m_scanner_iter = m_scanners.erase(m_scanner_iter);

    if (m_scanner_iter == m_scanners.end()) {
      m_scanner_iter = m_scanners.begin();
      if (m_scanner_iter == m_scanners.end())
	break;
    }

  } while (true);

  m_eod = true;
  return false;
}


void Hypertable::copy(TableDumper &dumper, CellsBuilder &b) {
  Cell cell;

  while (dumper.next(cell))
    b.add(cell);
}
