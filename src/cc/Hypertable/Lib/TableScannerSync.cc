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
#include "TableScannerSync.h"

extern "C" {
#include <poll.h>
}

using namespace Hypertable;


TableScannerSync::TableScannerSync(Comm *comm, Table *table,
    RangeLocatorPtr &range_locator, const ScanSpec &scan_spec,
    uint32_t timeout_ms, bool retry_table_not_found)
  : m_callback(this), m_cur_cells(0), m_cur_cells_index(0), m_cur_cells_size(0),
    m_error(Error::OK),
    m_eos(false), m_bytes_scanned(0) {

  m_queue = new TableScannerQueue;
  ApplicationQueuePtr app_queue = (ApplicationQueue *)m_queue.get();
  m_scanner = new TableScannerAsync(comm, app_queue, table, range_locator, scan_spec,
                                    timeout_ms, retry_table_not_found, &m_callback);
}


bool TableScannerSync::next(Cell &cell) {

  if (m_ungot.row_key) {
    cell = m_ungot;
    m_ungot.row_key = 0;
    return true;
  }

  if (m_eos)
    return false;

  while (true) {

    // serve out ready results
    if (m_cur_cells_index < m_cur_cells_size) {
      m_cur_cells->get_cell_unchecked(cell, m_cur_cells_index);
      m_cur_cells_index++;
      return true;
    }

    if (m_cur_cells != 0) {
      m_eos = m_cur_cells->get_eos();
      if (m_eos)
        return false;
    }

    m_queue->next_result(m_cur_cells, m_error, m_error_msg);
    if (m_error != Error::OK) {
      m_eos = true;
      HT_THROW(m_error, m_error_msg);
    }
    m_cur_cells_size = m_cur_cells->size();
    m_cur_cells_index=0;
  }
}

void TableScannerSync::unget(const Cell &cell) {
  if (m_ungot.row_key)
    HT_THROW_(Error::DOUBLE_UNGET);

  m_ungot = cell;
}

void TableScannerSync::scan_ok(ScanCellsPtr &cells) {
  m_queue->add_cells(cells);
}

void TableScannerSync::scan_error(int error, const String &error_msg) {
  m_queue->set_error(error, error_msg);
}


void Hypertable::copy(TableScannerSync &scanner, CellsBuilder &b) {
  Cell cell;

  while (scanner.next(cell))
    b.add(cell);
}
