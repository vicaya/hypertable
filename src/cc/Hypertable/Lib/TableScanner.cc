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
#include "TableScanner.h"

extern "C" {
#include <poll.h>
}

using namespace Hypertable;


TableScanner::TableScanner(Comm *comm, ApplicationQueuePtr &app_queue, Table *table,
    RangeLocatorPtr &range_locator, const ScanSpec &scan_spec,
    uint32_t timeout_ms, bool retry_table_not_found, size_t max_queued_results)
  : m_callback(this), m_cur_cells_index(0), m_error(Error::OK),
    m_queue_capacity(max_queued_results), m_started(false), m_eos(false), m_bytes_scanned(0) {

  HT_ASSERT(m_queue_capacity > 0);
  m_scanner = new TableScannerAsync(comm, app_queue, table, range_locator, scan_spec,
                                    timeout_ms, retry_table_not_found, &m_callback);
}


bool TableScanner::next(Cell &cell) {

  {
    ScopedLock lock(m_mutex);
    if (m_error != Error::OK)
      HT_THROW(m_error, m_error_msg);
    if (m_eos)
      return false;
  }

  if (m_ungot.row_key) {
    cell = m_ungot;
    m_ungot.row_key = 0;
    return true;
  }

  while (true) {
    // if we have cells ready serve them out
    if (m_cur_cells_index < m_cur_cells.size()) {
      cell = m_cur_cells[m_cur_cells_index++];
      return true;
    }
    else {
      ScopedLock lock (m_mutex);
      // pop the front of the queue check for eos, signal and wait till the queue is not empty
      if (!is_empty() && m_started) {
        m_eos = m_queue.front()->get_eos();
        m_queue.pop_front();
        m_cond.notify_one();
      }
      if (m_eos)
        return false;
      while (is_empty() && m_error == Error::OK) {
        m_cond.wait(lock);
      }

      if (m_error != Error::OK)
        HT_THROW(m_error, m_error_msg);
      m_queue.front()->get(m_cur_cells);
      m_cur_cells_index=0;
      m_started = true;
    }
  }

}

void TableScanner::unget(const Cell &cell) {
  if (m_ungot.row_key)
    HT_THROW_(Error::DOUBLE_UNGET);

  m_ungot = cell;
}

void TableScanner::scan_ok(ScanCellsPtr &cells) {
  ScopedLock lock(m_mutex);
  while (m_error == Error::OK && is_full()) {
    m_cond.wait(lock);
  }
  if (m_error == Error::OK) {
    m_queue.push_back(cells);
  }
  m_cond.notify_one();
}

void TableScanner::scan_error(int error, const String &error_msg, bool eos) {
  ScopedLock lock(m_mutex);

  m_error = error;
  m_error_msg = error_msg;
  if (eos)
    m_eos = eos;
  m_cond.notify_one();
}


void Hypertable::copy(TableScanner &scanner, CellsBuilder &b) {
  Cell cell;

  while (scanner.next(cell))
    b.add(cell);
}
