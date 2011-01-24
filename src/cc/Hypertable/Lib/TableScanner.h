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

#ifndef HYPERTABLE_TABLESCANNER_H
#define HYPERTABLE_TABLESCANNER_H

#include <list>
#include "Common/ReferenceCount.h"
#include "TableScannerAsync.h"
#include "ResultCallback.h"
#include "TableScannerCallback.h"
#include "ScanCells.h"

namespace Hypertable {

  /**
   */
  class TableScanner : public ReferenceCount {

  public:

    /**
     * Constructs a TableScanner object.
     *
     * @param comm pointer to the Comm layer
     * @param app_queue pointer to ApplicationQueue
     * @param table pointer to the table object
     * @param range_locator smart pointer to range locator
     * @param scan_spec reference to scan specification object
     * @param timeout_ms maximum time in milliseconds to allow scanner
     *        methods to execute before throwing an exception
     * @param retry_table_not_found whether to retry upon errors caused by
     *        drop/create tables with the same name
     * @param max_queued_results max number of results to enqueue before blocking
     */
    TableScanner(Comm *comm, ApplicationQueuePtr &app_queue, Table *table,
                     RangeLocatorPtr &range_locator,
                     const ScanSpec &scan_spec, uint32_t timeout_ms,
                     bool retry_table_not_found, size_t queue_capacity);

    /**
     * Get the next cell.
     *
     * @param cell The cell object to contain the result
     * @return true for success
     */
    bool next(Cell &cell);

    /**
     * Unget one cell.
     *
     * Only one cell that's previously obtained from #next can be unget. Mostly
     * designed to provide one cell look-ahead for downstream wrapper to
     * implement #next_row.
     *
     * @param cell the cell object to unget
     * @throws exception if unget is called twice without intervening next
     */
    void unget(const Cell &cell);

    /**
     * Returns number of bytes scanned
     *
     * @return byte count
     */
    int64_t bytes_scanned() { return m_bytes_scanned; }

  private:

    friend class TableScannerCallback;
    typedef std::list<ScanCellsPtr> CellsQueue;
    /**
     * Callback method for successful scan
     *
     * @param scanner
     * @param cells vector of returned cells
     */
    void scan_ok(ScanCellsPtr &cells);

    /**
     * Callback method for scan errors
     *
     * @param error
     * @param error_msg
     * @param eos
     */
    void scan_error(int error, const String &error_msg, bool eos);

    bool is_full() { return (m_queue_capacity <= m_queue.size()); }

    bool is_empty() { return m_queue.empty(); }

    TableScannerAsyncPtr m_scanner;
    TableScannerCallback m_callback;
    Cells m_cur_cells;
    size_t m_cur_cells_index;
    int m_error;
    String m_error_msg;
    CellsQueue m_queue;
    size_t m_queue_capacity;
    bool m_started;
    bool m_eos;
    Cell m_ungot;
    int64_t m_bytes_scanned;
    Mutex m_mutex;
    boost::condition m_cond;
  };
  typedef intrusive_ptr<TableScanner> TableScannerPtr;

  void copy(TableScanner &scanner, CellsBuilder &b);
  inline void copy(TableScannerPtr &p, CellsBuilder &v) { copy(*p.get(), v); }
} // namesapce Hypertable

#endif // HYPERTABLE_TABLESCANNER_H
