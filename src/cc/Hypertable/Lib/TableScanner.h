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

#ifndef HYPERTABLE_TABLESCANNERSYNC_H
#define HYPERTABLE_TABLESCANNERSYNC_H

#include <list>
#include "Common/ReferenceCount.h"
#include "TableScannerQueue.h"
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
    TableScanner(Comm *comm, Table *table,  RangeLocatorPtr &range_locator,
                     const ScanSpec &scan_spec, uint32_t timeout_ms,
                     bool retry_table_not_found);

    /**
     * Cancel asynchronous scanner and keep dealing with RangeServer responses
     * till async scanner is done
     */
    ~TableScanner() {
      m_scanner->cancel();
      if (!m_scanner->is_complete()) {
        ScanCellsPtr cells;
        int error=Error::OK;
        String error_msg;
        while (!m_scanner->is_complete())
          m_queue->next_result(cells, &error, error_msg);
      }
    }

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
     */
    void scan_error(int error, const String &error_msg);

    TableScannerQueuePtr m_queue;
    TableScannerAsyncPtr m_scanner;
    TableScannerCallback m_callback;
    ScanCellsPtr m_cur_cells;
    size_t m_cur_cells_index;
    size_t m_cur_cells_size;
    int m_error;
    String m_error_msg;
    bool m_eos;
    Cell m_ungot;
    int64_t m_bytes_scanned;
  };
  typedef intrusive_ptr<TableScanner> TableScannerPtr;

  void copy(TableScanner &scanner, CellsBuilder &b);
  inline void copy(TableScannerPtr &p, CellsBuilder &v) { copy(*p.get(), v); }
} // namesapce Hypertable

#endif // HYPERTABLE_TABLESCANNER_H
