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

#ifndef HYPERTABLE_TABLESCANNER_H
#define HYPERTABLE_TABLESCANNER_H

#include "Common/ReferenceCount.h"

#include "AsyncComm/DispatchHandlerSynchronizer.h"

#include "Cells.h"
#include "RangeLocator.h"
#include "RangeServerClient.h"
#include "IntervalScanner.h"
#include "ScanBlock.h"
#include "Schema.h"
#include "Types.h"

namespace Hypertable {

  class Table;

  class TableScanner : public ReferenceCount {

  public:
    /**
     * Constructs a TableScanner object.
     *
     * @param comm pointer to the Comm layer
     * @param table pointer to the table object
     * @param range_locator smart pointer to range locator
     * @param scan_spec reference to scan specification object
     * @param timeout_ms maximum time in milliseconds to allow scanner
     *        methods to execute before throwing an exception
     * @param retry_table_not_found whether to retry upon errors caused by
     *        drop/create tables with the same name
     */
    TableScanner(Comm *comm, Table *table, RangeLocatorPtr &range_locator,
                 const ScanSpec &scan_spec, uint32_t timeout_ms,
                 bool retry_table_not_found);

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

  private:
    std::vector<IntervalScannerPtr>  m_interval_scanners;

    bool      m_eos;
    size_t    m_scanneri;
    int64_t   m_rows_seen;
    Cell      m_ungot;
  };

  typedef intrusive_ptr<TableScanner> TableScannerPtr;

  void copy(TableScanner &, CellsBuilder &);
  inline void copy(TableScannerPtr &p, CellsBuilder &v) { copy(*p.get(), v); }

} // namespace Hypertable

#endif // HYPERTABLE_TABLESCANNER_H
