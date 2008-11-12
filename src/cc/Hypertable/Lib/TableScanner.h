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

#include "Common/Config.h"
#include "Common/ReferenceCount.h"

#include "AsyncComm/DispatchHandlerSynchronizer.h"

#include "Cells.h"
#include "RangeLocator.h"
#include "RangeServerClient.h"
#include "IntervalScanner.h"
#include "ScanBlock.h"
#include "Schema.h"
#include "Types.h"
#include "Table.h"

namespace Hypertable {

  class TableScanner : public ReferenceCount {

  public:
    /**
     * Constructs a TableScanner object.
     *
     * @param comm pointer to the Comm layer
     * @param table pointer to the table object
     * @param schema smart pointer to schema object for table
     * @param range_locator smart pointer to range locator
     * @param scan_spec reference to scan specification object
     * @param timeout_ms maximum time in milliseconds to allow scanner
     *        methods to execute before throwing an exception
     */
    TableScanner(Comm *comm, Table *table, SchemaPtr &schema,
                 RangeLocatorPtr &range_locator, const ScanSpec &scan_spec,
                 uint32_t timeout_ms);

    bool next(Cell &cell);

  private:
    std::vector<IntervalScannerPtr>  m_interval_scanners;

    bool      m_eos;
    size_t    m_scanneri;
    int64_t   m_rows_seen;
    TablePtr  m_table;
  };

  typedef intrusive_ptr<TableScanner> TableScannerPtr;

  void copy(TableScanner &, CellsBuilder &);
  inline void copy(TableScannerPtr &p, CellsBuilder &v) { copy(*p.get(), v); }

} // namespace Hypertable

#endif // HYPERTABLE_TABLESCANNER_H
