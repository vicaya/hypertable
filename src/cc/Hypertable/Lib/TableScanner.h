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

#include "Common/Properties.h"
#include "Common/ReferenceCount.h"

#include "AsyncComm/DispatchHandlerSynchronizer.h"

#include "Cell.h"
#include "RangeLocator.h"
#include "RangeServerClient.h"
#include "IntervalScanner.h"
#include "ScanBlock.h"
#include "Schema.h"
#include "Types.h"

namespace Hypertable {

  class TableScanner : public ReferenceCount {

  public:
    /**
     * Constructs a TableScanner object.
     *
     * @param props_ptr smart pointer to configuration properties object
     * @param comm pointer to the Comm layer
     * @param table_identifier pointer to the identifier of the table being mutated
     * @param schema_ptr smart pointer to schema object for table
     * @param range_locator_ptr smart pointer to range locator
     * @param scan_spec reference to scan specification object
     * @param timeout maximum time in seconds to allow scanner methods to execute before throwing an exception
     */
    TableScanner(PropertiesPtr &props_ptr, Comm *comm, TableIdentifier *table_identifier, SchemaPtr &schema_ptr, RangeLocatorPtr &range_locator_ptr, ScanSpec &scan_spec, int timeout);

    bool next(Cell &cell);

  private:

    std::vector<IntervalScannerPtr>  m_interval_scanners;
    bool      m_eos;
    size_t    m_scanneri;
    int32_t   m_rows_seen;
  };
  typedef boost::intrusive_ptr<TableScanner> TableScannerPtr;
}

#endif // HYPERTABLE_TABLESCANNER_H
