/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This file is part of Hypertable.
 * 
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#ifndef HYPERTABLE_TABLESCANNER_H
#define HYPERTABLE_TABLESCANNER_H

#include "Common/ReferenceCount.h"

#include "Cell.h"
#include "RangeLocator.h"
#include "ScanBlock.h"
#include "Schema.h"
#include "TableScanner.h"
#include "Types.h"

namespace Hypertable {

  class TableScanner : public ReferenceCount {

  public:
    TableScanner(SchemaPtr &schema_ptr, RangeLocatorPtr &range_locator_ptr, ScanSpecificationT &scan_spec);
    virtual ~TableScanner();

    bool next(CellT &cell);

  private:
    SchemaPtr           m_schema_ptr;
    RangeLocatorPtr     m_range_locator_ptr;
    ScanSpecificationT  m_scan_spec;
    bool                m_eos;
    ScanBlock           m_scanblock;
  };
  typedef boost::intrusive_ptr<TableScanner> TableScannerPtr;
}

#endif // HYPERTABLE_TABLESCANNER_H
