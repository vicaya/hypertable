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

#include "AsyncComm/DispatchHandlerSynchronizer.h"

#include "Cell.h"
#include "RangeLocator.h"
#include "RangeServerClient.h"
#include "ScanBlock.h"
#include "Schema.h"
#include "TableScanner.h"
#include "Types.h"

namespace Hypertable {

  class TableScanner : public ReferenceCount {

  public:
    TableScanner(ConnectionManagerPtr &conn_manager_ptr, TableIdentifierT *table_ptr, SchemaPtr &schema_ptr, RangeLocatorPtr &range_locator_ptr, ScanSpecificationT &scan_spec);
    virtual ~TableScanner();

    bool next(CellT &cell);

  private:

    void find_range_and_start_scan(const char *row_key);
    
    ConnectionManagerPtr m_conn_manager_ptr;
    TableIdentifierT     m_table;
    SchemaPtr            m_schema_ptr;
    RangeLocatorPtr      m_range_locator_ptr;
    ScanSpecificationT   m_scan_spec;
    RangeServerClient    m_range_server;
    bool                m_started;
    bool                m_eos;
    ScanBlock           m_scanblock;
    std::string         m_cur_row_key;
    RangeLocationInfo   m_range_info;
    struct sockaddr_in  m_cur_addr;
    RangeT              m_cur_range;
    bool                m_readahead;
    bool                m_fetch_outstanding;
    DispatchHandlerSynchronizer  m_sync_handler;
    EventPtr            m_event_ptr;
  };
  typedef boost::intrusive_ptr<TableScanner> TableScannerPtr;
}

#endif // HYPERTABLE_TABLESCANNER_H
