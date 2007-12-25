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

#include "Common/Properties.h"
#include "Common/ReferenceCount.h"

#include "AsyncComm/DispatchHandlerSynchronizer.h"

#include "Cell.h"
#include "RangeLocator.h"
#include "RangeServerClient.h"
#include "ScanBlock.h"
#include "Schema.h"
#include "Types.h"

namespace Hypertable {

  class TableScanner : public ReferenceCount {

  public:
    TableScanner(PropertiesPtr &props_ptr, Comm *comm, TableIdentifierT *table_identifier, SchemaPtr &schema_ptr, RangeLocatorPtr &range_locator_ptr, ScanSpecificationT &scan_spec);
    virtual ~TableScanner();

    bool next(CellT &cell);

  private:

    void find_range_and_start_scan(const char *row_key);

    Comm               *m_comm;
    SchemaPtr           m_schema_ptr;
    RangeLocatorPtr     m_range_locator_ptr;
    ScanSpecificationT  m_scan_spec;
    RangeServerClient   m_range_server;
    std::string         m_table_name;
    TableIdentifierT    m_table_identifier;
    bool                m_started;
    bool                m_eos;
    ScanBlock           m_scanblock;
    std::string         m_cur_row;
    RangeLocationInfo   m_range_info;
    struct sockaddr_in  m_cur_addr;
    bool                m_readahead;
    bool                m_fetch_outstanding;
    DispatchHandlerSynchronizer  m_sync_handler;
    EventPtr            m_event_ptr;
    uint32_t            m_rows_seen;
  };
  typedef boost::intrusive_ptr<TableScanner> TableScannerPtr;
}

#endif // HYPERTABLE_TABLESCANNER_H
