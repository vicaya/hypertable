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

#ifndef HYPERTABLE_INTERVALSCANNER_H
#define HYPERTABLE_INTERVALSCANNER_H

#include "Common/Properties.h"
#include "Common/ReferenceCount.h"

#include "AsyncComm/DispatchHandlerSynchronizer.h"

#include "Cell.h"
#include "RangeLocator.h"
#include "RangeServerClient.h"
#include "ScanBlock.h"
#include "Types.h"

namespace Hypertable {

  class Table;

  class IntervalScanner : public ReferenceCount {

  public:
    /**
     * Constructs a IntervalScanner object.
     *
     * @param comm pointer to the Comm layer
     * @param table ponter to the table
     * @param range_locator smart pointer to range locator
     * @param scan_spec reference to scan specification object
     * @param timeout_ms maximum time in milliseconds to allow scanner
     *        methods to execute before throwing an exception
     * @param retry_table_not_found whether to retry upon errors caused by
     *        drop/create tables with the same name
     */
    IntervalScanner(Comm *comm, Table *table, RangeLocatorPtr &range_locator,
                    const ScanSpec &scan_spec, uint32_t timeout_ms,
                    bool retry_table_not_found);

    virtual ~IntervalScanner();

    bool next(Cell &cell);

    int32_t get_rows_seen() { return m_rows_seen; }
    void    set_rows_seen(int32_t n) { m_rows_seen = n; }

    void find_range_and_start_scan(const char *row_key, Timer &timer, bool synchronous=false);

    int64_t bytes_scanned() { return m_bytes_scanned; }

  private:
    void init(const ScanSpec &, Timer &);

    Comm               *m_comm;
    Table              *m_table;
    SchemaPtr           m_schema;
    RangeLocatorPtr     m_range_locator;
    LocationCachePtr    m_loc_cache;
    ScanSpecBuilder     m_scan_spec_builder;
    RangeServerClient   m_range_server;
    TableIdentifierManaged m_table_identifier;
    bool                m_eos;
    ScanBlock           m_scanblock;
    String              m_cur_row;
    String              m_create_scanner_row;
    RangeLocationInfo   m_range_info;
    struct sockaddr_in  m_cur_addr;
    bool                m_readahead;
    bool                m_fetch_outstanding;
    bool                m_create_scanner_outstanding;
    DispatchHandlerSynchronizer  m_sync_handler;
    EventPtr            m_event;
    String              m_start_row;
    String              m_end_row;
    bool                m_end_inclusive;
    int32_t             m_rows_seen;
    uint32_t            m_timeout_ms;
    bool                m_retry_table_not_found;
    int64_t             m_bytes_scanned;
  };

  typedef intrusive_ptr<IntervalScanner> IntervalScannerPtr;

} // namespace Hypertable

#endif // HYPERTABLE_INTERVALSCANNER_H
