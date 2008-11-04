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
#include "Schema.h"
#include "Types.h"

namespace Hypertable {

  class IntervalScanner : public ReferenceCount {

  public:
    /**
     * Constructs a IntervalScanner object.
     *
     * @param comm pointer to the Comm layer
     * @param table_identifier pointer to the identifier of the table
     * @param schema_ptr smart pointer to schema object for table
     * @param range_locator_ptr smart pointer to range locator
     * @param scan_spec reference to scan specification object
     * @param timeout maximum time in seconds to allow scanner methods to
     *        execute before throwing an exception
     */
    IntervalScanner(Comm *comm, const TableIdentifier *, SchemaPtr &,
                    RangeLocatorPtr &, const ScanSpec &, int timeout);

    virtual ~IntervalScanner();

    bool next(Cell &cell);

    int32_t get_rows_seen() { return m_rows_seen; }
    void    set_rows_seen(int32_t n) { m_rows_seen = n; }

    void find_range_and_start_scan(const char *row_key, Timer &timer);

  private:

    Comm               *m_comm;
    SchemaPtr           m_schema_ptr;
    RangeLocatorPtr     m_range_locator_ptr;
    LocationCachePtr    m_cache_ptr;
    ScanSpecBuilder     m_scan_spec_builder;
    RangeServerClient   m_range_server;
    TableIdentifierManaged m_table_identifier;
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
    std::string         m_start_row;
    std::string         m_end_row;
    bool                m_end_inclusive;
    int32_t             m_rows_seen;
    int                 m_timeout;
  };

  typedef intrusive_ptr<IntervalScanner> IntervalScannerPtr;

} // namespace Hypertable

#endif // HYPERTABLE_INTERVALSCANNER_H
