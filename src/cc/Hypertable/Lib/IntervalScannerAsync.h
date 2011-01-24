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

#ifndef HYPERTABLE_INTERVALSCANNERASYNC_H
#define HYPERTABLE_INTERVALSCANNERASYNC_H

#include "Common/Properties.h"
#include "Common/ReferenceCount.h"
#include "Common/StringExt.h"

#include "ScanCells.h"
#include "RangeLocator.h"
#include "RangeServerClient.h"
#include "ScanBlock.h"
#include "Types.h"
#include "TableScannerDispatchHandler.h"

namespace Hypertable {

  class Table;
  class TableScannerAsync;

  class IntervalScannerAsync : public ReferenceCount {

  public:
    /**
     * Constructs a IntervalScannerAsync object.
     *
     * @param comm pointer to the Comm layer
     * @param app_queue Application Queue pointer
     * @param table ponter to the table
     * @param range_locator smart pointer to range locator
     * @param scan_spec reference to scan specification object
     * @param timeout_ms maximum time in milliseconds to allow scanner
     *        methods to execute before throwing an exception
     * @param retry_table_not_found whether to retry upon errors caused by
     *        drop/create tables with the same name
     * @param current is this scanner the current scanner being used
     * @param scanner pointer to table scanner
     * @param id scanner id
     */
    IntervalScannerAsync(Comm *comm, ApplicationQueuePtr &app_queue, Table *table,
                         RangeLocatorPtr &range_locator,
                         const ScanSpec &scan_spec, uint32_t timeout_ms,
                         bool retry_table_not_found, bool current,
                         TableScannerAsync *scanner, int id);

    virtual ~IntervalScannerAsync();

    int32_t get_rows_seen() { return m_rows_seen; }
    void    set_rows_seen(int32_t n) { m_rows_seen = n; }

    void abort();
    bool retry(bool refresh, bool hard);
    bool handle_result(EventPtr &event, ScanCellsPtr &cells, bool &show_results);
    bool set_current(ScanCellsPtr &cells, bool &show_results);

    int64_t bytes_scanned() { return m_bytes_scanned; }

  private:
    void do_readahead();
    void init(const ScanSpec &);
    void find_range_and_start_scan(const char *row_key, bool hard=false);
    void load_result(EventPtr &event, ScanCellsPtr &cells);
    void set_range_spec(DynamicBuffer &dbuf, RangeSpec &range);

    Comm               *m_comm;
    Table              *m_table;
    SchemaPtr           m_schema;
    RangeLocatorPtr     m_range_locator;
    LocationCachePtr    m_loc_cache;
    ScanSpecBuilder     m_scan_spec_builder;
    RangeServerClient   m_range_server;
    TableIdentifierManaged m_table_identifier;
    bool                m_eos;
    String              m_cur_row;
    String              m_create_scanner_row;
    RangeLocationInfo   m_range_info;
    RangeLocationInfo   m_next_range_info;
    bool                m_fetch_outstanding;
    bool                m_create_outstanding;
    EventPtr            m_event;
    String              m_start_row;
    String              m_end_row;
    bool                m_end_inclusive;
    int32_t             m_rows_seen;
    uint32_t            m_timeout_ms;
    bool                m_retry_table_not_found;
    bool                m_current;
    int64_t             m_bytes_scanned;
    CstrSet             m_rowset;
    TableScannerDispatchHandler m_handler;
    TableScannerAsync  *m_scanner;
    int                 m_id;
    Timer               m_timer;
    bool                m_cur_scanner_finished;
    int                 m_cur_scanner_id;
  };

  typedef intrusive_ptr<IntervalScannerAsync> IntervalScannerAsyncPtr;

} // namespace Hypertable

#endif // HYPERTABLE_INTERVALSCANNERASYNC_H
