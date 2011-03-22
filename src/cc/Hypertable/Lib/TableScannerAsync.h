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

#ifndef HYPERTABLE_TABLESCANNERASYNC_H
#define HYPERTABLE_TABLESCANNERASYNC_H

#include "Common/ReferenceCount.h"

#include "AsyncComm/DispatchHandlerSynchronizer.h"

#include "Cells.h"
#include "RangeLocator.h"
#include "RangeServerClient.h"
#include "IntervalScannerAsync.h"
#include "ScanBlock.h"
#include "Schema.h"
#include "Types.h"
#include "ResultCallback.h"

namespace Hypertable {

  class Table;

  class TableScannerAsync : public ReferenceCount {

  public:
    /**
     * Constructs a TableScannerAsync object.
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
     * @param cb callback to be notified when results arrive
     */
    TableScannerAsync(Comm *comm, ApplicationQueuePtr &app_queue, Table *table,
                      RangeLocatorPtr &range_locator,
                      const ScanSpec &scan_spec, uint32_t timeout_ms,
                      bool retry_table_not_found, ResultCallback *cb);

    ~TableScannerAsync();

    /**
     * Cancels the scanner
     */
    void cancel();

    bool is_cancelled();

    /**
     *
     */
    bool is_complete() {
      ScopedLock lock(m_mutex);
      return m_outstanding == 0;
    }

    /**
     * Deal with results of a scanner
     * @param scanner_id id of the scanner which triggered the error
     * @param event event with results
     * @param is_create true if this is event is for a create_scanner request
     */
    void handle_result(int scanner_id, EventPtr &event, bool is_result);

    /**
     * Deal with errors
     *
     * @param scanner_id id of the scanner which triggered the error
     * @param error error code
     * @param error_msg error message
     * @param is_create true if this is event is for a create_scanner request
     */
    void handle_error(int scanner_id, int error, const String &error_msg, bool is_create);

    /**
     * Deal with timeouts
     *
     * @param scanner_id id of the scanner which triggered the error
     * @param error_msg error message
     * @param is_create true if this is event is for a create_scanner request
     */
    void handle_timeout(int scanner_id, const String &error_msg, bool is_create);


    /**
     * Returns number of bytes scanned
     *
     * @return byte count
     */
    int64_t bytes_scanned() { return m_bytes_scanned; }

    /**
     * Returns the name of the table as it was when the scanner was created
     */
    String get_table_name() const;

    /**
     * Returns a pointer to the table
     */
    Table *get_table() {return m_table; }

    /**
     * Returns scanspec for this scanner
     */
    const ScanSpec &get_scan_spec() { return m_scan_spec_builder.get(); }
  private:
    void maybe_callback_ok(int scanner_id, bool next, bool do_callback, ScanCellsPtr &cells);
    void maybe_callback_error(int scanner_id, bool next);
    void wait_for_completion();

    std::vector<IntervalScannerAsyncPtr>  m_interval_scanners;
    uint32_t            m_timeout_ms;
    bool                m_retry_table_not_found;
    int64_t             m_bytes_scanned;
    typedef std::set<const char *, LtCstr> CstrRowSet;
    CstrRowSet          m_rowset;
    ResultCallback     *m_cb;
    int                 m_current_scanner;
    Mutex               m_mutex;
    Mutex               m_cancel_mutex;
    boost::condition    m_cond;
    int                 m_outstanding;
    int                 m_error;
    String              m_error_msg;
    String              m_table_name;
    Table              *m_table;
    ScanSpecBuilder     m_scan_spec_builder;
    bool                m_cancelled;
    bool                m_error_shown;
  };

  typedef intrusive_ptr<TableScannerAsync> TableScannerAsyncPtr;
} // namespace Hypertable

#endif // HYPERTABLE_TABLESCANNERASYNC_H
