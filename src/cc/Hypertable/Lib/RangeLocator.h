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

#ifndef HYPERTABLE_RANGELOCATOR_H
#define HYPERTABLE_RANGELOCATOR_H

#include <deque>

#include "Common/Mutex.h"
#include "Common/Error.h"
#include "Common/ReferenceCount.h"
#include "Common/Timer.h"
#include "Common/Config.h"

#include "AsyncComm/ConnectionManager.h"

#include "Hyperspace/Session.h"

#include "LocationCache.h"
#include "RangeServerClient.h"
#include "RangeLocationInfo.h"
#include "Schema.h"
#include "Types.h"

namespace Hypertable {

  class RangeServerClient;

  /** Locates containing range given a key.  This class does the METADATA range
   * searching to find the location of the range that contains a row key.
   */
  class RangeLocator : public ReferenceCount {

  public:

    /** Constructor.  Loads the METADATA schema and the root range address from
     * Hyperspace. Installs a RootFileHandler to handle root range location
     * changes.
     *
     * @param cfg reference to config properties
     * @param conn_mgr smart pointer to connection manager
     * @param hyperspace smart pointer to Hyperspace session
     * @param timeout timeout in seconds
     */
    RangeLocator(PropertiesPtr &cfg, ConnectionManagerPtr &conn_mgr,
                 Hyperspace::SessionPtr &hyperspace, int timeout);

    /** Destructor.  Closes the root file in Hyperspace */
    ~RangeLocator();

    /** Locates the range that contains the given row key.
     *
     * @param table pointer to table identifier structure
     * @param row_key row key to locate
     * @param range_loc_infop address of RangeLocationInfo to hold result
     * @param timer reference to timer object
     * @param hard don't consult cache
     * @return Error::OK on success or error code on failure
     */
    void find_loop(const TableIdentifier *table, const char *row_key,
                   RangeLocationInfo *range_loc_infop, Timer &timer, bool hard);

    /** Locates the range that contains the given row key.
     *
     * @param table pointer to table identifier structure
     * @param row_key row key to locate
     * @param range_loc_infop address of RangeLocationInfo to hold result
     * @param timer reference to timer object
     * @param hard don't consult cache
     * @return Error::OK on success or error code on failure
     */
    int find(const TableIdentifier *table, const char *row_key,
             RangeLocationInfo *range_loc_infop, Timer &timer, bool hard);

    /**
     * Invalidates the cached entry for the given row key
     *
     * @param table pointer to table identifier structure
     * @param row_key row key to invalidate
     * @return true if invalidated, false if not cached
     */
    bool invalidate(const TableIdentifier *table, const char *row_key) {
      return m_cache_ptr->invalidate(table->id, row_key);
    }

    /** Sets the "root stale" flag.  Causes methods to reread the root range
     * location before doing METADATA scans.
     */
    void set_root_stale() { m_root_stale=true; }

    /**
     * Returns the location cache
     *
     * @param cache_ptr reference to smart pointer to return location cache
     */
    void get_location_cache(LocationCachePtr &cache_ptr) {
      cache_ptr = m_cache_ptr;
    }

    /**
     * Clears the error history
     */
    void clear_error_history() {
      ScopedLock lock(m_mutex);
      m_last_errors.clear();
    }

    /**
     * Displays the error history
     */
    void dump_error_history() {
      ScopedLock lock(m_mutex);
      foreach(Exception &e, m_last_errors)
        HT_ERROR_OUT << e << HT_END;
      m_last_errors.clear();
    }

  private:

    void initialize();
    int process_metadata_scanblock(ScanBlock &scan_block);
    int read_root_location(Timer &timer);

    Mutex                  m_mutex;
    ConnectionManagerPtr   m_conn_manager_ptr;
    Hyperspace::SessionPtr m_hyperspace_ptr;
    LocationCachePtr       m_cache_ptr;
    uint64_t               m_root_file_handle;
    Hyperspace::HandleCallbackPtr m_root_handler_ptr;
    bool                   m_root_stale;
    struct sockaddr_in     m_root_addr;
    RangeLocationInfo      m_root_range_info;
    RangeServerClient      m_range_server;
    SchemaPtr              m_metadata_schema_ptr;
    uint8_t                m_startrow_cid;
    uint8_t                m_location_cid;
    TableIdentifier        m_metadata_table;
    std::deque<Exception>  m_last_errors;
  };

  typedef intrusive_ptr<RangeLocator> RangeLocatorPtr;

} // namespace Hypertable


#endif // HYPERTABLE_RANGELOCATOR_H
