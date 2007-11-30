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

#ifndef HYPERTABLE_RANGELOCATOR_H
#define HYPERTABLE_RANGELOCATOR_H

#include "Common/ReferenceCount.h"

#include "AsyncComm/ConnectionManager.h"

#include "Hyperspace/Session.h"

#include "LocationCache.h"
#include "RangeServerClient.h"
#include "RangeLocationInfo.h"
#include "Schema.h"
#include "Types.h"

namespace Hyperspace {
  class Session;
}

namespace Hypertable {

  class RangeServerClient;

  /** Locates containing range given a key.  This class does the METADATA range searching to
   * find the location of the range that contains a row key.
   */
  class RangeLocator : public ReferenceCount {

  public:

    /** Constructor.  Loads the METADATA schema and the root range address from Hyperspace.
     * Installs a RootFileHandler to handle root range location changes.
     *
     * @param connManagerPtr smart pointer to connection manager
     * @param hyperspacePtr smart pointer to Hyperspace session
     */
    RangeLocator(ConnectionManagerPtr &connManagerPtr, Hyperspace::SessionPtr &hyperspacePtr);

    /** Destructor.  Closes the root file in Hyperspace */
    ~RangeLocator();

    /** Locates the range that contains the given row key.
     * 
     * @param table pointer to table identifier structure
     * @param row_key row key to locate
     * @param range_loc_info_p address of RangeLocationInfo structure to hold result
     * @param hard find the hard way (e.g. don't consult cache)
     * @return Error::OK on success or error code on failure
     */
    int find(TableIdentifierT *table, const char *row_key, RangeLocationInfo *range_loc_info_p, bool hard=false);

    /** Sets the "root stale" flag.  Causes methods to reread the root range location before
     * doing METADATA scans.
     */
    void set_root_stale() { m_root_stale=true; }

  private:

    int process_metadata_scanblock(ScanBlock &scan_block);

    const char *build_metadata_start_row_key(char *buf, const char *format, uint32_t table_id, const char *row_key);
    const char *build_metadata_end_row_key(char *buf, const char *format, uint32_t table_id);
    int read_root_location();

    ConnectionManagerPtr   m_conn_manager_ptr;
    Hyperspace::SessionPtr m_hyperspace_ptr;
    LocationCache          m_cache;
    uint64_t               m_root_file_handle;
    HandleCallbackPtr      m_root_handler_ptr;
    bool                   m_root_stale;
    struct sockaddr_in     m_root_addr;
    RangeLocationInfo      m_root_range_info;
    RangeServerClient      m_range_server;
    SchemaPtr              m_metadata_schema_ptr;
    uint8_t                m_startrow_cid;
    uint8_t                m_location_cid;
    TableIdentifierT       m_metadata_table;
  };
  typedef boost::intrusive_ptr<RangeLocator> RangeLocatorPtr;


}

#endif // HYPERTABLE_RANGELOCATOR_H
