/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERTABLE_RANGESERVER_H
#define HYPERTABLE_RANGESERVER_H

#include "Common/Properties.h"
#include "Common/ReferenceCount.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/ResponseCallback.h"

#include "Hyperspace/Session.h"

#include "Hypertable/Lib/MasterClient.h"
#include "Hypertable/Lib/Types.h"

#include "ResponseCallbackCreateScanner.h"
#include "ResponseCallbackFetchScanblock.h"
#include "ResponseCallbackUpdate.h"
#include "TableInfo.h"

using namespace Hypertable;

namespace Hypertable {

  class ConnectionHandler;

  class RangeServer : public ReferenceCount {
  public:
    RangeServer(PropertiesPtr &propsPtr, ConnectionManagerPtr &conn_manager_ptr, ApplicationQueuePtr &app_queue_ptr, Hyperspace::SessionPtr &hyperspace_ptr);
    virtual ~RangeServer();

    void compact(ResponseCallback *cb, TableIdentifierT *table, RangeT *range, uint8_t compaction_type);
    void create_scanner(ResponseCallbackCreateScanner *cb, TableIdentifierT *table, RangeT *range, ScanSpecificationT *scan_spec);
    void destroy_scanner(ResponseCallback *cb, uint32_t scannerId);
    void fetch_scanblock(ResponseCallbackFetchScanblock *cb, uint32_t scannerId);
    void load_range(ResponseCallback *cb, TableIdentifierT *table, RangeT *range, uint64_t soft_limit, uint16_t flags);
    void update(ResponseCallbackUpdate *cb, TableIdentifierT *table, BufferT &buffer);
    void drop_table(ResponseCallback *cb, const char *table_name);
    void dump_stats(ResponseCallback *cb);
    void do_maintenance();
    void log_cleanup();

    uint64_t get_timer_interval();
    
    ApplicationQueuePtr get_application_queue_ptr() { return m_app_queue_ptr; }

    std::string &get_location() { return m_location; }

    void master_change();

  private:
    int directory_initialize(PropertiesPtr &props_ptr);

    bool get_table_info(std::string name, TableInfoPtr &info);
    void set_table_info(std::string name, TableInfoPtr &info);
    bool remove_table_info(std::string name, TableInfoPtr &info);

    int verify_schema(TableInfoPtr &tableInfoPtr, int generation, std::string &errMsg);

    typedef __gnu_cxx::hash_map<string, TableInfoPtr> TableInfoMapT;

    boost::mutex           m_mutex;
    boost::mutex           m_update_mutex_a;
    boost::mutex           m_update_mutex_b;
    PropertiesPtr          m_props_ptr;
    bool                   m_verbose;
    Comm                  *m_comm;
    TableInfoMapT          m_table_info_map;
    ConnectionManagerPtr   m_conn_manager_ptr;
    ApplicationQueuePtr    m_app_queue_ptr;
    uint64_t               m_existence_file_handle;
    struct LockSequencerT  m_existence_file_sequencer;
    std::string            m_location;
    ConnectionHandler     *m_master_connection_handler;
    MasterClientPtr        m_master_client_ptr;
    Hyperspace::SessionPtr m_hyperspace_ptr;
    time_t                 m_scanner_ttl;
    long                   m_last_commit_log_clean;
    uint64_t               m_timer_interval;
  };
  typedef boost::intrusive_ptr<RangeServer> RangeServerPtr;
  

}

#endif // HYPERTABLE_RANGESERVER_H
