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

#ifndef HYPERTABLE_MASTER_H
#define HYPERTABLE_MASTER_H

#include <ext/hash_map>

#include <boost/thread/mutex.hpp>

#include "Common/Thread.h"
#include "Common/Properties.h"
#include "Common/ReferenceCount.h"
#include "Common/StringExt.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/ResponseCallback.h"

#include "Hyperspace/Session.h"

#include "Hypertable/Lib/Filesystem.h"
#include "Hypertable/Lib/Table.h"
#include "Hypertable/Lib/Types.h"

#include "HyperspaceSessionHandler.h"
#include "RangeServerState.h"
#include "ResponseCallbackGetSchema.h"
#include "MasterGc.h"

namespace Hypertable {

  class Master : public ReferenceCount {
  public:
    Master(ConnectionManagerPtr &connManagerPtr, PropertiesPtr &props_ptr, ApplicationQueuePtr &appQueuePtr);
    ~Master();

    void create_table(ResponseCallback *cb, const char *tableName, const char *schemaString);
    void get_schema(ResponseCallbackGetSchema *cb, const char *tableName);
    void register_server(ResponseCallback *cb, const char *location, struct sockaddr_in &addr);
    void report_split(ResponseCallback *cb, TableIdentifierT &table, RangeT &range, uint64_t soft_limit);
    void drop_table(ResponseCallback *cb, const char *table_name, bool if_exists);

    void server_joined(std::string &location);
    void server_left(std::string &location);

    void join();

  protected:
    int create_table(const char *tableName, const char *schemaString, std::string &errMsg);

  private:
    bool initialize();
    void scan_servers_directory();
    bool create_hyperspace_dir(std::string dir);

    boost::mutex m_mutex;
    PropertiesPtr m_props_ptr;
    ConnectionManagerPtr m_conn_manager_ptr;
    ApplicationQueuePtr m_app_queue_ptr;
    bool m_verbose;
    Hyperspace::SessionPtr m_hyperspace_ptr;
    Filesystem *m_dfs_client;
    atomic_t m_last_table_id;
    HyperspaceSessionHandler m_hyperspace_session_handler;
    uint64_t m_master_file_handle;
    uint64_t m_servers_dir_handle;
    struct LockSequencerT m_master_file_sequencer;
    HandleCallbackPtr m_servers_dir_callback_ptr;
    TablePtr m_metadata_table_ptr;
    uint64_t m_max_range_bytes;

    /** temporary vairables **/
    bool m_initialized;

    typedef __gnu_cxx::hash_map<std::string, RangeServerStatePtr> ServerMapT;

    ServerMapT  m_server_map;
    ServerMapT::iterator m_server_map_iter;

    ThreadGroup m_threads;
  };
  typedef boost::intrusive_ptr<Master> MasterPtr;

}

#endif // HYPERTABLE_MASTER_H

