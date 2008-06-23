/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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

#ifndef HYPERTABLE_RANGESERVER_H
#define HYPERTABLE_RANGESERVER_H

#include "Common/Properties.h"
#include "Common/ReferenceCount.h"
#include "Common/HashMap.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/ResponseCallback.h"

#include "Hyperspace/Session.h"

#include "Hypertable/Lib/MasterClient.h"
#include "Hypertable/Lib/RangeState.h"
#include "Hypertable/Lib/Types.h"

#include "ResponseCallbackCreateScanner.h"
#include "ResponseCallbackFetchScanblock.h"
#include "ResponseCallbackUpdate.h"
#include "TableInfo.h"
#include "TableInfoMap.h"


namespace Hypertable {
  using namespace Hyperspace;

  class ConnectionHandler;

  class RangeServer : public ReferenceCount {
  public:
    RangeServer(PropertiesPtr &, ConnectionManagerPtr &, ApplicationQueuePtr &,
                Hyperspace::SessionPtr &);
    virtual ~RangeServer();

    // range server protocol implementations
    void compact(ResponseCallback *, TableIdentifier *, RangeSpec *,
                 uint8_t compaction_type);
    void create_scanner(ResponseCallbackCreateScanner *, TableIdentifier *,
                        RangeSpec *, ScanSpec *);
    void destroy_scanner(ResponseCallback *cb, uint32_t scanner_id);
    void fetch_scanblock(ResponseCallbackFetchScanblock *, uint32_t scanner_id);
    void load_range(ResponseCallback *, TableIdentifier *, RangeSpec *,
                    const char *transfer_log_dir, RangeState *, uint16_t flags);
    void update(ResponseCallbackUpdate *, TableIdentifier *, StaticBuffer &);
    void drop_table(ResponseCallback *, TableIdentifier *);
    void dump_stats(ResponseCallback *);

    void replay_start(ResponseCallback *);
    void replay_update(ResponseCallback *, const uint8_t *data, size_t len);
    void replay_commit(ResponseCallback *);

    void drop_range(ResponseCallback *, TableIdentifier *, RangeSpec *);

    // Other methods
    void do_maintenance();
    void log_cleanup();

    uint64_t get_timer_interval();

    ApplicationQueuePtr get_application_queue_ptr() { return m_app_queue_ptr; }

    std::string &get_location() { return m_location; }

    void master_change();

  private:
    int initialize(PropertiesPtr &);
    void fast_recover();
    void reload_range(TableIdentifier *, RangeSpec *, uint64_t soft_limit,
                      const String &split_log);

    int verify_schema(TableInfoPtr &, int generation, std::string &errmsg);

    Mutex                  m_mutex;
    Mutex                  m_update_mutex_a;
    Mutex                  m_update_mutex_b;
    PropertiesPtr          m_props_ptr;
    bool                   m_verbose;
    Comm                  *m_comm;
    TableInfoMapPtr        m_live_map_ptr;
    TableInfoMapPtr        m_replay_map_ptr;
    CommitLogPtr           m_replay_log_ptr;
    ConnectionManagerPtr   m_conn_manager_ptr;
    ApplicationQueuePtr    m_app_queue_ptr;
    uint64_t               m_existence_file_handle;
    LockSequencer          m_existence_file_sequencer;
    std::string            m_location;
    ConnectionHandler     *m_master_connection_handler;
    MasterClientPtr        m_master_client_ptr;
    Hyperspace::SessionPtr m_hyperspace_ptr;
    time_t                 m_scanner_ttl;
    long                   m_last_commit_log_clean;
    uint64_t               m_timer_interval;
    uint64_t               m_bytes_loaded;
  };

  typedef intrusive_ptr<RangeServer> RangeServerPtr;

} // namespace Hypertable

#endif // HYPERTABLE_RANGESERVER_H
