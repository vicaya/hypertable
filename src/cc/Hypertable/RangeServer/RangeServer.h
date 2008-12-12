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

#ifndef HYPERTABLE_RANGESERVER_H
#define HYPERTABLE_RANGESERVER_H

#include <boost/thread/condition.hpp>

#include "Common/Logger.h"
#include "Common/Properties.h"
#include "Common/HashMap.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/ResponseCallback.h"

#include "Hyperspace/Session.h"

#include "Hypertable/Lib/MasterClient.h"
#include "Hypertable/Lib/RangeState.h"
#include "Hypertable/Lib/Types.h"

#include "Config.h"
#include "Global.h"
#include "ResponseCallbackCreateScanner.h"
#include "ResponseCallbackFetchScanblock.h"
#include "ResponseCallbackGetStatistics.h"
#include "ResponseCallbackUpdate.h"
#include "TableInfo.h"
#include "TableInfoMap.h"


namespace Hypertable {
  using namespace Hyperspace;

  class ConnectionHandler;

  class RangeServer : public ReferenceCount {
  public:
    RangeServer(PropertiesPtr &, ConnectionManagerPtr &,
                ApplicationQueuePtr &, Hyperspace::SessionPtr &);
    virtual ~RangeServer();

    // range server protocol implementations
    void compact(ResponseCallback *, const TableIdentifier *, const RangeSpec *,
                 uint8_t compaction_type);
    void create_scanner(ResponseCallbackCreateScanner *,
                        const TableIdentifier *,
                        const  RangeSpec *, const ScanSpec *);
    void destroy_scanner(ResponseCallback *cb, uint32_t scanner_id);
    void fetch_scanblock(ResponseCallbackFetchScanblock *, uint32_t scanner_id);
    void load_range(ResponseCallback *, const TableIdentifier *,
                    const RangeSpec *, const char *transfer_log_dir,
                    const RangeState *);
    void update(ResponseCallbackUpdate *, const TableIdentifier *,
                uint32_t count, StaticBuffer &);
    void drop_table(ResponseCallback *, const TableIdentifier *);
    void dump_stats(ResponseCallback *);
    void get_statistics(ResponseCallbackGetStatistics *);

    void replay_begin(ResponseCallback *, uint16_t group);
    void replay_load_range(ResponseCallback *, const TableIdentifier *,
                           const RangeSpec *, const RangeState *);
    void replay_update(ResponseCallback *, const uint8_t *data, size_t len);
    void replay_commit(ResponseCallback *);

    void drop_range(ResponseCallback *, const TableIdentifier *,
                    const RangeSpec *);

    void shutdown(ResponseCallback *cb);

    // Other methods
    void do_maintenance();
    void log_cleanup();

    uint64_t get_timer_interval();

    ApplicationQueuePtr get_application_queue() { return m_app_queue; }

    std::string &get_location() { return Global::location; }

    void master_change();

    void wait_for_recovery_finish();
    void wait_for_recovery_finish(const TableIdentifier *table,
                                  const RangeSpec *range);

  private:
    void initialize(PropertiesPtr &);
    void local_recover();
    void replay_log(CommitLogReaderPtr &log_reader);
    void verify_schema(TableInfoPtr &, int generation);
    void
    schedule_log_cleanup_compactions(std::vector<RangePtr> &range_vec,
                                     CommitLog *log, uint64_t prune_threshold);
    void transform_key(ByteString &bskey, DynamicBuffer *dest_bufp,
                       int64_t revision, int64_t *revisionp);

    Mutex                  m_mutex;
    boost::condition       m_root_replay_finished_cond;
    boost::condition       m_metadata_replay_finished_cond;
    boost::condition       m_replay_finished_cond;
    bool                   m_root_replay_finished;
    bool                   m_metadata_replay_finished;
    bool                   m_replay_finished;
    Mutex                  m_update_mutex_a;
    Mutex                  m_update_mutex_b;
    PropertiesPtr          m_props;
    bool                   m_verbose;
    Comm                  *m_comm;
    TableInfoMapPtr        m_live_map;
    TableInfoMapPtr        m_replay_map;
    CommitLogPtr           m_replay_log;
    ConnectionManagerPtr   m_conn_manager;
    ApplicationQueuePtr    m_app_queue;
    uint64_t               m_existence_file_handle;
    LockSequencer          m_existence_file_sequencer;
    ConnectionHandler     *m_master_connection_handler;
    MasterClientPtr        m_master_client;
    Hyperspace::SessionPtr m_hyperspace;
    uint32_t               m_scanner_ttl;
    long                   m_last_commit_log_clean;
    uint32_t               m_timer_interval;
    uint64_t               m_bytes_loaded;
    uint64_t               m_log_roll_limit;
    int                    m_replay_group;
  };

  typedef intrusive_ptr<RangeServer> RangeServerPtr;

} // namespace Hypertable

#endif // HYPERTABLE_RANGESERVER_H
