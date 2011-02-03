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

#include <map>

#include "Common/Logger.h"
#include "Common/Properties.h"
#include "Common/HashMap.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/ResponseCallback.h"

#include "Hyperspace/Session.h"

#include "Hypertable/Lib/Cells.h"
#include "Hypertable/Lib/MasterClient.h"
#include "Hypertable/Lib/RangeState.h"
#include "Hypertable/Lib/Types.h"
#include "Hypertable/Lib/NameIdMapper.h"
#include "Hypertable/Lib/StatsRangeServer.h"

#include "Global.h"
#include "GroupCommitInterface.h"
#include "GroupCommitTimerHandler.h"
#include "MaintenanceScheduler.h"
#include "MetaLogEntityRange.h"
#include "QueryCache.h"
#include "RSStats.h"
#include "ResponseCallbackCreateScanner.h"
#include "ResponseCallbackFetchScanblock.h"
#include "ResponseCallbackGetStatistics.h"
#include "ResponseCallbackUpdate.h"
#include "TableIdCache.h"
#include "TableInfo.h"
#include "TableInfoMap.h"
#include "TimerInterface.h"

namespace Hypertable {
  using namespace Hyperspace;

  class ConnectionHandler;
  class TableUpdate;

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
                        const  RangeSpec *, const ScanSpec *,
			QueryCache::Key *);
    void destroy_scanner(ResponseCallback *cb, uint32_t scanner_id);
    void fetch_scanblock(ResponseCallbackFetchScanblock *, uint32_t scanner_id);
    void load_range(ResponseCallback *, const TableIdentifier *,
                    const RangeSpec *, const char *transfer_log_dir,
                    const RangeState *);
    void update_schema(ResponseCallback *, const TableIdentifier *,
                       const char *);
    void update(ResponseCallbackUpdate *, const TableIdentifier *,
                uint32_t count, StaticBuffer &, uint32_t flags);
    void batch_update(std::vector<TableUpdate *> &updates);

    void commit_log_sync(ResponseCallback *);
    void drop_table(ResponseCallback *, const TableIdentifier *);
    void dump(ResponseCallback *, const char *, bool);
    void get_statistics(ResponseCallbackGetStatistics *);

    void replay_begin(ResponseCallback *, uint16_t group);
    void replay_load_range(ResponseCallback *, MetaLog::EntityRange *,
                           bool write_rsml=true);
    void replay_update(ResponseCallback *, const uint8_t *data, size_t len);
    void replay_commit(ResponseCallback *);

    void drop_range(ResponseCallback *, const TableIdentifier *,
                    const RangeSpec *);

    void relinquish_range(ResponseCallback *, const TableIdentifier *,
                          const RangeSpec *);

    void close(ResponseCallback *cb);

    /**
     * Blocks while the maintenance queue is non-empty
     *
     * @param cb Response callback
     */
    void wait_for_maintenance(ResponseCallback *cb);

    // Other methods
    void group_commit();
    void do_maintenance();

    MaintenanceSchedulerPtr &get_scheduler() { return m_maintenance_scheduler; }

    ApplicationQueuePtr get_application_queue() { return m_app_queue; }

    void master_change();

    void wait_for_recovery_finish();
    void wait_for_root_recovery_finish();
    void wait_for_metadata_recovery_finish();
    void wait_for_system_recovery_finish();
    void wait_for_recovery_finish(const TableIdentifier *table,
                                  const RangeSpec *range);

    void register_timer(TimerInterface *timer) {
      ScopedLock lock(m_mutex);
      HT_ASSERT(m_timer_handler == 0);
      m_timer_handler = timer;
    }

    void shutdown();

  private:
    void initialize(PropertiesPtr &);
    void local_recover();
    void replay_log(CommitLogReaderPtr &log_reader);
    void verify_schema(TableInfoPtr &, uint32_t generation);
    void transform_key(ByteString &bskey, DynamicBuffer *dest_bufp,
                       int64_t revision, int64_t *revisionp);

    Mutex                  m_mutex;
    Mutex                  m_drop_table_mutex;
    boost::condition       m_root_replay_finished_cond;
    boost::condition       m_metadata_replay_finished_cond;
    boost::condition       m_system_replay_finished_cond;
    boost::condition       m_replay_finished_cond;
    bool                   m_root_replay_finished;
    bool                   m_metadata_replay_finished;
    bool                   m_system_replay_finished;
    bool                   m_replay_finished;
    Mutex                  m_update_mutex_a;
    Mutex                  m_update_mutex_b;
    Mutex                  m_stats_mutex;
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
    int32_t                m_max_clock_skew;
    uint64_t               m_bytes_loaded;
    uint64_t               m_log_roll_limit;
    int                    m_replay_group;
    TableIdCachePtr        m_dropped_table_id_cache;

    StatsRangeServerPtr    m_stats;
    RSStatsPtr             m_server_stats;
    int64_t                m_server_stats_timestamp;

    RangeStatsGathererPtr  m_maintenance_stats_gatherer;
    NameIdMapperPtr        m_namemap;

    MaintenanceSchedulerPtr m_maintenance_scheduler;
    TimerInterface        *m_timer_handler;
    GroupCommitInterface  *m_group_commit;
    GroupCommitTimerHandler *m_group_commit_timer_handler;
    uint32_t               m_update_delay;
    QueryCache            *m_query_cache;
    int64_t                m_last_revision;
    int64_t                m_scanner_buffer_size;
    time_t                 m_next_metrics_update;
    double                 m_loadavg_accum;
    uint64_t               m_page_in_accum;
    uint64_t               m_page_out_accum;
    size_t                 m_metric_samples;
    CellsBuilder          *m_pending_metrics_updates;
  };

  typedef intrusive_ptr<RangeServer> RangeServerPtr;

} // namespace Hypertable

#endif // HYPERTABLE_RANGESERVER_H
