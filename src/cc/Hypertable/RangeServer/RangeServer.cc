/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
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

#include "Common/Compat.h"
#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <fstream>

extern "C" {
#include <fcntl.h>
#include <math.h>
#include <sys/time.h>
#include <sys/resource.h>
}

#include "Common/FailureInducer.h"
#include "Common/FileUtils.h"
#include "Common/md5.h"
#include "Common/StringExt.h"
#include "Common/SystemInfo.h"

#include "Hypertable/Lib/CommitLog.h"
#include "Hypertable/Lib/Defaults.h"
#include "Hypertable/Lib/Key.h"
#include "Hypertable/Lib/Stat.h"
#include "Hypertable/Lib/RangeServerMetaLogReader.h"
#include "Hypertable/Lib/RangeServerMetaLogEntries.h"
#include "Hypertable/Lib/RangeServerProtocol.h"

#include "DfsBroker/Lib/Client.h"

#include "FillScanBlock.h"
#include "Global.h"
#include "HandlerFactory.h"
#include "Location.h"
#include "MaintenanceQueue.h"
#include "MaintenanceScheduler.h"
#include "MaintenanceTaskCompaction.h"
#include "MaintenanceTaskSplit.h"
#include "RangeServer.h"
#include "RangeStatsGatherer.h"
#include "ScanContext.h"

using namespace std;
using namespace Hypertable;
using namespace Serialization;
using namespace Hypertable::Property;

RangeServer::RangeServer(PropertiesPtr &props, ConnectionManagerPtr &conn_mgr,
    ApplicationQueuePtr &app_queue, Hyperspace::SessionPtr &hyperspace)
  : m_root_replay_finished(false), m_metadata_replay_finished(false),
    m_replay_finished(false), m_props(props), m_verbose(false),
    m_conn_manager(conn_mgr), m_app_queue(app_queue), m_hyperspace(hyperspace),
    m_timer_handler(0), m_query_cache(0) {

  uint16_t port;
  uint32_t maintenance_threads = std::min(2, System::cpu_info().total_cores);
  Comm *comm = conn_mgr->get_comm();
  SubProperties cfg(props, "Hypertable.RangeServer.");

  m_verbose = props->get_bool("verbose");
  Global::range_metadata_split_size = cfg.get_i64("Range.MetadataSplitSize", 0);
  Global::range_split_size = cfg.get_i64("Range.SplitSize");
  Global::range_maximum_size = cfg.get_i64("Range.MaximumSize");
  Global::access_group_max_files = cfg.get_i32("AccessGroup.MaxFiles");
  Global::access_group_merge_files = cfg.get_i32("AccessGroup.MergeFiles");
  Global::access_group_max_mem = cfg.get_i64("AccessGroup.MaxMemory");
  maintenance_threads = cfg.get_i32("MaintenanceThreads", maintenance_threads);
  port = cfg.get_i16("Port");

  m_server_stats = new RangeServerStats((int64_t)cfg.get_i32("Maintenance.Interval"));

  if (cfg.has("Scanner.Ttl"))
    m_scanner_ttl = (time_t)cfg.get_i32("Scanner.Ttl");
  else
    m_scanner_ttl = (time_t)props->get_i32("Hypertable.Request.Timeout");

  if (Global::access_group_merge_files > Global::access_group_max_files)
    Global::access_group_merge_files = Global::access_group_max_files;

  Global::cell_cache_scanner_cache_size = 
    cfg.get_i32("AccessGroup.CellCache.ScannerCacheSize");

  if (m_scanner_ttl < (time_t)10000) {
    HT_WARNF("Value %u for Hypertable.RangeServer.Scanner.ttl is too small, "
             "setting to 10000", (unsigned int)m_scanner_ttl);
    m_scanner_ttl = (time_t)10000;
  }

  if (cfg.has("MemoryLimit"))
    Global::memory_limit = cfg.get_i64("MemoryLimit");
  else {
    double pct = (double)cfg.get_i32("MemoryLimit.Percentage") / 100.0;
    Global::memory_limit = (int64_t)((double)System::mem_stat().ram * 1000000.0 * pct);
  }

  m_max_clock_skew = cfg.get_i32("ClockSkew.Max");

  m_update_delay = cfg.get_i32("UpdateDelay", 0);

  int64_t block_cache_min = cfg.get_i64("BlockCache.MinMemory");
  int64_t block_cache_max;
  if (cfg.has("BlockCache.MaxMemory"))
    block_cache_max = cfg.get_i64("BlockCache.MaxMemory");
  else
    block_cache_max = std::numeric_limits<int64_t>::max();
  
  Global::block_cache = new FileBlockCache(block_cache_min, block_cache_max);

  uint64_t query_cache_memory = cfg.get_i64("QueryCache.MaxMemory");
  if (query_cache_memory > 0)
    m_query_cache = new QueryCache(query_cache_memory);

  Global::memory_tracker = new MemoryTracker(Global::block_cache);
  Global::memory_tracker->add(query_cache_memory);

  Global::protocol = new Hypertable::RangeServerProtocol();

  DfsBroker::Client *dfsclient = new DfsBroker::Client(conn_mgr, props);

  int dfs_timeout;
  if (props->has("DfsBroker.Timeout"))
    dfs_timeout = props->get_i32("DfsBroker.Timeout");
  else
    dfs_timeout = props->get_i32("Hypertable.Request.Timeout");

  if (!dfsclient->wait_for_connection(dfs_timeout))
    HT_THROW(Error::REQUEST_TIMEOUT, "connecting to DFS Broker");

  Global::dfs = dfsclient;

  m_log_roll_limit = cfg.get_i64("CommitLog.RollLimit");

  m_dropped_table_id_cache = new TableIdCache(50);

  /**
   * Check for and connect to commit log DFS broker
   */
  if (cfg.has("CommitLog.DfsBroker.Host")) {
    String loghost = cfg.get_str("CommitLog.DfsBroker.Host");
    uint16_t logport = cfg.get_i16("CommitLog.DfsBroker.Port");
    InetAddr addr(loghost, logport);

    dfsclient = new DfsBroker::Client(conn_mgr, addr, dfs_timeout);

    if (!dfsclient->wait_for_connection(30000))
      HT_THROW(Error::REQUEST_TIMEOUT, "connecting to commit log DFS broker");

    Global::log_dfs = dfsclient;
  }
  else
    Global::log_dfs = Global::dfs;

  // Create the maintenance queue
  Global::maintenance_queue = new MaintenanceQueue(maintenance_threads);

  // Create table info maps
  m_live_map = new TableInfoMap();
  m_replay_map = new TableInfoMap();

  /**
   * Create maintenance scheduler
   */
  m_stats_gatherer = new RangeStatsGatherer(m_live_map);
  m_maintenance_scheduler = new MaintenanceScheduler(Global::maintenance_queue, m_server_stats, m_stats_gatherer);

  /**
   * Listen for incoming connections
   */
  ConnectionHandlerFactoryPtr chfp =
      new HandlerFactory(comm, m_app_queue, this);

  InetAddr listen_addr(INADDR_ANY, port);
  comm->listen(listen_addr, chfp);

  /**
   * Create Master client
   */
  int timeout = props->get_i32("Hypertable.Request.Timeout");
  m_master_client = new MasterClient(m_conn_manager, m_hyperspace,
                                     timeout, m_app_queue);
  m_master_connection_handler = new ConnectionHandler(comm, m_app_queue,
                                    this, m_master_client);
  m_master_client->initiate_connection(m_master_connection_handler);

  Location::wait_until_assigned();

  initialize(props);

  local_recover();

  Global::log_prune_threshold_min = cfg.get_i64("CommitLog.PruneThreshold.Min",
      2 * Global::user_log->get_max_fragment_size());

  uint32_t max_memory_percentage =
    cfg.get_i32("CommitLog.PruneThreshold.Max.MemoryPercentage");

  HT_ASSERT(max_memory_percentage >= 0 && max_memory_percentage <= 100);

  double max_memory_ratio = (double)max_memory_percentage / 100.0;

  int64_t threshold_max = (int64_t)((double)System::mem_stat().ram *
                                    max_memory_ratio * (double)MiB);
  // cap at 4GB
  if (threshold_max > (int64_t)(4LL * GiB))
    threshold_max = 4LL * GiB;

  Global::log_prune_threshold_max = cfg.get_i64("CommitLog.PruneThreshold.Max", threshold_max);
}

void RangeServer::shutdown() {

  // stop maintenance timer
  m_timer_handler->shutdown();
#if defined(CLEAN_SHUTDOWN)
  m_timer_handler->wait_for_shutdown();
#endif

  // stop maintenance queue
  Global::maintenance_queue->shutdown();
#if defined(CLEAN_SHUTDOWN)
  Global::maintenance_queue->join();
#endif

  // delete global objects
  delete Global::block_cache;
  delete Global::user_log;
  delete Global::metadata_log;
  delete Global::root_log;
  delete Global::range_log;

  delete m_query_cache;

  Global::maintenance_queue = 0;
  Global::metadata_table = 0;
  Global::hyperspace = 0;

  if (Global::log_dfs != Global::dfs)
    delete Global::log_dfs;
  delete Global::dfs;

  delete Global::protocol;

}


RangeServer::~RangeServer() {
}


/**
 * - Figure out and create range server directory
 * - Clear any Range server state (including any partially created commit logs)
 * - Open the commit log
 */
void RangeServer::initialize(PropertiesPtr &props) {

  if (!m_hyperspace->exists("/hypertable/servers")) {
    if (!m_hyperspace->exists("/hypertable"))
      m_hyperspace->mkdir("/hypertable");
    m_hyperspace->mkdir("/hypertable/servers");
  }

  String top_dir("/hypertable/servers/");
  top_dir += Location::get();

  /**
   * Create "server existence" file in Hyperspace and lock it exclusively
   */
  uint32_t lock_status;
  uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_CREATE
      | OPEN_FLAG_CREATE | OPEN_FLAG_LOCK;
  HandleCallbackPtr null_callback;

  m_existence_file_handle = m_hyperspace->open(top_dir.c_str(), oflags,
                                               null_callback);

  while (true) {
    lock_status = 0;

    m_hyperspace->try_lock(m_existence_file_handle, LOCK_MODE_EXCLUSIVE,
                           &lock_status, &m_existence_file_sequencer);

    if (lock_status == LOCK_STATUS_GRANTED)
      break;

    HT_INFO_OUT << "Waiting for exclusive lock on hyperspace:/" << top_dir
                << " ..." << HT_END;
    poll(0, 0, 5000);
  }

  Global::log_dir = top_dir + "/log";

  /**
   * Create log directories
   */
  String path;
  try {
    path = Global::log_dir + "/user";
    Global::log_dfs->mkdirs(path);
    path = Global::log_dir + "/range_txn";
    Global::log_dfs->mkdirs(path);
  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Problem creating commit log directory '%s': %s",
               path.c_str(), e.what());
  }

  HT_INFO_OUT << "log_dir=" << Global::log_dir << HT_END;
}


void RangeServer::local_recover() {
  String meta_log_dir = Global::log_dir + "/range_txn";
  RangeServerMetaLogReaderPtr rsml_reader;
  CommitLogReaderPtr root_log_reader;
  CommitLogReaderPtr metadata_log_reader;
  CommitLogReaderPtr user_log_reader;
  std::vector<RangePtr> rangev;

  try {
    std::vector<MaintenanceTask*> maintenance_tasks;
    boost::xtime now;
    boost::xtime_get(&now, boost::TIME_UTC);

    /**
     * Check for existence of
     * /hypertable/servers/X.X.X.X_port/log/range_txn/0.log file
     */

    rsml_reader = new RangeServerMetaLogReader(Global::log_dfs,
                                               meta_log_dir);

    if (!rsml_reader->empty()) {
      HT_DEBUG_OUT <<"Found "<< meta_log_dir <<", start recovering"<< HT_END;

      // Load range states
      const RangeStates &range_states = rsml_reader->load_range_states();

      // Re-open RSML for writing
      Global::range_log = new RangeServerMetaLog(Global::log_dfs, meta_log_dir);

      /**
       * First ROOT metadata range
       */
      m_replay_group = RangeServerProtocol::GROUP_METADATA_ROOT;

      // clear the replay map
      m_replay_map->clear();

      foreach(const RangeStateInfo *i, range_states) {
        if (i->table.id == 0 && i->range.end_row
            && !strcmp(i->range.end_row, Key::END_ROOT_ROW)) {
          HT_ASSERT(i->transactions.empty());
          replay_load_range(0, &i->table, &i->range, &i->range_state, false);
        }
      }

      if (!m_replay_map->empty()) {
        root_log_reader = new CommitLogReader(Global::log_dfs,
                                              Global::log_dir + "/root");
        replay_log(root_log_reader);

        // Perform any range specific post-replay tasks
        rangev.clear();
        m_replay_map->get_range_vector(rangev);
        foreach(RangePtr &range, rangev) {
          range->recovery_finalize();
	  if (range->get_state() == RangeState::SPLIT_LOG_INSTALLED ||
	      range->get_state() == RangeState::SPLIT_SHRUNK)
	    maintenance_tasks.push_back(new MaintenanceTaskSplit(now, range));
	  else 
	    HT_ASSERT(range->get_state() == RangeState::STEADY);
	}
      }

      // Create ROOT log and wake up anybody waiting for root replay to complete
      {
        ScopedLock lock(m_mutex);

	if (!m_replay_map->empty())
	  m_live_map->merge(m_replay_map);

        if (root_log_reader)
          Global::root_log = new CommitLog(Global::log_dfs, Global::log_dir
              + "/root", m_props, root_log_reader.get());
        m_root_replay_finished = true;
        m_root_replay_finished_cond.notify_all();
      }

      // Finish mid-maintenance
      if (!maintenance_tasks.empty()) {
	for (size_t i=0; i<maintenance_tasks.size(); i++)
	  Global::maintenance_queue->add(maintenance_tasks[i]);
	Global::maintenance_queue->wait_for_empty();
	maintenance_tasks.clear();
      }

      /**
       * Then recover other METADATA ranges
       */
      m_replay_group = RangeServerProtocol::GROUP_METADATA;

      // clear the replay map
      m_replay_map->clear();

      foreach(const RangeStateInfo *i, range_states) {
        if (i->table.id == 0 && !(i->range.end_row
	    && !strcmp(i->range.end_row, Key::END_ROOT_ROW)))
          replay_load_range(0, &i->table, &i->range, &i->range_state, false);
      }

      if (!m_replay_map->empty()) {
        metadata_log_reader =
            new CommitLogReader(Global::log_dfs, Global::log_dir + "/metadata");
        replay_log(metadata_log_reader);

        // Perform any range specific post-replay tasks
        rangev.clear();
        m_replay_map->get_range_vector(rangev);
        foreach(RangePtr &range, rangev) {
          range->recovery_finalize();
	  if (range->get_state() == RangeState::SPLIT_LOG_INSTALLED ||
	      range->get_state() == RangeState::SPLIT_SHRUNK)
	    maintenance_tasks.push_back(new MaintenanceTaskSplit(now, range));
	  else 
	    HT_ASSERT(range->get_state() == RangeState::STEADY);
	}
      }

      // Create metadata log and wake up anybody waiting for metadata replay to
      // complete
      {
        ScopedLock lock(m_mutex);

	if (!m_replay_map->empty())
	  m_live_map->merge(m_replay_map);

        if (metadata_log_reader)
          Global::metadata_log = new CommitLog(Global::log_dfs,
              Global::log_dir + "/metadata", m_props,
              metadata_log_reader.get());
        m_metadata_replay_finished = true;
        m_metadata_replay_finished_cond.notify_all();
      }

      // Finish mid-maintenance
      if (!maintenance_tasks.empty()) {
	for (size_t i=0; i<maintenance_tasks.size(); i++)
	  Global::maintenance_queue->add(maintenance_tasks[i]);
	Global::maintenance_queue->wait_for_empty();
	maintenance_tasks.clear();
      }

      /**
       * Then recover the normal ranges
       */
      m_replay_group = RangeServerProtocol::GROUP_USER;

      // clear the replay map
      m_replay_map->clear();

      foreach(const RangeStateInfo *i, range_states) {
        if (i->table.id != 0)
          replay_load_range(0, &i->table, &i->range, &i->range_state, false);
      }

      if (!m_replay_map->empty()) {
        user_log_reader = new CommitLogReader(Global::log_dfs,
                                              Global::log_dir + "/user");
        replay_log(user_log_reader);

        // Perform any range specific post-replay tasks
        rangev.clear();
        m_replay_map->get_range_vector(rangev);
        foreach(RangePtr &range, rangev) {
          range->recovery_finalize();
	  if (range->get_state() == RangeState::SPLIT_LOG_INSTALLED ||
	      range->get_state() == RangeState::SPLIT_SHRUNK)
	    maintenance_tasks.push_back(new MaintenanceTaskSplit(now, range));
	  else 
	    HT_ASSERT(range->get_state() == RangeState::STEADY);
	}
      }


      // Create user log and range txn log and
      // wake up anybody waiting for replay to complete
      {
        ScopedLock lock(m_mutex);

	if (!m_replay_map->empty())
	  m_live_map->merge(m_replay_map);

        Global::user_log = new CommitLog(Global::log_dfs, Global::log_dir
            + "/user", m_props, user_log_reader.get());
        m_replay_finished = true;
        m_replay_finished_cond.notify_all();
      }

      // Finish mid-maintenance
      if (!maintenance_tasks.empty()) {
	for (size_t i=0; i<maintenance_tasks.size(); i++)
	  Global::maintenance_queue->add(maintenance_tasks[i]);
	Global::maintenance_queue->wait_for_empty();
	maintenance_tasks.clear();
      }

    }
    else {
      ScopedLock lock(m_mutex);

      /**
       *  Create the logs
       */

      if (root_log_reader)
        Global::root_log = new CommitLog(Global::log_dfs, Global::log_dir
            + "/root", m_props, root_log_reader.get());

      if (metadata_log_reader)
        Global::metadata_log = new CommitLog(Global::log_dfs, Global::log_dir
            + "/metadata", m_props, metadata_log_reader.get());

      Global::user_log = new CommitLog(Global::log_dfs, Global::log_dir
          + "/user", m_props, user_log_reader.get());

      Global::range_log = new RangeServerMetaLog(Global::log_dfs,
                                                 meta_log_dir);

      m_root_replay_finished = true;
      m_metadata_replay_finished = true;
      m_replay_finished = true;

      m_root_replay_finished_cond.notify_all();
      m_metadata_replay_finished_cond.notify_all();
      m_replay_finished_cond.notify_all();
    }

  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    HT_ABORT;
  }
}


void RangeServer::replay_log(CommitLogReaderPtr &log_reader) {
  BlockCompressionHeaderCommitLog header;
  uint8_t *base;
  size_t len;
  TableIdentifier table_id;
  DynamicBuffer dbuf;
  const uint8_t *ptr, *end;
  int64_t revision;
  TableInfoPtr table_info;
  RangePtr range;
  SerializedKey key;
  ByteString value;
  uint32_t block_count = 0;
  String start_row, end_row;

  while (log_reader->next((const uint8_t **)&base, &len, &header)) {

    revision = header.get_revision();

    ptr = base;
    end = base + len;

    table_id.decode(&ptr, &len);

    // Fetch table info
    if (!m_replay_map->get(table_id.id, table_info))
      continue;

    dbuf.ensure(table_id.encoded_length() + 12 + len);
    dbuf.clear();

    dbuf.ptr += 4;  // skip size
    encode_i64(&dbuf.ptr, revision);
    table_id.encode(&dbuf.ptr);
    base = dbuf.ptr;

    while (ptr < end) {

      // extract the key
      key.ptr = ptr;
      ptr += key.length();
      if (ptr > end)
        HT_THROW(Error::REQUEST_TRUNCATED, "Problem decoding key");

      // extract the value
      value.ptr = ptr;
      ptr += value.length();
      if (ptr > end)
        HT_THROW(Error::REQUEST_TRUNCATED, "Problem decoding value");

      // Look for containing range, add to stop mods if not found
      if (!table_info->find_containing_range(key.row(), range,
                                             start_row, end_row))
        continue;

      // add key/value pair to buffer
      memcpy(dbuf.ptr, key.ptr, ptr-key.ptr);
      dbuf.ptr += ptr-key.ptr;

    }

    uint32_t block_size = dbuf.ptr - base;
    base = dbuf.base;
    encode_i32(&base, block_size);

    replay_update(0, dbuf.base, dbuf.fill());
    block_count++;
  }

  HT_INFOF("Replayed %u blocks of updates from '%s'", block_count,
           log_reader->get_log_dir().c_str());
}


void
RangeServer::compact(ResponseCallback *cb, const TableIdentifier *table,
                     const RangeSpec *range_spec, uint8_t compaction_type) {
  int error = Error::OK;
  String errmsg;
  TableInfoPtr table_info;
  RangePtr range;
  bool major = false;

  // Check for major compaction
  if (compaction_type == 1)
    major = true;

  HT_INFO_OUT <<"compacting\n"<< *table << *range_spec
              <<"Compaction type="<< (major ? "major" : "minor") << HT_END;

  if (!m_replay_finished)
    wait_for_recovery_finish(table, range_spec);

  try {

    m_live_map->get(table, table_info);

    /**
     * Fetch range info
     */
    if (!table_info->get_range(range_spec, range))
      HT_THROW(Error::RANGESERVER_RANGE_NOT_FOUND,
               format("%s[%s..%s]", table->name,range_spec->start_row,
                      range_spec->end_row));

    /*** FIX ME
    Global::maintenance_queue->add(new MaintenanceTaskCompaction(range, major));
    **/

    if ((error = cb->response_ok()) != Error::OK)
      HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));

    HT_DEBUGF("Compaction (%s) scheduled for table '%s' end row '%s'",
              (major ? "major" : "minor"), table->name, range_spec->end_row);

  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    if (cb && (error = cb->error(e.code(), e.what())) != Error::OK) {
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
    }
  }
}


void
RangeServer::create_scanner(ResponseCallbackCreateScanner *cb,
    const TableIdentifier *table, const RangeSpec *range_spec,
    const ScanSpec *scan_spec, QueryCache::Key *cache_key) {
  int error = Error::OK;
  String errmsg;
  TableInfoPtr table_info;
  RangePtr range;
  CellListScannerPtr scanner;
  bool more = true;
  uint32_t id = 0;
  SchemaPtr schema;
  ScanContextPtr scan_ctx;
  bool decrement_needed=false;
  const char *row = "";

  HT_DEBUG_OUT <<"Creating scanner:\n"<< *table << *range_spec
               << *scan_spec << HT_END;

  if (!m_replay_finished)
    wait_for_recovery_finish(table, range_spec);

  try {
    DynamicBuffer rbuf;

    if (scan_spec->row_intervals.size() > 0) {
      if (scan_spec->row_intervals.size() > 1)
        HT_THROW(Error::RANGESERVER_BAD_SCAN_SPEC,
                 "can only scan one row interval");
      row = scan_spec->row_intervals[0].start;
      if (scan_spec->cell_intervals.size() > 0)
        HT_THROW(Error::RANGESERVER_BAD_SCAN_SPEC,
                 "both row and cell intervals defined");
    }
    else if (scan_spec->cell_intervals.size() > 0)
      row = scan_spec->cell_intervals[0].start_row;

    if (scan_spec->cell_intervals.size() > 1)
      HT_THROW(Error::RANGESERVER_BAD_SCAN_SPEC,
               "can only scan one cell interval");

    m_live_map->get(table, table_info);

    if (!table_info->get_range(range_spec, range))
      HT_THROWF(Error::RANGESERVER_RANGE_NOT_FOUND, "(a) %s[%s..%s]",
                table->name, range_spec->start_row, range_spec->end_row);

    schema = table_info->get_schema();

    // verify schema
    if (schema->get_generation() != table->generation) {
      HT_THROW(Error::RANGESERVER_GENERATION_MISMATCH,
               (String)"RangeServer Schema generation for table '"
               + table_info->get_name() + "' is " +
               schema->get_generation() + " but supplied is "
               + table->generation);
    }

    range->increment_scan_counter();
    decrement_needed = true;

    // Check to see if range jus shrunk
    if (strcmp(range->start_row().c_str(), range_spec->start_row) ||
        strcmp(range->end_row().c_str(), range_spec->end_row))
      HT_THROWF(Error::RANGESERVER_RANGE_NOT_FOUND, "(b) %s[%s..%s]",
                table->name, range_spec->start_row, range_spec->end_row);

    // check query cache
    if (cache_key && m_query_cache && table->id) {
      boost::shared_array<uint8_t> ext_buffer;
      uint32_t ext_len;
      if (m_query_cache->lookup(cache_key, ext_buffer, &ext_len)) {
	// The first argument to the response method is flags and the
	// 0th bit is the EOS (end-of-scan) bit, hence the 1
	if ((error = cb->response(1, id, ext_buffer, ext_len)) != Error::OK)
	  HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
	range->decrement_scan_counter();
	decrement_needed = false;
	return;
      }
    }

    scan_ctx = new ScanContext(range->get_scan_revision(),
                               scan_spec, range_spec, schema);

    scanner = range->create_scanner(scan_ctx);

    range->decrement_scan_counter();
    decrement_needed = false;

    size_t count;

    more = FillScanBlock(scanner, rbuf, &count);

    range->add_bytes_read( rbuf.fill() );
    if (table->id != 0)
      m_server_stats->add_scan_data(1, rbuf.fill());

    id = (more) ? Global::scanner_map.put(scanner, range, table) : 0;

    HT_INFOF("Successfully created scanner (id=%u) on table '%s', returning "
             "%d k/v pairs", id, table->name, (int)count);

    /**
     *  Send back data
     */
    if (cache_key && m_query_cache && table->id && !more) {
      const char *cache_row_key = scan_spec->cache_key();
      char *ptr;
      uint8_t *buffer = new uint8_t [ rbuf.fill() + strlen(cache_row_key) + 1 ];
      memcpy(buffer, rbuf.base, rbuf.fill());
      ptr = (char *)buffer + rbuf.fill();
      strcpy(ptr, cache_row_key);
      boost::shared_array<uint8_t> ext_buffer(buffer);
      if ((error = cb->response(1, id, ext_buffer, rbuf.fill())) != Error::OK) {
        HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
      }
      m_query_cache->insert(cache_key, table->id, ptr, ext_buffer, rbuf.fill());
    }
    else {
      short moreflag = more ? 0 : 1;
      StaticBuffer ext(rbuf);
      if ((error = cb->response(moreflag, id, ext)) != Error::OK) {
        HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
      }
    }

  }
  catch (Hypertable::Exception &e) {
    int error;
    if (decrement_needed)
      range->decrement_scan_counter();
    if (e.code() == Error::RANGESERVER_RANGE_NOT_FOUND)
      HT_INFO_OUT << e << HT_END;
    else
      HT_ERROR_OUT << e << HT_END;
    if ((error = cb->error(e.code(), e.what())) != Error::OK)
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
  }
}


void RangeServer::destroy_scanner(ResponseCallback *cb, uint32_t scanner_id) {
  HT_DEBUGF("destroying scanner id=%u", scanner_id);
  Global::scanner_map.remove(scanner_id);
  cb->response_ok();
}


void
RangeServer::fetch_scanblock(ResponseCallbackFetchScanblock *cb,
                             uint32_t scanner_id) {
  String errmsg;
  int error = Error::OK;
  CellListScannerPtr scanner;
  RangePtr range;
  bool more = true;
  DynamicBuffer rbuf;
  TableInfoPtr table_info;
  TableIdentifierManaged scanner_table;
  SchemaPtr schema;

  HT_DEBUG_OUT <<"Scanner ID = " << scanner_id << HT_END;

  try {

    if (!Global::scanner_map.get(scanner_id, scanner, range, scanner_table))
      HT_THROW(Error::RANGESERVER_INVALID_SCANNER_ID,
               format("scanner ID %d", scanner_id));

    m_live_map->get(&scanner_table, table_info);

    schema = table_info->get_schema();

    // verify schema
    if (schema->get_generation() != scanner_table.generation) {
      Global::scanner_map.remove(scanner_id);
      HT_THROW(Error::RANGESERVER_GENERATION_MISMATCH,
               format("RangeServer Schema generation for table '%s' is %d but "
                      "scanner has generation %d", scanner_table.name,
                      schema->get_generation(), scanner_table.generation));
    }

    size_t count;
    more = FillScanBlock(scanner, rbuf, &count);

    range->add_bytes_read( rbuf.fill() );
    m_server_stats->add_scan_data(0, rbuf.fill());

    if (!more)
      Global::scanner_map.remove(scanner_id);

    /**
     *  Send back data
     */
    {
      short moreflag = more ? 0 : 1;
      StaticBuffer ext(rbuf);

      if ((error = cb->response(moreflag, scanner_id, ext)) != Error::OK)
        HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));

      HT_DEBUGF("Successfully fetched %u bytes (%d k/v pairs) of scan data",
                ext.size-4, (int)count);
    }

  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    if (cb && (error = cb->error(e.code(), e.what())) != Error::OK)
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
  }
}


void
RangeServer::load_range(ResponseCallback *cb, const TableIdentifier *table,
    const RangeSpec *range_spec, const char *transfer_log_dir,
    const RangeState *range_state) {
  int error = Error::OK;
  TableMutatorPtr mutator;
  KeySpec key;
  String metadata_key_str;
  bool is_root;
  String errmsg;
  SchemaPtr schema;
  TableInfoPtr table_info;
  RangePtr range;
  String table_dfsdir;
  String range_dfsdir;
  char md5DigestStr[33];
  bool register_table = false;
  String location;

  try {

    // this needs to be here to avoid a race condition with drop_table
    if (m_dropped_table_id_cache->contains(table->id)) {
      HT_WARNF("Table %s (id=%d) has been dropped", table->name, table->id);
      cb->error(Error::RANGESERVER_TABLE_DROPPED, table->name);
      return;
    }

    {
      ScopedLock lock(m_drop_table_mutex);

      is_root = table->id == 0 && (*range_spec->start_row == 0)
        && !strcmp(range_spec->end_row, Key::END_ROOT_ROW);

      HT_INFO_OUT <<"Loading range: "<< *table <<" "<< *range_spec << HT_END;

      if (m_dropped_table_id_cache->contains(table->id)) {
        HT_WARNF("Table %s (id=%d) has been dropped", table->name, table->id);
        cb->error(Error::RANGESERVER_TABLE_DROPPED, table->name);
        return;
      }

      HT_MAYBE_FAIL("load-range-1");

      if (!m_replay_finished)
        wait_for_recovery_finish(table, range_spec);

      /** Get TableInfo, create if doesn't exist **/
      {
        ScopedLock lock(m_mutex);
        if (!m_live_map->get(table->id, table_info)) {
          table_info = new TableInfo(m_master_client, table, schema);
          register_table = true;
        }
      }

      // Verify schema, this will create the Schema object and add it to
      // table_info if it doesn't exist
      verify_schema(table_info, table->generation);

      if (register_table)
        m_live_map->set(table->id, table_info);

      /**
       * Make sure this range is not already loaded
       */
      if (table_info->get_range(range_spec, range)) {
        HT_INFOF("Range %s[%s..%s] already loaded",
                 table->name, range_spec->start_row,
                 range_spec->end_row);
        cb->error(Error::RANGESERVER_RANGE_ALREADY_LOADED, "");
        return;
      }

      /**
       * Lazily create METADATA table pointer
       */
      if (!Global::metadata_table) {
        ScopedLock lock(m_mutex);
        // double-check locking (works fine on x86 and amd64 but may fail
        // on other archs without using a memory barrier
        if (!Global::metadata_table)
          Global::metadata_table = new Table(m_props, m_conn_manager,
                                             Global::hyperspace, "METADATA");
      }

      schema = table_info->get_schema();

      /**
       * Check for existence of and create, if necessary, range directory (md5 of
       * endrow) under all locality group directories for this table
       */
      {
        assert(*range_spec->end_row != 0);
        md5_string(range_spec->end_row, md5DigestStr);
        md5DigestStr[24] = 0;
        table_dfsdir = (String)"/hypertable/tables/" + table->name;

        foreach(Schema::AccessGroup *ag, schema->get_access_groups()) {
          // notice the below variables are different "range" vs. "table"
          range_dfsdir = table_dfsdir + "/" + ag->name + "/" + md5DigestStr;
          Global::dfs->mkdirs(range_dfsdir);
        }
      }
    }

    HT_MAYBE_FAIL_X("metadata-load-range-1", table->id==0);
    
    range = new Range(m_master_client, table, schema, range_spec,
                      table_info.get(), range_state);

    HT_MAYBE_FAIL_X("metadata-load-range-2", table->id==0);

    {
      ScopedLock lock(m_drop_table_mutex);

      if (m_dropped_table_id_cache->contains(table->id)) {
        HT_WARNF("Table %s (id=%d) has been dropped", table->name, table->id);
        cb->error(Error::RANGESERVER_TABLE_DROPPED, table->name);
        return;
      }

      /** Check again to see if range already loaded **/
      if (table_info->has_range(range_spec)) {
        HT_INFOF("Range %s[%s..%s] already loaded",
                 table->name, range_spec->start_row,
                 range_spec->end_row);
        cb->error(Error::RANGESERVER_RANGE_ALREADY_LOADED, "");
        return;
      }

      /**
       * Create root and/or metadata log if necessary
       */
      if (table->id == 0) {
        if (is_root) {
          Global::log_dfs->mkdirs(Global::log_dir + "/root");
          Global::root_log = new CommitLog(Global::log_dfs, Global::log_dir
                                           + "/root", m_props);
        }
        else if (Global::metadata_log == 0) {
          Global::log_dfs->mkdirs(Global::log_dir + "/metadata");
          Global::metadata_log = new CommitLog(Global::log_dfs,
                                               Global::log_dir + "/metadata", m_props);
        }
      }

      /**
       * NOTE: The range does not need to be locked in the following replay since
       * it has not been added yet and therefore no one else can find it and
       * concurrently access it.
       */
      if (transfer_log_dir && *transfer_log_dir) {
        CommitLogReaderPtr commit_log_reader =
          new CommitLogReader(Global::dfs, transfer_log_dir, true);
        if (!commit_log_reader->empty()) {
          CommitLog *log;
          if (is_root)
            log = Global::root_log;
          else if (table->id == 0)
            log = Global::metadata_log;
          else
            log = Global::user_log;

          range->replay_transfer_log(commit_log_reader.get());

          if ((error = log->link_log(commit_log_reader.get())) != Error::OK)
            HT_THROWF(error, "Unable to link transfer log (%s) into commit log(%s)",
                      transfer_log_dir, log->get_log_dir().c_str());

          // transfer the in-memory log fragments
          log->stitch_in(commit_log_reader.get());
        }
      }

    }

    HT_MAYBE_FAIL_X("metadata-load-range-3", table->id==0);

    /**
     * Take ownership of the range by writing the 'Location' column in the
     * METADATA table, or /hypertable/root{location} attribute of Hyperspace
     * if it is the root range.
     */

    if (!is_root) {
      metadata_key_str = format("%lu:%s", (Lu)table->id, range_spec->end_row);

      /**
       * Take ownership of the range
       */
      mutator = Global::metadata_table->create_mutator();

      key.row = metadata_key_str.c_str();
      key.row_len = strlen(metadata_key_str.c_str());
      key.column_family = "Location";
      key.column_qualifier = 0;
      key.column_qualifier_len = 0;
      location = Location::get();
      mutator->set(key, location.c_str(), location.length());
      mutator->flush();
    }
    else {  //root
      uint64_t handle;
      HandleCallbackPtr null_callback;
      uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_CREATE;

      HT_INFO("Loading root METADATA range");

      try {
        handle = m_hyperspace->open("/hypertable/root", oflags, null_callback);
	location = Location::get();
        m_hyperspace->attr_set(handle, "Location", location.c_str(),
                               location.length());
        m_hyperspace->close(handle);
      }
      catch (Exception &e) {
        HT_ERROR_OUT << "Problem setting attribute 'location' on Hyperspace "
          "file '/hypertable/root'" << HT_END;
        HT_ERROR_OUT << e << HT_END;
        HT_ABORT;
      }

    }

    HT_MAYBE_FAIL_X("metadata-load-range-4", table->id==0);

    {
      ScopedLock lock(m_drop_table_mutex);

      if (m_dropped_table_id_cache->contains(table->id)) {
        HT_WARNF("Table %s (id=%d) has been dropped", table->name, table->id);
        cb->error(Error::RANGESERVER_TABLE_DROPPED, table->name);
        return;
      }

      if (Global::range_log)
        Global::range_log->log_range_loaded(*table, *range_spec, *range_state);

      table_info->add_range(range);
    }

    HT_MAYBE_FAIL_X("metadata-load-range-5", table->id==0);

    if (cb && (error = cb->response_ok()) != Error::OK)
      HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
    else
      HT_INFOF("Successfully loaded range %s[%s..%s]", table->name,
               range_spec->start_row, range_spec->end_row);

  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    if (cb && (error = cb->error(e.code(), e.what())) != Error::OK)
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
  }

}

void
RangeServer::update_schema(ResponseCallback *cb,
    const TableIdentifier *table, const char *schema_str) {
  ScopedLock lock(m_drop_table_mutex);
  TableInfoPtr table_info;
  SchemaPtr schema;

  HT_INFO_OUT <<"Updating schema for: "<< *table <<" schema = "<<
      schema_str << HT_END;

  try {
    /**
     * Create new schema object and test for validity
     */
    schema = Schema::new_instance(schema_str, strlen(schema_str), true);

    if (!schema->is_valid()) {
      HT_THROW(Error::RANGESERVER_SCHEMA_PARSE_ERROR,
        (String) "Update schema Parse Error for table '"
        + table->name + "' : " + schema->get_error_string());
    }

    m_live_map->get(table, table_info);

    table_info->update_schema(schema);

  }
  catch(Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb->error(e.code(), e.what());
    return;
  }

  HT_INFO_OUT << "Successfully updated schema for: "<< *table << HT_END;
  cb->response_ok();
  return;
}


void
RangeServer::transform_key(ByteString &bskey, DynamicBuffer *dest_bufp,
                           int64_t auto_revision, int64_t *revisionp) {
  size_t len;
  const uint8_t *ptr;

  len = bskey.decode_length(&ptr);

  if (*ptr == Key::AUTO_TIMESTAMP) {
    dest_bufp->ensure( (ptr-bskey.ptr) + len + 9 );
    Serialization::encode_vi32(&dest_bufp->ptr, len+8);
    memcpy(dest_bufp->ptr, ptr, len);
    *dest_bufp->ptr = Key::HAVE_REVISION
        | Key::HAVE_TIMESTAMP | Key::REV_IS_TS;
    dest_bufp->ptr += len;
    Key::encode_ts64(&dest_bufp->ptr, auto_revision);
    *revisionp = auto_revision;
    bskey.ptr = ptr + len;
  }
  else if (*ptr == Key::HAVE_TIMESTAMP) {
    dest_bufp->ensure( (ptr-bskey.ptr) + len + 9 );
    Serialization::encode_vi32(&dest_bufp->ptr, len+8);
    memcpy(dest_bufp->ptr, ptr, len);
    *dest_bufp->ptr = Key::HAVE_REVISION
        | Key::HAVE_TIMESTAMP;
    dest_bufp->ptr += len;
    Key::encode_ts64(&dest_bufp->ptr, auto_revision);
    *revisionp = auto_revision;
    bskey.ptr = ptr + len;
  }
  else {
    HT_THROW(Error::BAD_KEY , (String) "Unknown key control flag in key");
  }

  return;
}


namespace {

  struct UpdateExtent {
    const uint8_t *base;
    uint64_t len;
  };

  struct SendBackRec {
    int error;
    uint32_t count;
    uint32_t offset;
    uint32_t len;
  };

  struct RangeUpdateInfo {
    RangePtr range;
    DynamicBuffer *bufp;
    uint64_t offset;
    uint64_t len;
  };

}

void
RangeServer::commit_log_sync(ResponseCallback *cb) {
  String errmsg;
  int error = Error::OK;
  CommitLog* commit_log = 0;

  HT_DEBUG_OUT <<"received commit_log_sync request"<< HT_END;

  if (!m_replay_finished)
    wait_for_recovery_finish();

  // Global commit log is only available after local recovery
  commit_log = Global::user_log;

  try {
    commit_log->sync();
    HT_DEBUG_OUT << "commit log synced" << HT_END;
  }
  catch (Exception &e) {
    HT_ERRORF("Exception caught: %s", Error::get_text(e.code()));
    error = e.code();
    errmsg = e.what();
  }

  /**
   * Send back response
   */
  if (error == Error::OK) {
    if ((error = cb->response_ok()) != Error::OK)
      HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
  }
  else {
    HT_ERRORF("%s '%s'", Error::get_text(error), errmsg.c_str());
    if ((error = cb->error(error, errmsg)) != Error::OK)
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
  }
}


void
RangeServer::update(ResponseCallbackUpdate *cb, const TableIdentifier *table,
                    uint32_t count, StaticBuffer &buffer, uint32_t flags) {
  const uint8_t *mod, *mod_end;
  String errmsg;
  int error = Error::OK;
  TableInfoPtr table_info;
  int64_t last_revision;
  int64_t latest_range_revision;
  const char *row, *last_row;
  SplitPredicate split_predicate;
  CommitLogPtr splitlog;
  bool split_pending;
  SerializedKey key;
  ByteString value;
  bool a_locked = false;
  bool b_locked = false;
  vector<SendBackRec> send_back_vector;
  SendBackRec send_back;
  uint32_t total_added = 0;
  uint32_t split_added = 0;
  std::vector<RangeUpdateInfo> range_vector;
  DynamicBuffer root_buf;
  DynamicBuffer go_buf;
  DynamicBuffer *split_bufp = 0;
  std::vector<DynamicBufferPtr> split_bufs;
  DynamicBuffer *cur_bufp;
  uint32_t misses = 0;
  RangeUpdateInfo rui;
  std::set<Range *> reference_set;
  std::pair<std::set<Range *>::iterator, bool> reference_set_state;
  String start_row, end_row;
  std::vector<RangePtr> wait_ranges;
  bool wait_for_maintenance;
  bool wait_for_metadata_recovery = false;
  bool sync = !((flags & RangeServerProtocol::UPDATE_FLAG_NO_LOG_SYNC) ==
      RangeServerProtocol::UPDATE_FLAG_NO_LOG_SYNC);

  // Pre-allocate the go_buf - each key could expand by 8 or 9 bytes,
  // if auto-assigned (8 for the ts or rev and maybe 1 for possible
  // increase in vint length)
  const uint32_t encoded_table_len = table->encoded_length();
  go_buf.reserve(encoded_table_len + buffer.size + (count * 9));
  table->encode(&go_buf.ptr);

  if (m_update_delay)
    poll(0, 0, m_update_delay);

  HT_DEBUG_OUT <<"Update:\n"<< *table << HT_END;

  if (!m_replay_finished) {
    if (table->id == 0) {
      wait_for_root_recovery_finish();
      wait_for_metadata_recovery = true;
    }
    else
      wait_for_recovery_finish();
  }

  // Global commit log is only available after local recovery
  int64_t auto_revision = Hypertable::get_ts64();

  // TODO: Sanity check mod data (checksum validation)

  try {

    m_live_map->get(table, table_info);

    // verify schema
    if (table_info->get_schema()->get_generation() != table->generation)
      HT_THROW(Error::RANGESERVER_GENERATION_MISMATCH,
               (String)"RangeServer Schema generation for table '" +
               table_info ->get_name() + "' is " +
               table_info->get_schema()->get_generation()
               + " but supplied is " + table->generation);

    mod_end = buffer.base + buffer.size;
    mod = buffer.base;

    m_update_mutex_a.lock();
    a_locked = true;

    memset(&send_back, 0, sizeof(send_back));

    while (mod < mod_end) {
      key.ptr = mod;
      row = key.row();

      // If the row key starts with '\0' then the buffer is probably
      // corrupt, so mark the remaing key/value pairs as bad
      if (*row == 0) {
        send_back.error = Error::BAD_KEY;
        send_back.count = count;  // fix me !!!!
        send_back.offset = mod - buffer.base;
        send_back.len = mod_end - mod;
        send_back_vector.push_back(send_back);
        memset(&send_back, 0, sizeof(send_back));
        mod = mod_end;
        continue;
      }

      // Look for containing range, add to stop mods if not found
      if (!table_info->find_containing_range(row, rui.range,
                                             start_row, end_row)) {
        if (send_back.error != Error::RANGESERVER_OUT_OF_RANGE
            && send_back.count > 0) {
          send_back_vector.push_back(send_back);
          memset(&send_back, 0, sizeof(send_back));
        }
        if (send_back.count == 0) {
          send_back.error = Error::RANGESERVER_OUT_OF_RANGE;
          send_back.offset = mod - buffer.base;
        }
        key.next(); // skip key
        key.next(); // skip value;
        mod = key.ptr;
        send_back.count++;
        continue;
      }

      if (wait_for_metadata_recovery && !rui.range->is_root()) {
	wait_for_metadata_recovery_finish();
	wait_for_metadata_recovery = false;
      }

      // See if range has some other error preventing it from receiving updates
      if ((error = rui.range->get_error()) != Error::OK) {
        if (send_back.error != error && send_back.count > 0) {
          send_back_vector.push_back(send_back);
          memset(&send_back, 0, sizeof(send_back));
        }
        if (send_back.count == 0) {
          send_back.error = error;
          send_back.offset = mod - buffer.base;
        }
        key.next(); // skip key
        key.next(); // skip value;
        mod = key.ptr;
        send_back.count++;
        continue;
      }

      if (send_back.count > 0) {
        send_back.len = (mod - buffer.base) - send_back.offset;
        send_back_vector.push_back(send_back);
        memset(&send_back, 0, sizeof(send_back));
      }

      /** Increment update count (block if maintenance in progress) **/
      reference_set_state = reference_set.insert(rui.range.get());
      if (reference_set_state.second)
        rui.range->increment_update_counter();

      // Make sure range didn't just shrink
      if (rui.range->start_row() != start_row ||
          rui.range->end_row() != end_row) {
        if (reference_set_state.second) {
          rui.range->decrement_update_counter();
          reference_set.erase(rui.range.get());
        }
        continue;
      }

      /** Fetch range split information **/
      split_pending = rui.range->get_split_info(split_predicate, splitlog,
                                                &latest_range_revision, wait_for_maintenance);
      if (wait_for_maintenance)
        wait_ranges.push_back(rui.range);

      bool in_split_off_region = false;

      // Check for clock skew
      {
        ByteString tmp_key;
        const uint8_t *tmp;
        int64_t difference, tmp_timestamp;
        tmp_key.ptr = key.ptr;
        tmp_key.decode_length(&tmp);
        if ((*tmp & Key::HAVE_REVISION) == 0) {
          if (latest_range_revision > TIMESTAMP_MIN
              && auto_revision < latest_range_revision) {
            tmp_timestamp = Hypertable::get_ts64();
            if (tmp_timestamp > auto_revision)
              auto_revision = tmp_timestamp;
            if (auto_revision < latest_range_revision) {
              difference = (int32_t)((latest_range_revision - auto_revision)
                            / 1000LL);
              if (difference > m_max_clock_skew)
                HT_THROWF(Error::RANGESERVER_CLOCK_SKEW,
                          "Clocks skew of %lld microseconds exceeds maximum "
                          "(%lld) range=%s", (Lld)difference,
                          (Lld)m_max_clock_skew,
                          rui.range->get_name().c_str());
            }
          }
        }
      }

      if (split_pending) {
        split_bufp = new DynamicBuffer();
        split_bufs.push_back(split_bufp);
        split_bufp->reserve(encoded_table_len);
        table->encode(&split_bufp->ptr);
      }
      else
        split_bufp = 0;

      if (rui.range->is_root())
        cur_bufp = &root_buf;
      else
        cur_bufp = &go_buf;

      if (cur_bufp->ptr == 0) {
        cur_bufp->reserve(encoded_table_len);
        table->encode(&cur_bufp->ptr);
      }

      rui.bufp = cur_bufp;
      rui.offset = cur_bufp->fill();

      while (mod < mod_end && (end_row == ""
             || (strcmp(row, end_row.c_str()) <= 0))) {

        if (split_pending) {

          if (split_predicate.split_off(row)) {
            if (!in_split_off_region) {
              rui.len = cur_bufp->fill() - rui.offset;
              if (rui.len)
                range_vector.push_back(rui);
              cur_bufp = split_bufp;
              rui.bufp = cur_bufp;
              rui.offset = cur_bufp->fill();
              in_split_off_region = true;
            }
            split_added++;
          }
          else {
            if (in_split_off_region) {
              rui.len = cur_bufp->fill() - rui.offset;
              if (rui.len)
                range_vector.push_back(rui);
              cur_bufp = &go_buf;
              rui.bufp = cur_bufp;
              rui.offset = cur_bufp->fill();
              in_split_off_region = false;
            }
          }
        }

        // This will transform keys that need to be assigned a
        // timestamp and/or revision number by re-writing the key
        // with the added timestamp and/or revision tacked on to the end
        transform_key(key, cur_bufp, ++auto_revision, &last_revision);

        // Validate revision number
        if (last_revision < latest_range_revision) {
          if (last_revision != auto_revision)
            HT_THROWF(Error::RANGESERVER_REVISION_ORDER_ERROR,
                      "Supplied revision (%lld) is less than most recently "
                      "seen revision (%lld) for range %s",
                      (Lld)last_revision, (Lld)latest_range_revision,
                      rui.range->get_name().c_str());
        }

        // Now copy the value (with sanity check)
        mod = key.ptr;
        key.next(); // skip value
        HT_ASSERT(key.ptr <= mod_end);
        cur_bufp->add(mod, key.ptr-mod);
        mod = key.ptr;

        total_added++;

        if (mod < mod_end)
          row = key.row();
      }

      rui.len = cur_bufp->fill() - rui.offset;
      if (rui.len)
        range_vector.push_back(rui);
      rui.range = 0;
      rui.bufp = 0;

      // if there were split-off updates, write the split log entry
      if (split_bufp && split_bufp->fill() > encoded_table_len) {
        if ((error = splitlog->write(*split_bufp, last_revision)) != Error::OK)
          HT_THROWF(error, "Problem writing %d bytes to split log",
                    (int)split_bufp->fill());
        splitlog = 0;
      }
    }

    HT_DEBUGF("Added %d (%d split off) updates to '%s'", total_added,
              split_added, table->name);

    if (send_back.count > 0) {
      send_back.len = (mod - buffer.base) - send_back.offset;
      send_back_vector.push_back(send_back);
      memset(&send_back, 0, sizeof(send_back));
    }

    m_update_mutex_b.lock();
    b_locked = true;

    m_update_mutex_a.unlock();
    a_locked = false;

    /**
     * Commit ROOT mutations
     */
    if (root_buf.fill() > encoded_table_len) {
      if ((error = Global::root_log->write(root_buf, last_revision))
          != Error::OK)
        HT_THROWF(error, "Problem writing %d bytes to ROOT commit log",
                  (int)root_buf.fill());
    }

    /**
     * Commit valid (go) mutations
     */
    if (go_buf.fill() > encoded_table_len) {
      CommitLog *log;
      if (table->id == 0) {
        HT_ASSERT(sync == true);
        log = Global::metadata_log;
      }
      else
        log = Global::user_log;

      if ((error = log->write(go_buf, last_revision, sync)) != Error::OK)
        HT_THROWF(error, "Problem writing %d bytes to commit log (%s)",
                  (int)go_buf.fill(), log->get_log_dir().c_str());
    }

    for (size_t rangei=0; rangei<range_vector.size(); rangei++) {

      /**
       * Apply the modifications
       */
      {
        Locker<Range> lock(*range_vector[rangei].range);
        Key key_comps;
        uint8_t *ptr = range_vector[rangei].bufp->base
            + range_vector[rangei].offset;
        uint8_t *end = ptr + range_vector[rangei].len;
        range_vector[rangei].range->add_bytes_written( range_vector[rangei].len );
	last_row = "";
        while (ptr < end) {
          key.ptr = ptr;
          key_comps.load(key);
          if (key_comps.column_family_code == 0 && key_comps.flag != FLAG_DELETE_ROW) {
            HT_THROW(Error::BAD_KEY,
                (String)"Column family not specified in non-delete row update on "
                + table_info->get_name() + " row= " + key_comps.row);
          }
          ptr += key_comps.length;
          value.ptr = ptr;
          ptr += value.length();
          range_vector[rangei].range->add(key_comps, value);
	  // invalidate
	  if (m_query_cache && strcmp(last_row, key_comps.row))
	    m_query_cache->invalidate(table->id, key_comps.row);
          last_row = key_comps.row;
        }
      }

      if (range_vector[rangei].range->need_maintenance() &&
          !Global::maintenance_queue->is_scheduled(range_vector[rangei].range.get())) {
	ScopedLock lock(m_mutex);
        m_maintenance_scheduler->need_scheduling();
	if (m_timer_handler)
	  m_timer_handler->schedule_maintenance();
      }

    }

    if (Global::verbose && misses)
      HT_INFOF("Sent back %d updates because out-of-range", misses);

    error = Error::OK;
  }
  catch (Exception &e) {
    HT_ERRORF("Exception caught: %s", Error::get_text(e.code()));
    error = e.code();
    errmsg = e.what();
  }

  // decrement usage counters for all referenced ranges
  foreach(Range *range, reference_set)
    range->decrement_update_counter();

  if (b_locked)
    m_update_mutex_b.unlock();
  else if (a_locked)
    m_update_mutex_a.unlock();

  /**
   * wait for these ranges to complete maintenance
   */
  foreach(RangePtr range, wait_ranges)
    range->wait_for_maintenance_to_complete();

  if (table->id != 0)
    m_server_stats->add_update_data(1, buffer.size);

  if (error == Error::OK) {
    /**
     * Send back response
     */
    if (!send_back_vector.empty()) {
      StaticBuffer ext(new uint8_t [send_back_vector.size() * 16],
                       send_back_vector.size() * 16);
      uint8_t *ptr = ext.base;
      for (size_t i=0; i<send_back_vector.size(); i++) {
        encode_i32(&ptr, send_back_vector[i].error);
        encode_i32(&ptr, send_back_vector[i].count);
        encode_i32(&ptr, send_back_vector[i].offset);
        encode_i32(&ptr, send_back_vector[i].len);
        HT_INFOF("omega Sending back error %x, count %d, offset %d, len %d",
                 send_back_vector[i].error, send_back_vector[i].count,
                 send_back_vector[i].offset, send_back_vector[i].len);
      }
      if ((error = cb->response(ext)) != Error::OK)
        HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
    }
    else {
      if ((error = cb->response_ok()) != Error::OK)
        HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
    }
  }
  else {
    HT_ERRORF("%s '%s'", Error::get_text(error), errmsg.c_str());
    if ((error = cb->error(error, errmsg)) != Error::OK)
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
  }
}


void
RangeServer::drop_table(ResponseCallback *cb, const TableIdentifier *table) {
  TableInfoPtr table_info;
  std::vector<RangePtr> range_vector;
  String metadata_prefix;
  String metadata_key;
  TableMutatorPtr mutator;
  KeySpec key;

  HT_INFO_OUT << "drop table " << table->name << HT_END;

  if (!m_replay_finished)
    wait_for_recovery_finish();

  {
    ScopedLock lock(m_drop_table_mutex);

    m_dropped_table_id_cache->insert(table->id);

    // create METADATA table mutator for clearing 'Location' columns
    mutator = Global::metadata_table->create_mutator();

    // initialize key structure
    memset(&key, 0, sizeof(key));
    key.column_family = "Location";

    try {

      // For each range in dropped table, Set the 'drop' bit and clear
      // the 'Location' column of the corresponding METADATA entry
      if (m_live_map->remove(table->id, table_info)) {
        metadata_prefix = String("") + table_info->get_id() + ":";
        table_info->get_range_vector(range_vector);
        for (size_t i=0; i<range_vector.size(); i++) {
          range_vector[i]->drop();
          metadata_key = metadata_prefix + range_vector[i]->end_row();
          key.row = metadata_key.c_str();
          key.row_len = metadata_key.length();
          mutator->set(key, "!", 1);
        }
      }
      else {
        HT_ERRORF("drop_table '%s' id=%u - table not found", table->name,
                  table->id);
      }
      mutator->flush();
    }
    catch (Hypertable::Exception &e) {
      HT_ERRORF("Problem clearing 'Location' columns of METADATA - %s",
                Error::get_text(e.code()));
      cb->error(e.code(), "Problem clearing 'Location' columns of METADATA");
      return;
    }

  }

  for (size_t i=0; i<range_vector.size(); i++)
    range_vector[i]->wait_for_maintenance_to_complete();

  // write range transaction entry
  if (Global::range_log)
    Global::range_log->log_drop_table(*table);

  HT_INFOF("Successfully dropped table '%s'", table->name);

  cb->response_ok();
}


void RangeServer::dump(ResponseCallback *cb, const char *outfile,
		       bool nokeys) {
  RangeStatsVector range_data;
  AccessGroup::MaintenanceData *ag_data;
  RangeStatsGatherer stats_gatherer(m_live_map);
  String str, ag_name;

  HT_INFO("dump");

  try {
    std::ofstream out(outfile);

    stats_gatherer.fetch(range_data);

    AccessGroup::CellStoreMaintenanceData *cs_data;
    int64_t block_index_memory = 0;
    int64_t bloom_filter_memory = 0;
    int64_t shadow_cache_memory = 0;

    for (size_t i=0; i<range_data.size(); i++) {
      for (ag_data = range_data[i]->agdata; ag_data; ag_data = ag_data->next) {
	for (cs_data = ag_data->csdata; cs_data; cs_data = cs_data->next) {
	  shadow_cache_memory += cs_data->shadow_cache_size;
	  block_index_memory += cs_data->index_stats.block_index_memory;
	  bloom_filter_memory += cs_data->index_stats.bloom_filter_memory;
	}
	ag_name = range_data[i]->range->get_name() + "(" + ag_data->ag->get_name() + ")";
	out << ag_name << "\tecr\t" << ag_data->earliest_cached_revision << "\n";
	out << ag_name << "\tlsr\t" << ag_data->latest_stored_revision << "\n";
	out << ag_name << "\tmemory\t" << ag_data->mem_used << "\n";
	out << ag_name << "\tcached items\t" << ag_data->cached_items << "\n";
	out << ag_name << "\timmutable items\t" << ag_data->immutable_items << "\n";
	out << ag_name << "\tdisk\t" << ag_data->disk_used << "\n";
	out << ag_name << "\tscanners\t" << ag_data->outstanding_scanners << "\n";
	out << ag_name << "\tbindex_mem\t" << block_index_memory << "\n";
	out << ag_name << "\tbloom_mem\t" << bloom_filter_memory << "\n";
	out << ag_name << "\tshadow_mem\t" << shadow_cache_memory << "\n";
      }
    }

    // dump keys
    if (!nokeys) {
      for (size_t i=0; i<range_data.size(); i++)
	for (ag_data = range_data[i]->agdata; ag_data; ag_data = ag_data->next)
	  ag_data->ag->dump_keys(out);
    }

    out << "\nCommit Log Info\n";
    str = "";

    if (Global::root_log)
      Global::root_log->get_stats("ROOT", str);

    if (Global::metadata_log)
      Global::metadata_log->get_stats("METADATA", str);

    if (Global::user_log)
      Global::user_log->get_stats("USER", str);

    out << str;

  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb->error(e.code(), "Problem executing dump() command");
    return;
  }
  catch (std::exception &e) {
    cb->error(Error::LOCAL_IO_ERROR, e.what());
    return;
  }
  cb->response_ok();
}

void RangeServer::get_statistics(ResponseCallbackGetStatistics *cb) {
  std::vector<TableInfoPtr> table_vec;
  std::vector<RangePtr> range_vec;
  RangeServerStat stat;

  HT_DEBUG("get_statistics");

  m_live_map->get_all(table_vec);

  for (size_t i=0; i<table_vec.size(); i++) {
    range_vec.clear();
    table_vec[i]->get_range_vector(range_vec);
    for (size_t i=0; i<range_vec.size(); i++) {
      RangeStat rstat;
      range_vec[i]->get_statistics(&rstat);
      stat.range_stats.push_back(rstat);
    }
  }

  StaticBuffer ext(stat.encoded_length());
  uint8_t *bufp = ext.base;
  stat.encode(&bufp);

  cb->response(ext);
}


void RangeServer::replay_begin(ResponseCallback *cb, uint16_t group) {
  String replay_log_dir = String("/hypertable/servers/") + Location::get() + "/log/replay";

  m_replay_group = group;

  HT_INFOF("replay_start group=%d", m_replay_group);

  m_replay_log = 0;

  m_replay_map->clear_ranges();

  /**
   * Remove old replay log directory
   */
  try { Global::log_dfs->rmdir(replay_log_dir); }
  catch (Exception &e) {
    HT_ERRORF("Problem removing replay log directory: %s", e.what());
    cb->error(e.code(), format("Problem removing replay log directory: %s",
              e.what()));
    return;
  }

  /**
   * Create new replay log directory
   */
  try { Global::log_dfs->mkdirs(replay_log_dir); }
  catch (Exception &e) {
    HT_ERRORF("Problem creating replay log directory: %s ", e.what());
    cb->error(e.code(), format("Problem creating replay log directory: %s",
              e.what()));
    return;
  }

  m_replay_log = new CommitLog(Global::log_dfs, replay_log_dir, m_props);

  cb->response_ok();
}


void
RangeServer::replay_load_range(ResponseCallback *cb,
    const TableIdentifier *table, const RangeSpec *range_spec,
    const RangeState *range_state, bool write_rsml) {
  int error = Error::OK;
  SchemaPtr schema;
  TableInfoPtr table_info, live_table_info;
  RangePtr range;
  bool register_table = false;

  HT_DEBUG_OUT<< "replay_load_range\n"<< *table << *range_spec << HT_END;

  try {

    /** Get TableInfo from replay map, or copy it from live map, or create if
     * doesn't exist **/
    if (!m_replay_map->get(table->id, table_info)) {
      table_info = new TableInfo(m_master_client, table, schema);
      register_table = true;
    }

    if (!m_live_map->get(table->id, live_table_info))
      live_table_info = table_info;

    // Verify schema, this will create the Schema object and add it to
    // table_info if it doesn't exist
    verify_schema(table_info, table->generation);

    if (register_table)
      m_replay_map->set(table->id, table_info);

    /**
     * Make sure this range is not already loaded
     */
    if (table_info->get_range(range_spec, range) ||
        live_table_info->get_range(range_spec, range))
      HT_THROWF(Error::RANGESERVER_RANGE_ALREADY_LOADED, "%s[%s..%s]",
                table->name, range_spec->start_row, range_spec->end_row);

    /**
     * Lazily create METADATA table pointer
     */
    if (!Global::metadata_table) {
      ScopedLock lock(m_mutex);
      Global::metadata_table = new Table(m_props, m_conn_manager,
          Global::hyperspace, "METADATA");
    }

    schema = table_info->get_schema();

    range = new Range(m_master_client, table, schema, range_spec,
                      live_table_info.get(), range_state);

    range->recovery_initialize();

    table_info->add_range(range);

    if (write_rsml)
      Global::range_log->log_range_loaded(*table, *range_spec, *range_state);

    if (cb && (error = cb->response_ok()) != Error::OK) {
      HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
    }
    else {
      HT_INFOF("Successfully replay loaded range %s[%s..%s]", table->name,
               range_spec->start_row, range_spec->end_row);
    }

  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    if (cb && (error = cb->error(e.code(), e.what())) != Error::OK)
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
  }
}



void
RangeServer::replay_update(ResponseCallback *cb, const uint8_t *data,
                           size_t len) {
  TableIdentifier table_identifier;
  TableInfoPtr table_info;
  SerializedKey serkey;
  ByteString bsvalue;
  Key key;
  const uint8_t *ptr = data;
  const uint8_t *end = data + len;
  const uint8_t *block_end;
  uint32_t block_size;
  size_t remaining = len;
  const char *row;
  String err_msg;
  int64_t revision;
  RangePtr range;
  String start_row, end_row;
  int error;

  //HT_DEBUGF("replay_update - length=%ld", len);

  try {

    while (ptr < end) {

      // decode key/value block size + revision
      block_size = decode_i32(&ptr, &remaining);
      revision = decode_i64(&ptr, &remaining);

      if (m_replay_log) {
        DynamicBuffer dbuf(0, false);
        dbuf.base = (uint8_t *)ptr;
        dbuf.ptr = dbuf.base + remaining;

        if ((error = m_replay_log->write(dbuf, revision)) != Error::OK)
          HT_THROW(error, "");
      }

      // decode table identifier
      table_identifier.decode(&ptr, &remaining);

      if (block_size > remaining)
        HT_THROWF(Error::MALFORMED_REQUEST, "Block (size=%lu) exceeds EOM",
                  (Lu)block_size);

      block_end = ptr + block_size;

      // Fetch table info
      if (!m_replay_map->get(table_identifier.id, table_info))
        HT_THROWF(Error::RANGESERVER_RANGE_NOT_FOUND, "Unable to find "
                  "table info for table name='%s' id=%lu",
                  table_identifier.name, (Lu)table_identifier.id);

      while (ptr < block_end) {

        row = SerializedKey(ptr).row();

        // Look for containing range, add to stop mods if not found
        if (!table_info->find_containing_range(row, range,
                                               start_row, end_row))
          HT_THROWF(Error::RANGESERVER_RANGE_NOT_FOUND, "Unable to find "
                    "range for row '%s'", row);

        serkey.ptr = ptr;

        while (ptr < block_end
            && (end_row == "" || (strcmp(row, end_row.c_str()) <= 0))) {

          // extract the key
          ptr += serkey.length();

          if (ptr > end)
            HT_THROW(Error::REQUEST_TRUNCATED, "Problem decoding key");

          bsvalue.ptr = ptr;
          ptr += bsvalue.length();

          if (ptr > end)
            HT_THROW(Error::REQUEST_TRUNCATED, "Problem decoding value");

          key.load(serkey);

          {
            Locker<Range> lock(*range);
            range->add(key, bsvalue);
          }
          serkey.ptr = ptr;

          if (ptr < block_end)
            row = serkey.row();
        }
      }
    }

  }
  catch (Exception &e) {

    if (e.code() == Error::RANGESERVER_RANGE_NOT_FOUND)
      HT_INFO_OUT << e << HT_END;
    else
      HT_ERROR_OUT << e << HT_END;

    if (cb) {
      cb->error(e.code(), format("%s - %s", e.what(),
                Error::get_text(e.code())));
      return;
    }
    HT_THROW(e.code(), e.what());
  }

  if (cb)
    cb->response_ok();
}


void RangeServer::replay_commit(ResponseCallback *cb) {
  int error;

  HT_INFO("replay_commit");

  try {
    CommitLog *log = 0;
    std::vector<RangePtr> rangev;

    if (m_replay_group == RangeServerProtocol::GROUP_METADATA_ROOT)
      log = Global::root_log;
    else if (m_replay_group == RangeServerProtocol::GROUP_METADATA)
      log = Global::metadata_log;
    else if (m_replay_group == RangeServerProtocol::GROUP_USER)
      log = Global::user_log;

    /** FIX ME - should we link here?  what about stitch_in? **/
    if ((error = log->link_log(m_replay_log.get())) != Error::OK)
      HT_THROW(error, String("Problem linking replay log (")
               + m_replay_log->get_log_dir() + ") into commit log ("
               + log->get_log_dir() + ")");

    // Perform any range specific post-replay tasks
    m_replay_map->get_range_vector(rangev);
    foreach(RangePtr &range, rangev)
      range->recovery_finalize();

    m_live_map->merge(m_replay_map);

  }
  catch (Hypertable::Exception &e) {
    HT_ERRORF("%s - %s", e.what(), Error::get_text(e.code()));
    if (cb) {
      cb->error(e.code(), e.what());
      return;
    }
    HT_THROW(e.code(), e.what());
  }

  if (cb)
    cb->response_ok();
}



void
RangeServer::drop_range(ResponseCallback *cb, const TableIdentifier *table,
                        const RangeSpec *range_spec) {
  TableInfoPtr table_info;
  RangePtr range;

  HT_INFO_OUT << "drop_range\n"<< *table << *range_spec << HT_END;

  try {

    m_live_map->get(table->id, table_info);

    /** Remove the range **/
    if (!table_info->remove_range(range_spec, range))
      HT_THROW(Error::RANGESERVER_RANGE_NOT_FOUND,
               format("%s[%s..%s]", table->name, range_spec->start_row, range_spec->end_row));

    cb->response_ok();
  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    int error = 0;
    if (cb && (error = cb->error(e.code(), e.what())) != Error::OK)
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
  }

}


void RangeServer::close(ResponseCallback *cb) {
  std::vector<TableInfoPtr> table_vec;
  std::vector<RangePtr> range_vec;

  (void)cb;

  HT_INFO("close");

  Global::maintenance_queue->stop();

  // block updates
  m_update_mutex_a.lock();
  m_update_mutex_b.lock();

  // get the tables
  m_live_map->get_all(table_vec);

  // add all ranges into the range vector
  for (size_t i=0; i<table_vec.size(); i++)
    table_vec[i]->get_range_vector(range_vec);

  // increment the update counters
  for (size_t i=0; i<range_vec.size(); i++)
    range_vec[i]->increment_update_counter();

  if (Global::range_log)
    Global::range_log->close();

  if (Global::root_log)
    Global::root_log->close();

  if (Global::metadata_log)
    Global::metadata_log->close();

  if (Global::user_log)
    Global::user_log->close();

  cb->response_ok();

}


void RangeServer::verify_schema(TableInfoPtr &table_info, uint32_t generation) {
  DynamicBuffer valbuf;
  HandleCallbackPtr null_handle_callback;
  uint64_t handle;
  SchemaPtr schema = table_info->get_schema();

  if (schema.get() == 0 || schema->get_generation() < generation) {
    String tablefile = (String)"/hypertable/tables/" + table_info->get_name();

    handle = m_hyperspace->open(tablefile.c_str(), OPEN_FLAG_READ,
                                null_handle_callback);

    m_hyperspace->attr_get(handle, "schema", valbuf);

    m_hyperspace->close(handle);

    schema = Schema::new_instance((char *)valbuf.base, valbuf.fill(), true);

    if (!schema->is_valid())
      HT_THROW(Error::RANGESERVER_SCHEMA_PARSE_ERROR,
               (String)"Schema Parse Error for table '"
               + table_info->get_name() + "' : " + schema->get_error_string());

    table_info->update_schema(schema);

    // Generation check ...
    if (schema->get_generation() < generation)
      HT_THROW(Error::RANGESERVER_GENERATION_MISMATCH,
               (String)"Fetched Schema generation for table '"
               + table_info->get_name() + "' is " + schema->get_generation()
               + " but supplied is " + generation);
  }
}


void RangeServer::do_maintenance() {

  HT_ASSERT(m_timer_handler);

  try {

    // Purge expired scanners
    Global::scanner_map.purge_expired(m_scanner_ttl);

    // Set Low Memory mode
    m_maintenance_scheduler->set_low_memory_mode( m_timer_handler->low_memory() );

    // Recompute stats
    m_server_stats->recompute();

    // Schedule maintenance
    m_maintenance_scheduler->schedule();

  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
  }

  // Notify timer handler so that it can resume
  m_timer_handler->complete_maintenance_notify();

  HT_INFOF("Memory Usage: %llu bytes", (Llu)Global::memory_tracker->balance());
  if (m_timer_handler->low_memory())
    HT_INFO("Application queue PAUSED due to low memory condition");
}



void RangeServer::wait_for_recovery_finish() {
  ScopedLock lock(m_mutex);
  while (!m_replay_finished) {
    HT_INFO_OUT << "Waiting for recovery to complete..." << HT_END;
    m_replay_finished_cond.wait(lock);
  }
}

void RangeServer::wait_for_root_recovery_finish() {
  ScopedLock lock(m_mutex);
  while (!m_root_replay_finished) {
    HT_INFO_OUT << "Waiting for ROOT recovery to complete..." << HT_END;
    m_root_replay_finished_cond.wait(lock);
  }
}

void RangeServer::wait_for_metadata_recovery_finish() {
  ScopedLock lock(m_mutex);
  while (!m_metadata_replay_finished) {
    HT_INFO_OUT << "Waiting for METADATA recovery to complete..." << HT_END;
    m_metadata_replay_finished_cond.wait(lock);
  }
}


void
RangeServer::wait_for_recovery_finish(const TableIdentifier *table,
                                      const RangeSpec *range_spec) {
  ScopedLock lock(m_mutex);
  if (table->id == 0) {
    if (!strcmp(range_spec->end_row, Key::END_ROOT_ROW)) {
      while (!m_root_replay_finished) {
        HT_INFO_OUT << "Waiting for ROOT recovery to complete..." << HT_END;
        m_root_replay_finished_cond.wait(lock);
      }
    }
    else {
      while (!m_metadata_replay_finished) {
        HT_INFO_OUT << "Waiting for METADATA recovery to complete..." << HT_END;
        m_metadata_replay_finished_cond.wait(lock);
      }
    }
  }
  else {
    while (!m_replay_finished) {
      HT_INFO_OUT << "Waiting for recovery to complete..." << HT_END;
      m_replay_finished_cond.wait(lock);
    }
  }
}
