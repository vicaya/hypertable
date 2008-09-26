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

#include "Common/Compat.h"
#include <cassert>
#include <string>

#include <boost/shared_array.hpp>

extern "C" {
#include <math.h>
#include <sys/time.h>
#include <sys/resource.h>
}

#include "Common/FileUtils.h"
#include "Common/md5.h"
#include "Common/StringExt.h"
#include "Common/System.h"

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
#include "MaintenanceQueue.h"
#include "RangeServer.h"
#include "ScanContext.h"
#include "MaintenanceTaskCompaction.h"
#include "MaintenanceTaskLogCleanup.h"
#include "MaintenanceTaskSplit.h"

using namespace std;
using namespace Hypertable;
using namespace Serialization;

namespace {
  const int DEFAULT_PORT    = 38060;
}



/**
 * Constructor
 */
RangeServer::RangeServer(PropertiesPtr &props_ptr, ConnectionManagerPtr &conn_manager_ptr, ApplicationQueuePtr &app_queue_ptr, Hyperspace::SessionPtr &hyperspace_ptr) : m_root_replay_finished(false), m_metadata_replay_finished(false), m_replay_finished(false), m_props_ptr(props_ptr), m_verbose(false), m_conn_manager_ptr(conn_manager_ptr), m_app_queue_ptr(app_queue_ptr), m_hyperspace_ptr(hyperspace_ptr), m_last_commit_log_clean(0), m_bytes_loaded(0) {
  uint16_t port;
  uint32_t maintenance_threads = 1;
  Comm *comm = conn_manager_ptr->get_comm();

  Global::range_max_bytes           = props_ptr->get_int64("Hypertable.RangeServer.Range.MaxBytes", 200000000LL);
  Global::access_group_max_files   = props_ptr->get_int("Hypertable.RangeServer.AccessGroup.MaxFiles", 10);
  Global::access_group_merge_files = props_ptr->get_int("Hypertable.RangeServer.AccessGroup.MergeFiles", 4);
  Global::access_group_max_mem  = props_ptr->get_int("Hypertable.RangeServer.AccessGroup.MaxMemory", 50000000);
  maintenance_threads             = props_ptr->get_int("Hypertable.RangeServer.MaintenanceThreads", 1);
  port                            = props_ptr->get_int("Hypertable.RangeServer.Port", DEFAULT_PORT);
  m_scanner_ttl                   = (time_t)props_ptr->get_int("Hypertable.RangeServer.Scanner.Ttl", 120);
  m_timer_interval                = props_ptr->get_int("Hypertable.RangeServer.Timer.Interval", 60);
  m_log_roll_limit                = props_ptr->get_int64("Hypertable.RangeServer.CommitLog.RollLimit", HYPERTABLE_RANGESERVER_COMMITLOG_ROLLLIMIT);

  if (m_timer_interval >= 1000) {
    HT_ERROR("Hypertable.RangeServer.Timer.Interval property too large, exiting ...");
    exit(1);
  }

  if (m_scanner_ttl < (time_t)10) {
    HT_WARNF("Value %u for Hypertable.RangeServer.Scanner.ttl is too small, setting to 10", (unsigned int)m_scanner_ttl);
    m_scanner_ttl = (time_t)10;
  }

  uint64_t block_cacheMemory = props_ptr->get_int64("Hypertable.RangeServer.BlockCache.MaxMemory", 200000000LL);
  Global::block_cache = new FileBlockCache(block_cacheMemory);

  assert(Global::access_group_merge_files <= Global::access_group_max_files);

  m_verbose = props_ptr->get_bool("Hypertable.Verbose", false);

  if (Global::verbose) {
    cout << "Hypertable.RangeServer.AccessGroup.MaxFiles=" << Global::access_group_max_files << endl;
    cout << "Hypertable.RangeServer.AccessGroup.MaxMemory=" << Global::access_group_max_mem << endl;
    cout << "Hypertable.RangeServer.AccessGroup.MergeFiles=" << Global::access_group_merge_files << endl;
    cout << "Hypertable.RangeServer.BlockCache.MaxMemory=" << block_cacheMemory << endl;
    cout << "Hypertable.RangeServer.Range.MaxBytes=" << Global::range_max_bytes << endl;
    cout << "Hypertable.RangeServer.MaintenanceThreads=" << maintenance_threads << endl;
    cout << "Hypertable.RangeServer.Port=" << port << endl;
    //cout << "Hypertable.RangeServer.workers=" << worker_count << endl;
  }

  //m_conn_manager_ptr = new ConnectionManager(comm);
  //m_app_queue_ptr = new ApplicationQueue(worker_count);

  Global::protocol = new Hypertable::RangeServerProtocol();

  DfsBroker::Client *dfs_client = new DfsBroker::Client(m_conn_manager_ptr, props_ptr);

  if (m_verbose) {
    cout << "DfsBroker.Host=" << props_ptr->get("DfsBroker.Host", "") << endl;
    cout << "DfsBroker.Port=" << props_ptr->get("DfsBroker.Port", "") << endl;
    cout << "DfsBroker.Timeout=" << props_ptr->get("DfsBroker.Timeout", "") << endl;
  }

  if (!dfs_client->wait_for_connection(30)) {
    HT_ERROR("Unable to connect to DFS Broker, exiting...");
    exit(1);
  }

  Global::dfs = dfs_client;

  /**
   * Check for and connect to commit log DFS broker
   */
  {
    const char *loghost = props_ptr->get("Hypertable.RangeServer.CommitLog.DfsBroker.Host", 0);
    uint16_t logport    = props_ptr->get_int("Hypertable.RangeServer.CommitLog.DfsBroker.Port", 0);
    struct sockaddr_in addr;
    if (loghost != 0) {
      InetAddr::initialize(&addr, loghost, logport);
      dfs_client = new DfsBroker::Client(m_conn_manager_ptr, addr, 600);
      if (!dfs_client->wait_for_connection(30)) {
        HT_ERROR("Unable to connect to DFS Broker, exiting...");
        exit(1);
      }
      Global::log_dfs = dfs_client;
    }
    else {
      Global::log_dfs = Global::dfs;
    }
  }

  /**
   *
   */
  {
    String host_str;
    struct sockaddr_in addr;

    InetAddr::get_hostname(host_str);
    if (!InetAddr::initialize(&addr, host_str.c_str(), port)) {
      if (!InetAddr::initialize(&addr, "localhost", port))
	exit(1);
    }

    m_location = (String)inet_ntoa(addr.sin_addr) + "_" + (int)port;
  }

  // Create the maintenance queue
  Global::maintenance_queue = new MaintenanceQueue(maintenance_threads);

  // Create table info maps
  m_live_map_ptr = new TableInfoMap();
  m_replay_map_ptr = new TableInfoMap();

  initialize(props_ptr);

  /**
   * Listen for incoming connections
   */
  {
    ConnectionHandlerFactoryPtr chfp(new HandlerFactory(comm, m_app_queue_ptr, this));
    struct sockaddr_in addr;
    InetAddr::initialize(&addr, INADDR_ANY, port);  // Listen on any interface
    comm->listen(addr, chfp);
  }

  /**
   * Create Master client
   */
  m_master_client_ptr = new MasterClient(m_conn_manager_ptr, m_hyperspace_ptr, 300, m_app_queue_ptr);
  m_master_connection_handler = new ConnectionHandler(comm, m_app_queue_ptr, this, m_master_client_ptr);
  m_master_client_ptr->initiate_connection(m_master_connection_handler);

  // Halt maintenance queue processing during recovery
  Global::maintenance_queue->stop();

  local_recover();

  Global::maintenance_queue->start();

  Global::log_prune_threshold_min = props_ptr->get_int64("Hypertable.RangeServer.CommitLog.PruneThreshold.Min",
                                                         2 * Global::user_log->get_max_fragment_size());
  Global::log_prune_threshold_max = props_ptr->get_int64("Hypertable.RangeServer.CommitLog.PruneThreshold.Max",
                                                         10 * Global::log_prune_threshold_min);

}


/**
 *
 */
RangeServer::~RangeServer() {
  delete Global::block_cache;
  delete Global::protocol;
  m_hyperspace_ptr = 0;
  delete Global::dfs;
  if (Global::log_dfs != Global::dfs)
    delete Global::log_dfs;
  Global::metadata_table_ptr = 0;
  m_master_client_ptr = 0;
  m_conn_manager_ptr = 0;
  m_app_queue_ptr = 0;
}



/**
 * - Figure out and create range server directory
 * - Clear any Range server state (including any partially created commit logs)
 * - Open the commit log
 */
void RangeServer::initialize(PropertiesPtr &props_ptr) {
  String top_dir;

  if (!m_hyperspace_ptr->exists("/hypertable/servers")) {
    if (!m_hyperspace_ptr->exists("/hypertable"))
      m_hyperspace_ptr->mkdir("/hypertable");
    m_hyperspace_ptr->mkdir("/hypertable/servers");
  }

  top_dir = (String)"/hypertable/servers/" + m_location;

  /**
   * Create "server existence" file in Hyperspace and obtain an exclusive lock on it
   */

  uint32_t lock_status;
  uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_CREATE | OPEN_FLAG_CREATE | OPEN_FLAG_LOCK;
  HandleCallbackPtr null_callback;

  m_existence_file_handle = m_hyperspace_ptr->open(top_dir.c_str(), oflags, null_callback);

  while (true) {

    lock_status = 0;

    m_hyperspace_ptr->try_lock(m_existence_file_handle, LOCK_MODE_EXCLUSIVE, &lock_status, &m_existence_file_sequencer);

    if (lock_status == LOCK_STATUS_GRANTED)
      break;

    cout << "Waiting for exclusive lock on hyperspace:/" << top_dir << " ..." << endl;
    poll(0, 0, 5000);
  }

  Global::log_dir = top_dir + "/log";

  /**
   * Create log directories
   */
  std::string path;
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

  if (Global::verbose)
    cout << "log_dir=" << Global::log_dir << endl;

}



/**
 */
void RangeServer::local_recover() {
  String meta_log_fname = Global::log_dir + "/range_txn/0.log";
  RangeServerMetaLogReaderPtr rsml_reader;
  CommitLogReaderPtr root_log_reader_ptr;
  CommitLogReaderPtr metadata_log_reader_ptr;
  CommitLogReaderPtr user_log_reader_ptr;

  try {

    /**
     * Check for existence of /hypertable/servers/X.X.X.X_port/log/range_txn/0.log file
     */
    if ( Global::log_dfs->exists(meta_log_fname) ) {

      // Load range states
      rsml_reader = new RangeServerMetaLogReader(Global::log_dfs, meta_log_fname);
      const RangeStates &range_states = rsml_reader->load_range_states();

      /**
       * First ROOT metadata range
       */
      m_replay_group = RangeServerProtocol::GROUP_METADATA_ROOT;

      // clear the replay map
      m_replay_map_ptr->clear();

      foreach(const RangeStateInfo *i, range_states) {
	if (i->table.id == 0 && i->range.end_row && !strcmp(i->range.end_row, Key::END_ROOT_ROW)) {
	  HT_EXPECT(i->transactions.empty(), Error::FAILED_EXPECTATION);
	  replay_load_range(0, &i->table, &i->range, &i->range_state);
	}
      }

      if (!m_replay_map_ptr->empty()) {
	root_log_reader_ptr = new CommitLogReader(Global::log_dfs, Global::log_dir + "/root");
	replay_log(root_log_reader_ptr);
	m_live_map_ptr->merge(m_replay_map_ptr);	
      }

      // Create root log and wake up anybody waiting for root replay to complete
      {
	boost::mutex::scoped_lock lock(m_mutex);
	if (root_log_reader_ptr)
	  Global::root_log = new CommitLog(Global::log_dfs, Global::log_dir + "/root", m_props_ptr, root_log_reader_ptr.get());	  
	m_root_replay_finished = true;
	m_root_replay_finished_cond.notify_all();
      }

      /**
       * Then recover other metadata ranges
       */
      m_replay_group = RangeServerProtocol::GROUP_METADATA;

      // clear the replay map
      m_replay_map_ptr->clear();

      foreach(const RangeStateInfo *i, range_states) {
	if (i->table.id == 0 && !(i->range.end_row && !strcmp(i->range.end_row, Key::END_ROOT_ROW)))
	  replay_load_range(0, &i->table, &i->range, &i->range_state);
      }

      if (!m_replay_map_ptr->empty()) {
	metadata_log_reader_ptr = new CommitLogReader(Global::log_dfs, Global::log_dir + "/metadata");
	replay_log(metadata_log_reader_ptr);
	m_live_map_ptr->merge(m_replay_map_ptr);
      }

      // Create metadata log and wake up anybody waiting for metadata replay to complete
      {
	boost::mutex::scoped_lock lock(m_mutex);
	if (metadata_log_reader_ptr)
	  Global::metadata_log = new CommitLog(Global::log_dfs, Global::log_dir + "/metadata", m_props_ptr, metadata_log_reader_ptr.get());
	m_metadata_replay_finished = true;
	m_metadata_replay_finished_cond.notify_all();
      }

      /**
       * Then recover the normal ranges
       */
      m_replay_group = RangeServerProtocol::GROUP_USER;

      // clear the replay map
      m_replay_map_ptr->clear();

      foreach(const RangeStateInfo *i, range_states) {
	if (i->table.id != 0)
	  replay_load_range(0, &i->table, &i->range, &i->range_state);
      }

      if (!m_replay_map_ptr->empty()) {
	user_log_reader_ptr = new CommitLogReader(Global::log_dfs, Global::log_dir + "/user");
	replay_log(user_log_reader_ptr);
	m_live_map_ptr->merge(m_replay_map_ptr);
      }


      // Create user log and range txn log and
      // wake up anybody waiting for replay to complete
      {
	boost::mutex::scoped_lock lock(m_mutex);
	Global::user_log = new CommitLog(Global::log_dfs, Global::log_dir + "/user", m_props_ptr, user_log_reader_ptr.get());
	Global::range_log = new RangeServerMetaLog(Global::log_dfs, meta_log_fname);
	m_replay_finished = true;
	m_replay_finished_cond.notify_all();
      }

    }
    else {
      boost::mutex::scoped_lock lock(m_mutex);

      /**
       *  Create the logs
       */

      if (root_log_reader_ptr)
	Global::root_log = new CommitLog(Global::log_dfs, Global::log_dir + "/root", m_props_ptr, root_log_reader_ptr.get());

      if (metadata_log_reader_ptr)
	Global::metadata_log = new CommitLog(Global::log_dfs, Global::log_dir + "/metadata", m_props_ptr, metadata_log_reader_ptr.get());

      Global::user_log = new CommitLog(Global::log_dfs, Global::log_dir + "/user", m_props_ptr, user_log_reader_ptr.get());

      Global::range_log = new RangeServerMetaLog(Global::log_dfs, meta_log_fname);

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



void RangeServer::replay_log(CommitLogReaderPtr &log_reader_ptr) {
  BlockCompressionHeaderCommitLog header;
  uint8_t *base;
  size_t len;
  TableIdentifier table_id;
  DynamicBuffer dbuf;
  const uint8_t *ptr, *end;
  int64_t revision;
  TableInfoPtr table_info_ptr;
  RangePtr range_ptr;
  SerializedKey key;
  ByteString value;

  while (log_reader_ptr->next((const uint8_t **)&base, &len, &header)) {

    revision = header.get_revision();

    ptr = base;
    end = base + len;

    table_id.decode(&ptr, &len);

    // Fetch table info
    if (!m_replay_map_ptr->get(table_id.id, table_info_ptr))
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
      if (!table_info_ptr->find_containing_range(key.row(), range_ptr))
	continue;

      // add key/value pair to buffer
      memcpy(dbuf.ptr, key.ptr, ptr-key.ptr);
      dbuf.ptr += ptr-key.ptr;

    }

    uint32_t block_size = dbuf.ptr - base;
    base = dbuf.base;
    encode_i32(&base, block_size);

    replay_update(0, dbuf.base, dbuf.fill());
  }
}



/**
 */
void RangeServer::compact(ResponseCallback *cb, TableIdentifier *table, RangeSpec *range, uint8_t compaction_type) {
  int error = Error::OK;
  String errmsg;
  TableInfoPtr table_info;
  RangePtr range_ptr;
  bool major = false;

  // Check for major compaction
  if (compaction_type == 1)
    major = true;

  if (Global::verbose) {
    cout << *table;
    cout << *range;
    cout << "Compaction type = " << (major ? "major" : "minor") << endl;
  }

  if (!m_replay_finished)
    wait_for_recovery_finish();

  /**
   * Fetch table info
   */
  if (!m_live_map_ptr->get(table->id, table_info)) {
    error = Error::RANGESERVER_RANGE_NOT_FOUND;
    errmsg = "No ranges loaded for table '" + (String)table->name + "'";
    goto abort;
  }

  /**
   * Fetch range info
   */
  if (!table_info->get_range(range, range_ptr)) {
    error = Error::RANGESERVER_RANGE_NOT_FOUND;
    errmsg = (String)table->name + "[" + range->start_row + ".." + range->end_row + "]";
    goto abort;
  }

  // schedule the compaction
  if (!range_ptr->test_and_set_maintenance())
    Global::maintenance_queue->add(new MaintenanceTaskCompaction(range_ptr, major));

  if ((error = cb->response_ok()) != Error::OK) {
    HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
  }

  if (Global::verbose) {
    HT_INFOF("Compaction (%s) scheduled for table '%s' end row '%s'",
                (major ? "major" : "minor"), table->name, range->end_row);
  }

  error = Error::OK;

 abort:
  if (error != Error::OK) {
    HT_ERRORF("%s '%s'", Error::get_text(error), errmsg.c_str());
    if ((error = cb->error(error, errmsg)) != Error::OK) {
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
    }
  }
}



/**
 * CreateScanner
 */
void RangeServer::create_scanner(ResponseCallbackCreateScanner *cb, TableIdentifier *table, RangeSpec *range, ScanSpec *scan_spec) {
  int error = Error::OK;
  String errmsg;
  TableInfoPtr table_info;
  RangePtr range_ptr;
  CellListScannerPtr scanner_ptr;
  bool more = true;
  uint32_t id;
  SchemaPtr schema_ptr;
  ScanContextPtr scan_ctx;

  if (Global::verbose) {
    cout << "RangeServer::create_scanner" << endl;
    cout << *table;
    cout << *range;
    cout << *scan_spec;
  }

  if (!m_replay_finished)
    wait_for_recovery_finish(table, range);

  try {
    DynamicBuffer rbuf;

    if (scan_spec->row_intervals.size() > 0) {
      if (scan_spec->row_intervals.size() > 1)
	HT_THROW(Error::RANGESERVER_BAD_SCAN_SPEC, "can only scan one row interval");
      if (scan_spec->cell_intervals.size() > 0)
	HT_THROW(Error::RANGESERVER_BAD_SCAN_SPEC, "both row and cell intervals defined");
    }

    if (scan_spec->cell_intervals.size() > 1)
      HT_THROW(Error::RANGESERVER_BAD_SCAN_SPEC, "can only scan one cell interval");

    if (!m_live_map_ptr->get(table->id, table_info))
      throw Hypertable::Exception(Error::RANGESERVER_RANGE_NOT_FOUND, (String)"unknown table '" + table->name + "'");

    if (!table_info->get_range(range, range_ptr))
      throw Hypertable::Exception(Error::RANGESERVER_RANGE_NOT_FOUND,
                                  (String)"(a) " + table->name + "[" + range->start_row + ".." + range->end_row + "]");

    schema_ptr = table_info->get_schema();

    scan_ctx = new ScanContext( range_ptr->get_scan_revision() , scan_spec, range, schema_ptr);

    scanner_ptr = range_ptr->create_scanner(scan_ctx);

    // TODO: fix this kludge (0 return above means range split)
    if (!scanner_ptr)
      throw Hypertable::Exception(Error::RANGESERVER_RANGE_NOT_FOUND,
                                  (String)"(b) " + table->name + "[" + range->start_row + ".." + range->end_row + "]");

    size_t count;
    more = FillScanBlock(scanner_ptr, rbuf, &count);

    id = (more) ? Global::scanner_map.put(scanner_ptr, range_ptr) : 0;

    if (Global::verbose) {
      HT_INFOF("Successfully created scanner (id=%d) on table '%s', returning %d k/v pairs", id, table->name, count);
    }

    /**
     *  Send back data
     */
    {
      short moreflag = more ? 0 : 1;
      StaticBuffer ext(rbuf);
      if ((error = cb->response(moreflag, id, ext)) != Error::OK) {
        HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
      }
    }
  }
  catch (Hypertable::Exception &e) {
    int error;
    HT_ERRORF("%s '%s'", Error::get_text(e.code()), e.what());
    if ((error = cb->error(e.code(), e.what())) != Error::OK) {
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
    }
  }
}


void RangeServer::destroy_scanner(ResponseCallback *cb, uint32_t scanner_id) {
  HT_INFOF("destroying scanner id=%u", scanner_id);
  Global::scanner_map.remove(scanner_id);
  cb->response_ok();
}


void RangeServer::fetch_scanblock(ResponseCallbackFetchScanblock *cb, uint32_t scanner_id) {
  string errmsg;
  int error = Error::OK;
  CellListScannerPtr scanner_ptr;
  RangePtr range_ptr;
  bool more = true;
  DynamicBuffer rbuf;

  if (Global::verbose) {
    cout << "RangeServer::fetch_scanblock" << endl;
    cout << "Scanner ID = " << scanner_id << endl;
  }

  if (!Global::scanner_map.get(scanner_id, scanner_ptr, range_ptr)) {
    error = Error::RANGESERVER_INVALID_SCANNER_ID;
    char tbuf[32];
    sprintf(tbuf, "%d", scanner_id);
    errmsg = (string)tbuf;
    goto abort;
  }

  size_t count;
  more = FillScanBlock(scanner_ptr, rbuf, &count);

  if (!more)
    Global::scanner_map.remove(scanner_id);

  /**
   *  Send back data
   */
  {
    short moreflag = more ? 0 : 1;
    StaticBuffer ext(rbuf);

    if ((error = cb->response(moreflag, scanner_id, ext)) != Error::OK) {
      HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
    }

    if (Global::verbose) {
      HT_INFOF("Successfully fetched %d bytes (%d k/v pairs) of scan data", ext.size-4, count);
    }
  }

  error = Error::OK;

 abort:
  if (error != Error::OK) {
    HT_ERRORF("%s '%s'", Error::get_text(error), errmsg.c_str());
    if ((error = cb->error(error, errmsg)) != Error::OK) {
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
    }
  }
}


/**
 * LoadRange
 */
void RangeServer::load_range(ResponseCallback *cb, const TableIdentifier *table, const RangeSpec *range, const char *transfer_log_dir, const RangeState *range_state) {
  String errmsg;
  int error = Error::OK;
  SchemaPtr schema_ptr;
  TableInfoPtr table_info_ptr;
  RangePtr range_ptr;
  string table_dfsdir;
  string range_dfsdir;
  char md5DigestStr[33];
  bool register_table = false;
  bool is_root = table->id == 0 && (*range->start_row == 0) && !strcmp(range->end_row, Key::END_ROOT_ROW);
  TableScannerPtr scanner_ptr;
  TableMutatorPtr mutator_ptr;
  KeySpec key;
  String metadata_key_str;

  if (Global::verbose)
    cout << "load_range " << *table << " " << *range << endl;

  if (!m_replay_finished)
    wait_for_recovery_finish();

  try {

    /** Get TableInfo, create if doesn't exist **/
    if (!m_live_map_ptr->get(table->id, table_info_ptr)) {
      table_info_ptr = new TableInfo(m_master_client_ptr, table, schema_ptr);
      register_table = true;
    }

    // Verify schema, this will create the Schema object and add it to
    // table_info_ptr if it doesn't exist
    verify_schema(table_info_ptr, table->generation);

    if (register_table)
      m_live_map_ptr->set(table->id, table_info_ptr);

    /**
     * Make sure this range is not already loaded
     */
    if (table_info_ptr->get_range(range, range_ptr))
      HT_THROW(Error::RANGESERVER_RANGE_ALREADY_LOADED, (String)table->name + "[" + range->start_row + ".." + range->end_row + "]");

    /**
     * Lazily create METADATA table pointer
     */
    if (!Global::metadata_table_ptr) {
      boost::mutex::scoped_lock lock(m_mutex);
      Global::metadata_table_ptr = new Table(m_props_ptr, m_conn_manager_ptr, Global::hyperspace_ptr, "METADATA");
    }

    schema_ptr = table_info_ptr->get_schema();

    /**
     * Take ownership of the range by writing the 'Location' column in the
     * METADATA table, or /hypertable/root{location} attribute of Hyperspace
     * if it is the root range.
     */
    if (!is_root) {

      metadata_key_str = String("") + (uint32_t)table->id + ":" + range->end_row;

      /**
       * Take ownership of the range
       */
      mutator_ptr = Global::metadata_table_ptr->create_mutator();

      key.row = metadata_key_str.c_str();
      key.row_len = strlen(metadata_key_str.c_str());
      key.column_family = "Location";
      key.column_qualifier = 0;
      key.column_qualifier_len = 0;
      mutator_ptr->set(key, (uint8_t *)m_location.c_str(), strlen(m_location.c_str()));
      mutator_ptr->flush();

    }
    else {  //root
      uint64_t handle;
      HandleCallbackPtr null_callback;
      uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_CREATE;

      HT_INFO("Loading root METADATA range");

      try {
	handle = m_hyperspace_ptr->open("/hypertable/root", oflags, null_callback);
	m_hyperspace_ptr->attr_set(handle, "Location", m_location.c_str(), strlen(m_location.c_str()));
	m_hyperspace_ptr->close(handle);
      }
      catch (Exception &e) {
	HT_ERROR_OUT << "Problem setting attribute 'location' on Hyperspace file '/hypertable/root'" << HT_END;
	HT_ERROR_OUT << e << HT_END;
	HT_ABORT;
      }

    }

    /**
     * Check for existence of and create, if necessary, range directory (md5 of endrow)
     * under all locality group directories for this table
     */
    {
      assert(*range->end_row != 0);
      md5_string(range->end_row, md5DigestStr);
      md5DigestStr[24] = 0;
      table_dfsdir = (string)"/hypertable/tables/" + (string)table->name;
      list<Schema::AccessGroup *> *aglist = schema_ptr->get_access_group_list();
      for (list<Schema::AccessGroup *>::iterator ag_it = aglist->begin(); ag_it != aglist->end(); ag_it++) {
	// notice the below variables are different "range" vs. "table"
	range_dfsdir = table_dfsdir + "/" + (*ag_it)->name + "/" + md5DigestStr;
	Global::dfs->mkdirs(range_dfsdir);
      }
    }

    range_ptr = new Range(m_master_client_ptr, table, schema_ptr, range, range_state);

    /**
     * Create root and/or metadata log if necessary
     */
    if (table->id == 0) {
      if (is_root) {
	Global::log_dfs->mkdirs(Global::log_dir + "/root");
	Global::root_log = new CommitLog(Global::log_dfs, Global::log_dir + "/root", m_props_ptr);
      }
      else if (Global::metadata_log == 0) {
	Global::log_dfs->mkdirs(Global::log_dir + "/metadata");
	Global::metadata_log = new CommitLog(Global::log_dfs, Global::log_dir + "/metadata", m_props_ptr);
      }
    }

    /**
     * NOTE: The range does not need to be locked in the following replay since
     * it has not been added yet and therefore no one else can find it and
     * concurrently access it.
     */
    if (transfer_log_dir && *transfer_log_dir) {
      CommitLogReaderPtr commit_log_reader_ptr = new CommitLogReader(Global::dfs, transfer_log_dir);
      CommitLog *log;
      if (is_root)
	log = Global::root_log;
      else if (table->id == 0)
	log = Global::metadata_log;
      else
	log = Global::user_log;
      
      range_ptr->replay_transfer_log(commit_log_reader_ptr.get());
      if ((error = log->link_log(commit_log_reader_ptr.get())) != Error::OK)
	HT_THROW(error, (String)"Unable to link transfer log (" + transfer_log_dir + ") into commit log (" + log->get_log_dir().c_str() + ")");
    }

    table_info_ptr->add_range(range_ptr);

    if (Global::range_log)
      Global::range_log->log_range_loaded(*table, *range, *range_state);

    if (cb && (error = cb->response_ok()) != Error::OK) {
      HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
    }
    else {
      HT_INFOF("Successfully loaded range %s[%s..%s]", table->name, range->start_row, range->end_row);
    }

  }
  catch (Hypertable::Exception &e) {
    HT_ERRORF("%s '%s'", Error::get_text(e.code()), e.what());
    if (cb && (error = cb->error(e.code(), e.what())) != Error::OK) {
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
    }
  }
}



void RangeServer::transform_key(ByteString &bskey, DynamicBuffer *dest_bufp, int64_t auto_revision, int64_t *revisionp) {
  size_t len;
  const uint8_t *ptr;

  len = bskey.decode_length(&ptr);

  if (*ptr == Key::CONTROL_MASK_AUTO_TIMESTAMP) {
    dest_bufp->ensure( (ptr-bskey.ptr) + len + 9 );
    Serialization::encode_vi32(&dest_bufp->ptr, len+8);
    memcpy(dest_bufp->ptr, ptr, len);
    *dest_bufp->ptr = Key::CONTROL_MASK_HAVE_REVISION | Key::CONTROL_MASK_HAVE_TIMESTAMP | Key::CONTROL_MASK_SHARED;
    dest_bufp->ptr += len;
    Key::encode_ts64(&dest_bufp->ptr, auto_revision);
    *revisionp = auto_revision;
    bskey.ptr = ptr + len;
  }
  else if (*ptr == Key::CONTROL_MASK_HAVE_TIMESTAMP) {
    dest_bufp->ensure( (ptr-bskey.ptr) + len + 9 );
    Serialization::encode_vi32(&dest_bufp->ptr, len+8);
    memcpy(dest_bufp->ptr, ptr, len);
    *dest_bufp->ptr = Key::CONTROL_MASK_HAVE_REVISION | Key::CONTROL_MASK_HAVE_TIMESTAMP;
    dest_bufp->ptr += len;
    Key::encode_ts64(&dest_bufp->ptr, auto_revision);
    *revisionp = auto_revision;
    bskey.ptr = ptr + len;
  }
  else {
    HT_EXPECT(false, Error::FAILED_EXPECTATION);
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
    RangePtr range_ptr;
    DynamicBuffer *bufp;
    uint64_t offset;
    uint64_t len;
  };

}



/**
 * Update
 */
void RangeServer::update(ResponseCallbackUpdate *cb, TableIdentifier *table, uint32_t count, StaticBuffer &buffer) {
  const uint8_t *mod_ptr, *mod_end;
  string errmsg;
  int error = Error::OK;
  TableInfoPtr table_info_ptr;
  int64_t auto_revision = Global::user_log->get_timestamp();
  int64_t last_revision;
  const char *row;
  String split_row;
  CommitLogPtr splitlog;
  String end_row;
  bool split_pending;
  SerializedKey key;
  ByteString value;
  bool a_locked = false;
  bool b_locked = false;
  vector<SendBackRec> send_back_vector;
  const uint8_t *send_back_ptr = 0;
  uint32_t total_added = 0;
  uint32_t split_added = 0;
  std::vector<RangeUpdateInfo> range_vector;
  DynamicBuffer root_buf;
  DynamicBuffer go_buf;
  DynamicBuffer split_buf;
  std::vector<DynamicBuffer> split_bufs;
  DynamicBuffer *cur_bufp;
  size_t send_back_count = 0;
  uint32_t misses = 0;
  RangeUpdateInfo rui;

  // Pre-allocate the go_buf - each key could expand by 8 or 9 bytes,
  // if auto-assigned (8 for the ts or rev and maybe 1 for possible
  // increase in vint length)
  const uint32_t encoded_table_len = table->encoded_length();
  go_buf.reserve(encoded_table_len + buffer.size + (count * 9));
  table->encode(&go_buf.ptr);

  if (Global::verbose) {
    cout << "RangeServer::update" << endl;
    cout << *table;
  }

  if (!m_replay_finished)
    wait_for_recovery_finish();

  // TODO: Sanity check mod data (checksum validation)

  try {

    // Fetch table info
    if (!m_live_map_ptr->get(table->id, table_info_ptr)) {
      StaticBuffer ext(new uint8_t [16], 16);
      uint8_t *ptr = ext.base;
      encode_i32(&ptr, Error::RANGESERVER_TABLE_NOT_FOUND);
      encode_i32(&ptr, count);
      encode_i32(&ptr, 0);
      encode_i32(&ptr, buffer.size);
      HT_ERRORF("Unable to find table info for table '%s'", table->name);
      if ((error = cb->response(ext)) != Error::OK)
        HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
      return;
    }

    // verify schema
    verify_schema(table_info_ptr, table->generation);

    mod_end = buffer.base + buffer.size;
    mod_ptr = buffer.base;

    m_update_mutex_a.lock();
    a_locked = true;

    send_back_ptr = 0;
    send_back_count = 0;

    while (mod_ptr < mod_end) {

      key.ptr = mod_ptr;

      row = key.row();

      // If the row key starts with '\0' then the buffer is probably
      // corrupt, so mark the remaing key/value pairs as bad
      if (*row == 0) {
        SendBackRec send_back;
        send_back.error = Error::BAD_KEY;
        send_back.count = count;  // fix me !!!!
        send_back.offset = mod_ptr - buffer.base;
        send_back.len = mod_end - mod_ptr;
        send_back_vector.push_back(send_back);
	mod_ptr = mod_end;
	continue;
      }

      // Look for containing range, add to stop mods if not found
      if (!table_info_ptr->find_containing_range(row, rui.range_ptr)) {
        if (send_back_ptr == 0)
          send_back_ptr = mod_ptr;
        key.next(); // skip key
        key.next(); // skip value;
        mod_ptr = key.ptr;
        send_back_count++;
        continue;
      }

      if (send_back_ptr) {
        SendBackRec send_back;
        send_back.error = Error::RANGESERVER_OUT_OF_RANGE;
        send_back.count = send_back_count;
        send_back.offset = send_back_ptr - buffer.base;
        send_back.len = mod_ptr - send_back_ptr;
        send_back_vector.push_back(send_back);
        send_back_ptr = 0;
	send_back_count = 0;
      }

      /** Increment update count (block if maintenance in progress) **/
      rui.range_ptr->increment_update_counter();

      // Make sure range didn't just shrink
      if (strcmp(row, (rui.range_ptr->start_row()).c_str()) <= 0) {
        rui.range_ptr->decrement_update_counter();
        continue;
      }

      end_row = rui.range_ptr->end_row();

      /** Fetch range split information **/
      split_pending = rui.range_ptr->get_split_info(split_row, splitlog);

      if (split_pending)
	cur_bufp = &split_buf;
      else if (rui.range_ptr->is_root())
	cur_bufp = &root_buf;
      else
	cur_bufp = &go_buf;

      if (cur_bufp->ptr == 0) {
	cur_bufp->reserve(encoded_table_len);
	table->encode(&cur_bufp->ptr);
      }

      rui.bufp = cur_bufp;
      rui.offset = cur_bufp->fill();

      while (mod_ptr < mod_end && (end_row == "" || (strcmp(row, end_row.c_str()) <= 0))) {

	if (split_pending) {
	  if (strcmp(row, split_row.c_str()) > 0) {
	    rui.len = cur_bufp->fill() - rui.offset;
	    range_vector.push_back(rui);
	    cur_bufp = &go_buf;
	    // increment update count again (for the second half)
	    rui.range_ptr->increment_update_counter();
	    rui.bufp = cur_bufp;
	    rui.offset = cur_bufp->fill();
	    split_pending = false;
	  }
	  else
	    split_added++;
	}

	// This will transform keys that need to be assigned a
	// timestamp and/or revision number by re-writing the key
	// with the added timestamp and/or revision tacked on to the end
	transform_key(key, cur_bufp, ++auto_revision, &last_revision);

	// Now copy the value (with sanity check)
        mod_ptr = key.ptr;
        key.next(); // skip value
	HT_EXPECT(key.ptr <= mod_end, Error::FAILED_EXPECTATION);
	cur_bufp->add(mod_ptr, key.ptr-mod_ptr);
        mod_ptr = key.ptr;

	total_added++;

        if (mod_ptr < mod_end)
          row = key.row();
      }

      rui.len = cur_bufp->fill() - rui.offset;
      range_vector.push_back(rui);
      rui.range_ptr = 0;
      rui.bufp = 0;

      // if there were split-off updates, write the split log entry
      if (split_buf.fill() > encoded_table_len) {
	if ((error = splitlog->write(split_buf, last_revision)) != Error::OK)
	  HT_THROW(error, (string)"Problem writing " + (int)split_buf.fill() + " bytes to split log");
	splitlog = 0;
	split_bufs.push_back(split_buf);
	split_buf.own = false;
	split_buf.clear();
      }

    }

    if (Global::verbose)
      HT_INFOF("Added %d (%d split off) updates to '%s'", total_added, split_added, table->name);

    if (send_back_ptr) {
      SendBackRec send_back;
      send_back.error = Error::RANGESERVER_OUT_OF_RANGE;
      send_back.count = send_back_count;
      send_back.offset = send_back_ptr - buffer.base;
      send_back.len = mod_ptr - send_back_ptr;
      send_back_vector.push_back(send_back);
      send_back_ptr = 0;
      send_back_count = 0;
    }

    m_update_mutex_b.lock();
    b_locked = true;

    m_update_mutex_a.unlock();
    a_locked = false;

    /**
     * Commit ROOT mutations
     */
    if (root_buf.fill() > encoded_table_len) {
      if ((error = Global::root_log->write(root_buf, last_revision)) != Error::OK)
	HT_THROW(error, (string)"Problem writing " + (int)root_buf.fill() + " bytes to ROOT commit log");
    }

    /**
     * Commit valid (go) mutations
     */
    if (go_buf.fill() > encoded_table_len) {
      CommitLog *log = (table->id == 0) ? Global::metadata_log : Global::user_log;
      if ((error = log->write(go_buf, last_revision)) != Error::OK)
	HT_THROW(error, (string)"Problem writing " + (int)go_buf.fill() + " bytes to commit log (" + log->get_log_dir() + ")");
    }

    for (size_t rangei=0; rangei<range_vector.size(); rangei++) {

      /**
       * Apply the modifications
       */
      range_vector[rangei].range_ptr->lock();
      {
	Key key_comps;
        uint8_t *ptr = range_vector[rangei].bufp->base + range_vector[rangei].offset;
	uint8_t *end = ptr + range_vector[rangei].len;
        while (ptr < end) {
          key.ptr = ptr;
	  key_comps.load(key);
          ptr += key_comps.length;
          value.ptr = ptr;
          ptr += value.length();
	  error = range_vector[rangei].range_ptr->add(key_comps, value);
	  HT_EXPECT(error == Error::OK, Error::FAILED_EXPECTATION);
        }
      }
      range_vector[rangei].range_ptr->unlock();

      range_vector[rangei].range_ptr->decrement_update_counter();

      /**
       * Split and Compaction processing
       */
      if (!range_vector[rangei].range_ptr->maintenance_in_progress()) {
        std::vector<AccessGroup::CompactionPriorityData> priority_data_vec;
        std::vector<AccessGroup *> compactions;
        uint64_t disk_usage = 0;

        range_vector[rangei].range_ptr->get_compaction_priority_data(priority_data_vec);
        for (size_t i=0; i<priority_data_vec.size(); i++) {
          disk_usage += priority_data_vec[i].disk_used;
          if (!priority_data_vec[i].in_memory && priority_data_vec[i].mem_used >= (uint32_t)Global::access_group_max_mem)
            compactions.push_back(priority_data_vec[i].ag);
        }

        if (!range_vector[rangei].range_ptr->is_root() &&
            (disk_usage > range_vector[rangei].range_ptr->get_size_limit() ||
             (Global::range_metadata_max_bytes && table->id == 0 && disk_usage > Global::range_metadata_max_bytes))) {
          if (!range_vector[rangei].range_ptr->test_and_set_maintenance())
            Global::maintenance_queue->add(new MaintenanceTaskSplit(range_vector[rangei].range_ptr));
        }
        else if (!compactions.empty()) {
          if (!range_vector[rangei].range_ptr->test_and_set_maintenance()) {
            for (size_t i=0; i<compactions.size(); i++)
              compactions[i]->set_compaction_bit();
            Global::maintenance_queue->add(new MaintenanceTaskCompaction(range_vector[rangei].range_ptr, false));
          }
        }
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

  if (b_locked)
    m_update_mutex_b.unlock();
  else if (a_locked)
    m_update_mutex_a.unlock();

  m_bytes_loaded += buffer.size;

  if (error == Error::OK) {
    /**
     * Send back response
     */
    if (!send_back_vector.empty()) {
      StaticBuffer ext(new uint8_t [send_back_vector.size() * 16], send_back_vector.size() * 16);
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



void RangeServer::drop_table(ResponseCallback *cb, TableIdentifier *table) {
  TableInfoPtr table_info_ptr;
  std::vector<RangePtr> range_vector;
  String metadata_prefix;
  String metadata_key; // ***********  = String("") + (uint32_t)m_table_identifier.id + ":" + m_end_row;
  TableMutatorPtr mutator_ptr;
  KeySpec key;

  if (Global::verbose) {
    HT_INFOF("drop_table '%s'", table->name);
    cout << flush;
  }

  if (!m_replay_finished)
    wait_for_recovery_finish();

  // create METADATA table mutator for clearing 'Location' columns
  mutator_ptr = Global::metadata_table_ptr->create_mutator();

  // initialize key structure
  memset(&key, 0, sizeof(key));
  key.column_family = "Location";

  try {

     // For each range in dropped table, Set the 'drop' bit and clear
    // the 'Location' column of the corresponding METADATA entry
    if (m_live_map_ptr->remove(table->id, table_info_ptr)) {
      metadata_prefix = String("") + table_info_ptr->get_id() + ":";
      table_info_ptr->get_range_vector(range_vector);
      for (size_t i=0; i<range_vector.size(); i++) {
        range_vector[i]->drop();
        metadata_key = metadata_prefix + range_vector[i]->end_row();
        key.row = metadata_key.c_str();
        key.row_len = metadata_key.length();
        mutator_ptr->set(key, "!", 1);
      }
      range_vector.clear();
    }
    else {
      HT_ERRORF("drop_table '%s' id=%u - table not found", table->name, table->id);
    }
    mutator_ptr->flush();
  }
  catch (Hypertable::Exception &e) {
    HT_ERRORF("Problem clearing 'Location' columns of METADATA - %s", Error::get_text(e.code()));
    cb->error(e.code(), "Problem clearing 'Location' columns of METADATA");
    return;
  }

  // write range transaction entry
  if (Global::range_log)
    Global::range_log->log_drop_table(*table);

  if (Global::verbose) {
    HT_INFOF("Successfully dropped table '%s'", table->name);
    cout << flush;
  }

  cb->response_ok();
}


void RangeServer::dump_stats(ResponseCallback *cb) {
  std::vector<TableInfoPtr> table_vec;
  std::vector<RangePtr> range_vec;

  HT_INFO("dump_stats");

  m_live_map_ptr->get_all(table_vec);

  for (size_t i=0; i<table_vec.size(); i++) {
    range_vec.clear();
    table_vec[i]->get_range_vector(range_vec);
    for (size_t i=0; i<range_vec.size(); i++)
      range_vec[i]->dump_stats();
  }
  cb->response_ok();
}

void RangeServer::get_statistics(ResponseCallbackGetStatistics *cb) {
  std::vector<TableInfoPtr> table_vec;
  std::vector<RangePtr> range_vec;
  RangeServerStat stat;

  HT_INFO("get_statistics");

  m_live_map_ptr->get_all(table_vec);

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

/**
 *
 */
void RangeServer::replay_begin(ResponseCallback *cb, uint16_t group) {
  String replay_log_dir = format("/hypertable/servers/%s/log/replay",
                                 m_location.c_str());

  m_replay_group = group;

  if (Global::verbose) {
    HT_INFOF("replay_start group=%d", m_replay_group);
    cout << flush;
  }

  m_replay_log_ptr = 0;

  m_replay_map_ptr->clear_ranges();

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

  m_replay_log_ptr = new CommitLog(Global::log_dfs, replay_log_dir, m_props_ptr);

  cb->response_ok();
}

/**
 */
void RangeServer::replay_load_range(ResponseCallback *cb, const TableIdentifier *table, const RangeSpec *range, const RangeState *range_state) {
  int error = Error::OK;
  SchemaPtr schema_ptr;
  TableInfoPtr table_info_ptr;
  RangePtr range_ptr;
  bool register_table = false;

  if (Global::verbose) {
    cout << "replay_load_range" << endl;
    cout << *table;
    cout << *range;
    cout << flush;
  }

  try {

    /** Get TableInfo from replay map, or copy it from live map, or create if doesn't exist **/
    if (!m_replay_map_ptr->get(table->id, table_info_ptr)) {
      if (!m_live_map_ptr->get(table->id, table_info_ptr))
	table_info_ptr = new TableInfo(m_master_client_ptr, table, schema_ptr);
      else
	table_info_ptr = table_info_ptr->create_shallow_copy();
      register_table = true;
    }

    // Verify schema, this will create the Schema object and add it to
    // table_info_ptr if it doesn't exist
    verify_schema(table_info_ptr, table->generation);

    if (register_table)
      m_replay_map_ptr->set(table->id, table_info_ptr);

    /**
     * Make sure this range is not already loaded
     */
    if (table_info_ptr->get_range(range, range_ptr))
      HT_THROW(Error::RANGESERVER_RANGE_ALREADY_LOADED, (String)table->name + "[" + range->start_row + ".." + range->end_row + "]");

    /**
     * Lazily create METADATA table pointer
     */
    if (!Global::metadata_table_ptr) {
      boost::mutex::scoped_lock lock(m_mutex);
      Global::metadata_table_ptr = new Table(m_props_ptr, m_conn_manager_ptr, Global::hyperspace_ptr, "METADATA");
    }

    schema_ptr = table_info_ptr->get_schema();

    range_ptr = new Range(m_master_client_ptr, table, schema_ptr, range, range_state);

    table_info_ptr->add_range(range_ptr);

    if (Global::range_log)
      Global::range_log->log_range_loaded(*table, *range, *range_state);

    if (cb && (error = cb->response_ok()) != Error::OK) {
      HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
    }
    else {
      HT_INFOF("Successfully replay loaded range %s[%s..%s]", table->name, range->start_row, range->end_row);
    }

  }
  catch (Hypertable::Exception &e) {
    HT_ERRORF("%s '%s'", Error::get_text(error), e.what());
    if (cb && (error = cb->error(e.code(), e.what())) != Error::OK) {
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
    }
  }
}



void RangeServer::replay_update(ResponseCallback *cb, const uint8_t *data, size_t len) {
  TableIdentifier table_identifier;
  TableInfoPtr table_info_ptr;
  SerializedKey serkey;
  ByteString bsvalue;
  Key key;
  const uint8_t *ptr = data;
  const uint8_t *end_ptr = data + len;
  const uint8_t *block_end_ptr;
  uint32_t block_size;
  size_t remaining = len;
  const char *row;
  String err_msg;
  int64_t revision;
  RangePtr range_ptr;
  String end_row;
  int error;

  if (Global::verbose) {
    HT_INFOF("replay_update - length=%ld", len);
    cout << flush;
  }

  try {

    while (ptr < end_ptr) {

      // decode key/value block size + revision
      block_size = decode_i32(&ptr, &remaining);
      revision = decode_i64(&ptr, &remaining);

      if (m_replay_log_ptr) {
	DynamicBuffer dbuf(0, false);
	dbuf.base = (uint8_t *)ptr;
	dbuf.ptr = dbuf.base + remaining;
	if ((error = m_replay_log_ptr->write(dbuf, revision)) != Error::OK)
	  HT_THROW(error, "");
      }

      // decode table identifier
      table_identifier.decode(&ptr, &remaining);

      if (block_size > remaining)
        HT_THROWF(Error::MALFORMED_REQUEST, "Block (size=%lu) exceeds EOM",
                  (Lu)block_size);

      block_end_ptr = ptr + block_size;

      // Fetch table info
      if (!m_replay_map_ptr->get(table_identifier.id, table_info_ptr))
        HT_THROWF(Error::RANGESERVER_RANGE_NOT_FOUND, "Unable to find "
                  "table info for table name='%s' id=%lu",
                  table_identifier.name, (Lu)table_identifier.id);

      while (ptr < block_end_ptr) {

        row = SerializedKey(ptr).row();

        // Look for containing range, add to stop mods if not found
        if (!table_info_ptr->find_containing_range(row, range_ptr))
          HT_THROWF(Error::RANGESERVER_RANGE_NOT_FOUND, "Unable to find "
                    "range for row '%s'", row);

        end_row = range_ptr->end_row();

	serkey.ptr = ptr;
        while (ptr < block_end_ptr &&
               (end_row == "" || (strcmp(row, end_row.c_str()) <= 0))) {

          // extract the key
          ptr += serkey.length();

          if (ptr > end_ptr)
            HT_THROW(Error::REQUEST_TRUNCATED, "Problem decoding key");

          bsvalue.ptr = ptr;
          ptr += bsvalue.length();

          if (ptr > end_ptr)
            HT_THROW(Error::REQUEST_TRUNCATED, "Problem decoding value");

	  key.load(serkey);

	  range_ptr->lock();
          HT_EXPECT(range_ptr->add(key, bsvalue) == Error::OK, Error::FAILED_EXPECTATION);
	  range_ptr->unlock();

	  serkey.ptr = ptr;
	  if (ptr < block_end_ptr)
	    row = serkey.row();
        }
      }
    }

  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    if (cb) {
      cb->error(e.code(), format("%s - %s", e.what(), Error::get_text(e.code())));
      return;
    }
    HT_THROW(e.code(), e.what());
  }

  if (cb)
    cb->response_ok();
}


void RangeServer::replay_commit(ResponseCallback *cb) {
  int error;

  if (Global::verbose) {
    HT_INFO("replay_commit");
    cout << flush;
  }

  try {
    CommitLog *log = 0;
    if (m_replay_group == RangeServerProtocol::GROUP_METADATA_ROOT)
      log = Global::root_log;
    else if (m_replay_group == RangeServerProtocol::GROUP_METADATA)
      log = Global::metadata_log;
    else if (m_replay_group == RangeServerProtocol::GROUP_USER)
      log = Global::user_log;

    if ((error = log->link_log(m_replay_log_ptr.get())) != Error::OK)
      HT_THROW(error, std::string("Problem linking replay log (") + m_replay_log_ptr->get_log_dir() + ") into commit log (" + log->get_log_dir() + ")");

    m_live_map_ptr->merge(m_replay_map_ptr);

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



void RangeServer::drop_range(ResponseCallback *cb, TableIdentifier *table, RangeSpec *range) {
  TableInfoPtr table_info_ptr;
  RangePtr range_ptr;

  if (Global::verbose) {
    cout << "drop_range" << endl;
    cout << *table;
    cout << *range;
    cout << flush;
  }

  /** Get TableInfo **/
  if (!m_live_map_ptr->get(table->id, table_info_ptr)) {
    cb->error(Error::RANGESERVER_RANGE_NOT_FOUND, String("No ranges loaded for table '") + table->name + "'");
    return;
  }

  /** Remove the range **/
  if (!table_info_ptr->remove_range(range, range_ptr)) {
    cb->error(Error::RANGESERVER_RANGE_NOT_FOUND, (String)table->name + "[" + range->start_row + ".." + range->end_row + "]");
    return;
  }

  cb->response_ok();
}

void RangeServer::shutdown(ResponseCallback *cb) {
  std::vector<TableInfoPtr> table_vec;
  std::vector<RangePtr> range_vec;

  (void)cb;

  Global::maintenance_queue->stop();

  // block updates
  m_update_mutex_a.lock();
  m_update_mutex_b.lock();

  // get the tables
  m_live_map_ptr->get_all(table_vec);

  // add all ranges into the range vector
  for (size_t i=0; i<table_vec.size(); i++)
    table_vec[i]->get_range_vector(range_vec);

  // increment the update counters
  for (size_t i=0; i<range_vec.size(); i++)
    range_vec[i]->increment_update_counter();

  m_hyperspace_ptr = 0;

  if (Global::range_log)
    Global::range_log->close();

  if (Global::root_log)
    Global::root_log->close();

  if (Global::metadata_log)
    Global::metadata_log->close();

  if (Global::user_log)
    Global::user_log->close();

}



void RangeServer::verify_schema(TableInfoPtr &table_info_ptr, int generation) {
  String tablefile = (String)"/hypertable/tables/" + table_info_ptr->get_name();
  DynamicBuffer valbuf;
  HandleCallbackPtr null_handle_callback;
  uint64_t handle;
  SchemaPtr schema_ptr = table_info_ptr->get_schema();

  if (schema_ptr.get() == 0 || schema_ptr->get_generation() < generation) {

    handle = m_hyperspace_ptr->open(tablefile.c_str(), OPEN_FLAG_READ, null_handle_callback);

    m_hyperspace_ptr->attr_get(handle, "schema", valbuf);

    m_hyperspace_ptr->close(handle);

    schema_ptr = Schema::new_instance((const char *)valbuf.base, valbuf.fill(), true);
    if (!schema_ptr->is_valid())
      HT_THROW(Error::RANGESERVER_SCHEMA_PARSE_ERROR,
	       (String)"Schema Parse Error for table '" + table_info_ptr->get_name() + "' : " + schema_ptr->get_error_string());

    table_info_ptr->update_schema(schema_ptr);

    // Generation check ...
    if (schema_ptr->get_generation() < generation)
      HT_THROW(Error::RANGESERVER_GENERATION_MISMATCH, (String)"Fetched Schema generation for table '" + table_info_ptr->get_name() + "' is " + schema_ptr->get_generation() + " but supplied is " + generation);
  }

}



void RangeServer::do_maintenance() {
  struct timeval tval;

  /**
   * Purge expired scanners
   */
  Global::scanner_map.purge_expired(m_scanner_ttl);

  /**
   * Schedule log cleanup
   */
  gettimeofday(&tval, 0);
  if ((tval.tv_sec - m_last_commit_log_clean) >= (int)(m_timer_interval*4)/5) {
    // schedule log cleanup
    Global::maintenance_queue->add(new MaintenanceTaskLogCleanup(this));
    m_last_commit_log_clean = tval.tv_sec;
  }
}

namespace {

  struct LtPriorityData {
    bool operator()(const AccessGroup::CompactionPriorityData &pd1, const AccessGroup::CompactionPriorityData &pd2) const {
      return pd1.log_space_pinned >= pd2.log_space_pinned;
    }
  };


}


/**
 *
 */
void RangeServer::log_cleanup() {
  std::vector<TableInfoPtr> table_vec;
  std::vector<RangePtr> range_vec;
  uint64_t prune_threshold;
  size_t first_user_table = 0;

  if (!m_replay_finished)
    wait_for_recovery_finish();

  m_live_map_ptr->get_all(table_vec);

  if (table_vec.empty())
    return;

  /**
   * If we've got METADATA ranges, process them first
   */
  if (table_vec[0]->get_id() == 0 && Global::metadata_log) {
    first_user_table = 1;
    table_vec[0]->get_range_vector(range_vec);
    // skip root
    if (!range_vec.empty() && range_vec[0]->end_row() == Key::END_ROOT_ROW)
      range_vec.erase(range_vec.begin());
    schedule_log_cleanup_compactions(range_vec, Global::metadata_log, Global::log_prune_threshold_min);
  }

  range_vec.clear();
  for (size_t i=first_user_table; i<table_vec.size(); i++)
    table_vec[i]->get_range_vector(range_vec);

  // compute prune threshold
  prune_threshold = (uint64_t)((((double)m_bytes_loaded / (double)m_timer_interval) / 1000000.0) * (double)Global::log_prune_threshold_max);
  if (prune_threshold < Global::log_prune_threshold_min)
    prune_threshold = Global::log_prune_threshold_min;
  else if (prune_threshold > Global::log_prune_threshold_max)
    prune_threshold = Global::log_prune_threshold_max;

  HT_INFOF("Cleaning log (threshold=%lld)", prune_threshold);
  cout << flush;

  schedule_log_cleanup_compactions(range_vec, Global::user_log, prune_threshold);

  m_bytes_loaded = 0;
}


void RangeServer::schedule_log_cleanup_compactions(std::vector<RangePtr> &range_vec, CommitLog *log, uint64_t prune_threshold) {
  std::vector<AccessGroup::CompactionPriorityData> priority_data_vec;
  LogFragmentPriorityMap log_frag_map;
  int64_t revision, earliest_cached_revision = TIMESTAMP_NULL;

  // Load up a vector of compaction priority data
  for (size_t i=0; i<range_vec.size(); i++) {
    size_t start = priority_data_vec.size();
    range_vec[i]->get_compaction_priority_data(priority_data_vec);
    for (size_t j=start; j<priority_data_vec.size(); j++) {
      priority_data_vec[j].user_data = (void *)i;
      if ((revision = priority_data_vec[j].ag->get_earliest_cached_revision()) != TIMESTAMP_NULL) {
	if (earliest_cached_revision == TIMESTAMP_NULL || revision < earliest_cached_revision)
	  earliest_cached_revision = revision;
      }
    }
  }

  log->load_fragment_priority_map(log_frag_map);

  /**
   * Determine which AGs need compaction for the sake of
   * garbage collecting commit log fragments
   */
  for (size_t i=0; i<priority_data_vec.size(); i++) {

    if (priority_data_vec[i].earliest_cached_revision == TIMESTAMP_NULL)
      continue;

    LogFragmentPriorityMap::iterator map_iter = log_frag_map.lower_bound(priority_data_vec[i].earliest_cached_revision);
    
    // this should never happen
    if (map_iter == log_frag_map.end())
      continue;

    if ((*map_iter).second.cumulative_size > prune_threshold) {
      if (priority_data_vec[i].mem_used > 0)
        priority_data_vec[i].ag->set_compaction_bit();
      size_t rangei = (size_t)priority_data_vec[i].user_data;
      if (!range_vec[rangei]->test_and_set_maintenance()) {
        Global::maintenance_queue->add(new MaintenanceTaskCompaction(range_vec[rangei], false));
      }
    }
  }

  // Purge the commit log
  if (earliest_cached_revision != TIMESTAMP_NULL)
    log->purge(earliest_cached_revision);

}



/**
 */
uint64_t RangeServer::get_timer_interval() {
  return m_timer_interval*1000;
}


void RangeServer::wait_for_recovery_finish() {
  boost::mutex::scoped_lock lock(m_mutex);
  while (!m_replay_finished) {
    HT_INFO_OUT << "Waiting for recovery to complete..." << HT_END;
    m_replay_finished_cond.wait(lock);
  }
}


void RangeServer::wait_for_recovery_finish(TableIdentifier *table, RangeSpec *range) {
  boost::mutex::scoped_lock lock(m_mutex);
  if (table->id == 0) {
    if (!strcmp(range->end_row, Key::END_ROOT_ROW)) {
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
