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
#include "Hypertable/Lib/RangeServerMetaLogReader.h"
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
RangeServer::RangeServer(PropertiesPtr &props_ptr, ConnectionManagerPtr &conn_manager_ptr, ApplicationQueuePtr &app_queue_ptr, Hyperspace::SessionPtr &hyperspace_ptr) : m_props_ptr(props_ptr), m_verbose(false), m_conn_manager_ptr(conn_manager_ptr), m_app_queue_ptr(app_queue_ptr), m_hyperspace_ptr(hyperspace_ptr), m_last_commit_log_clean(0), m_bytes_loaded(0) {
  int error;
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
    if (!InetAddr::initialize(&addr, host_str.c_str(), port))
      exit(1);

    m_location = (String)inet_ntoa(addr.sin_addr) + "_" + (int)port;
  }

  // Create the maintenance queue
  Global::maintenance_queue = new MaintenanceQueue(maintenance_threads);

  // Create table info maps
  m_live_map_ptr = new TableInfoMap();
  m_replay_map_ptr = new TableInfoMap();

  if (initialize(props_ptr) != Error::OK)
    exit(1);

  /**
   * Listen for incoming connections
   */
  {
    ConnectionHandlerFactoryPtr chfp(new HandlerFactory(comm, m_app_queue_ptr, this));
    struct sockaddr_in addr;
    InetAddr::initialize(&addr, INADDR_ANY, port);  // Listen on any interface
    if ((error = comm->listen(addr, chfp)) != Error::OK) {
      HT_ERRORF("Listen error address=%s:%d - %s", inet_ntoa(addr.sin_addr), port, Error::get_text(error));
      exit(1);
    }
  }

  /**
   * Create Master client
   */
  m_master_client_ptr = new MasterClient(m_conn_manager_ptr, m_hyperspace_ptr, 300, m_app_queue_ptr);
  m_master_connection_handler = new ConnectionHandler(comm, m_app_queue_ptr, this, m_master_client_ptr);
  m_master_client_ptr->initiate_connection(m_master_connection_handler);

  fast_recover();

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
int RangeServer::initialize(PropertiesPtr &props_ptr) {
  int error;
  bool exists;
  String top_dir;

  /**
   * Create /hypertable/servers directory
   */
  if ((error = m_hyperspace_ptr->exists("/hypertable/servers", &exists)) != Error::OK) {
    HT_ERRORF("Problem checking existence of '/hypertable/servers' Hyperspace directory - %s", Error::get_text(error));
    return error;
  }

  if (!exists) {
    m_hyperspace_ptr->mkdir("/hypertable");
    if ((error = m_hyperspace_ptr->mkdir("/hypertable/servers")) != Error::OK) {
      HT_ERRORF("Problem creating directory '/hypertable/servers' - %s", Error::get_text(error));
      return error;
    }
  }

  top_dir = (String)"/hypertable/servers/" + m_location;

  /**
   * Create "server existence" file in Hyperspace and obtain an exclusive lock on it
   */

  uint32_t lock_status;
  uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_CREATE | OPEN_FLAG_CREATE | OPEN_FLAG_LOCK;
  HandleCallbackPtr null_callback;

  if ((error = m_hyperspace_ptr->open(top_dir.c_str(), oflags, null_callback, &m_existence_file_handle)) != Error::OK) {
    HT_ERRORF("Problem creating Hyperspace server existance file '%s' - %s", top_dir.c_str(), Error::get_text(error));
    exit(1);
  }

  while (true) {

    lock_status = 0;

    if ((error = m_hyperspace_ptr->try_lock(m_existence_file_handle, LOCK_MODE_EXCLUSIVE, &lock_status, &m_existence_file_sequencer)) != Error::OK) {
      HT_ERRORF("Problem obtaining exclusive lock on master Hyperspace file '%s' - %s", top_dir.c_str(), Error::get_text(error));
      exit(1);
    }

    if (lock_status == LOCK_STATUS_GRANTED)
      break;

    cout << "Waiting for exclusive lock on hyperspace:/" << top_dir << " ..." << endl;
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
    HT_ERRORF("Problem creating commit log directory '%s': %s",
              path.c_str(), e.what());
    return error;
  }

  if (Global::verbose)
    cout << "log_dir=" << Global::log_dir << endl;

  return Error::OK;
}



/**
 */
void RangeServer::fast_recover() {
  String meta_log_fname = Global::log_dir + "/range_txn/0.log";
  RangeServerMetaLogReaderPtr rsml_reader;
  CommitLogReaderPtr root_log_reader_ptr;
  CommitLogReaderPtr metadata_log_reader_ptr;
  CommitLogReaderPtr user_log_reader_ptr;
  RangeState range_state;

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
	  range_state.clear();
	  replay_load_range(0, &i->table, &i->range, &range_state);
	}
      }

      if (!m_replay_map_ptr->empty()) {
	root_log_reader_ptr = new CommitLogReader(Global::log_dfs, Global::log_dir + "/root");
	replay_log(root_log_reader_ptr);
	m_live_map_ptr->merge(m_replay_map_ptr);
      }
      //m_live_map_ptr->dump();

      /**
       * Then recover other metadata ranges
       */
      m_replay_group = RangeServerProtocol::GROUP_METADATA;

      // clear the replay map
      m_replay_map_ptr->clear();

      foreach(const RangeStateInfo *i, range_states) {
	if (i->table.id == 0 && !(i->range.end_row && !strcmp(i->range.end_row, Key::END_ROOT_ROW))) {
	  if (i->transactions.empty()) {
	    range_state.clear();
	    range_state.soft_limit = i->soft_limit;
	    replay_load_range(0, &i->table, &i->range, &range_state);
	  }
	  else {
	    // TODO fix me !!!
	  }
	}
      }

      if (!m_replay_map_ptr->empty()) {
	metadata_log_reader_ptr = new CommitLogReader(Global::log_dfs, Global::log_dir + "/metadata");
	replay_log(metadata_log_reader_ptr);
	m_live_map_ptr->merge(m_replay_map_ptr);
      }
      //m_live_map_ptr->dump();

      /**
       * Then recover the normal ranges
       */
      m_replay_group = RangeServerProtocol::GROUP_USER;

      // clear the replay map
      m_replay_map_ptr->clear();

      foreach(const RangeStateInfo *i, range_states) {
	if (i->table.id != 0) {
	  if (i->transactions.empty()) {
	    range_state.clear();
	    range_state.soft_limit = i->soft_limit;
	    replay_load_range(0, &i->table, &i->range, &range_state);
	  }
	  else {
	    // TODO fix me !!!
	  }
	}
      }

      if (!m_replay_map_ptr->empty()) {
	user_log_reader_ptr = new CommitLogReader(Global::log_dfs, Global::log_dir + "/user");
	replay_log(user_log_reader_ptr);
	m_live_map_ptr->merge(m_replay_map_ptr);
      }
      //m_live_map_ptr->dump();

    }

    /**
     *  Create the logs
     */

    if (root_log_reader_ptr)
      Global::root_log = new CommitLog(Global::log_dfs, Global::log_dir + "/root", m_props_ptr, root_log_reader_ptr.get());

    if (metadata_log_reader_ptr)
      Global::metadata_log = new CommitLog(Global::log_dfs, Global::log_dir + "/metadata", m_props_ptr, metadata_log_reader_ptr.get());

    Global::user_log = new CommitLog(Global::log_dfs, Global::log_dir + "/user", m_props_ptr, user_log_reader_ptr.get());

    Global::range_log = new RangeServerMetaLog(Global::log_dfs, meta_log_fname);
  }
  catch (Exception &e) {
    HT_ERRORF("Problem attempting fast recovery - %s - %s", Error::get_text(e.code()), e.what());
    _exit(1);
  }
}



void RangeServer::replay_log(CommitLogReaderPtr &log_reader_ptr) {
  BlockCompressionHeaderCommitLog header;
  uint8_t *base;
  size_t len;
  TableIdentifier table_id;
  DynamicBuffer dbuf;
  const uint8_t *ptr, *end;
  uint64_t timestamp;
  TableInfoPtr table_info_ptr;
  RangePtr range_ptr;
  ByteString key, value;

  while (log_reader_ptr->next((const uint8_t **)&base, &len, &header)) {

    timestamp = header.get_timestamp();

    ptr = base;
    end = base + len;

    table_id.decode(&ptr, &len);

    // Fetch table info
    if (!m_replay_map_ptr->get(table_id.id, table_info_ptr))
      continue;

    dbuf.ensure(table_id.encoded_length() + 12 + len);
    dbuf.clear();

    dbuf.ptr += 4;  // skip size
    encode_i64(&dbuf.ptr, timestamp);
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
      if (!table_info_ptr->find_containing_range(key.str(), range_ptr))
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
 *  CreateScanner
 */
void RangeServer::create_scanner(ResponseCallbackCreateScanner *cb, TableIdentifier *table, RangeSpec *range, ScanSpec *scan_spec) {
  int error = Error::OK;
  String errmsg;
  TableInfoPtr table_info;
  RangePtr range_ptr;
  CellListScannerPtr scanner_ptr;
  bool more = true;
  uint32_t id;
  Timestamp scan_timestamp;
  SchemaPtr schema_ptr;
  ScanContextPtr scan_ctx;

  if (Global::verbose) {
    cout << "RangeServer::create_scanner" << endl;
    cout << *table;
    cout << *range;
    cout << *scan_spec;
  }

  try {
    DynamicBuffer rbuf;

    if (*scan_spec->end_row && strcmp(scan_spec->start_row, scan_spec->end_row) > 0)
      HT_THROW(Error::RANGESERVER_BAD_SCAN_SPEC, "start_row > end_row");

    if (!m_live_map_ptr->get(table->id, table_info))
      throw Hypertable::Exception(Error::RANGESERVER_RANGE_NOT_FOUND, (String)"unknown table '" + table->name + "'");

    if (!table_info->get_range(range, range_ptr))
      throw Hypertable::Exception(Error::RANGESERVER_RANGE_NOT_FOUND,
                                  (String)"(a) " + table->name + "[" + range->start_row + ".." + range->end_row + "]");

    schema_ptr = table_info->get_schema();

    range_ptr->get_scan_timestamp(scan_timestamp);

    scan_ctx = new ScanContext(scan_timestamp.logical, scan_spec, range, schema_ptr);

    scanner_ptr = range_ptr->create_scanner(scan_ctx);

    // TODO: fix this kludge (0 return above means range split)
    if (!scanner_ptr)
      throw Hypertable::Exception(Error::RANGESERVER_RANGE_NOT_FOUND,
                                  (String)"(b) " + table->name + "[" + range->start_row + ".." + range->end_row + "]");

    more = FillScanBlock(scanner_ptr, rbuf);

    id = (more) ? Global::scanner_map.put(scanner_ptr, range_ptr) : 0;

    if (Global::verbose) {
      HT_INFOF("Successfully created scanner (id=%d) on table '%s'", id, table->name);
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

  more = FillScanBlock(scanner_ptr, rbuf);

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
      HT_INFOF("Successfully fetched %d bytes of scan data", ext.size-4);
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
  ScanSpec scan_spec;
  KeySpec key;
  String metadata_key_str;

  if (Global::verbose)
    cout << "load_range " << *table << " " << *range << endl;

  try {

    /** Get TableInfo, create if doesn't exist **/
    if (!m_live_map_ptr->get(table->id, table_info_ptr)) {
      table_info_ptr = new TableInfo(m_master_client_ptr, table, schema_ptr);
      register_table = true;
    }

    /**
     * Verify schema, this will create the Schema object and add it to table_info_ptr if it doesn't exist
     */
    if ((error = verify_schema(table_info_ptr, table->generation, errmsg)) != Error::OK)
      HT_THROW(error, errmsg);

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
      mutator_ptr->set(0, key, (uint8_t *)m_location.c_str(), strlen(m_location.c_str()));
      mutator_ptr->flush();

    }
    else {  //root
      uint64_t handle;
      HandleCallbackPtr null_callback;
      uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_CREATE;

      HT_INFO("Loading root METADATA range");

      if ((error = m_hyperspace_ptr->open("/hypertable/root", oflags, null_callback, &handle)) != Error::OK) {
	HT_ERRORF("Problem creating Hyperspace root file '/hypertable/root' - %s", Error::get_text(error));
	HT_ABORT;
      }

      if ((error = m_hyperspace_ptr->attr_set(handle, "Location", m_location.c_str(), strlen(m_location.c_str()))) != Error::OK) {
	HT_ERRORF("Problem creating attribute 'location' on Hyperspace file '/hypertable/root' - %s", Error::get_text(error));
	HT_ABORT;
      }

      m_hyperspace_ptr->close(handle);
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
      uint64_t timestamp;
      CommitLogReaderPtr commit_log_reader_ptr = new CommitLogReader(Global::dfs, transfer_log_dir);
      CommitLog *log;
      if (is_root)
	log = Global::root_log;
      else if (table->id == 0)
	log = Global::metadata_log;
      else
	log = Global::user_log;
      
      timestamp = Global::user_log->get_timestamp();
      range_ptr->replay_transfer_log(commit_log_reader_ptr.get(), timestamp);
      if ((error = log->link_log(commit_log_reader_ptr.get(), timestamp)) != Error::OK)
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
    HT_ERRORF("%s '%s'", Error::get_text(error), e.what());
    if (cb && (error = cb->error(e.code(), e.what())) != Error::OK) {
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
    }
  }
}


namespace {

  struct UpdateRec {
    const uint8_t *base;
    size_t len;
  };

  struct SendBackRec {
    int error;
    uint32_t offset;
    uint32_t len;
  };

  struct MinTimestampRec {
    RangePtr range_ptr;
    Timestamp timestamp;
  };
}



/**
 * Update
 */
void RangeServer::update(ResponseCallbackUpdate *cb, TableIdentifier *table, StaticBuffer &buffer) {
  uint8_t *mod_ptr;
  const uint8_t *mod_end;
  const uint8_t *add_base_ptr;
  const uint8_t *add_end_ptr;
  uint8_t *ts_ptr;
  const uint8_t auto_ts[8] = { 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff };
  string errmsg;
  int error = Error::OK;
  TableInfoPtr table_info_ptr;
  uint64_t initial_timestamp = Global::user_log->get_timestamp();
  uint64_t update_timestamp = 0;
  uint64_t min_timestamp = 0;
  const char *row;
  String split_row;
  vector<UpdateRec> rootmods;
  vector<UpdateRec> gomods;
  vector<UpdateRec> splitmods;
  UpdateRec update;
  CommitLogPtr splitlog;
  size_t rootsz = 0;
  size_t gosz = 0;
  size_t splitsz = 0;
  String end_row;
  std::vector<MinTimestampRec>  min_ts_vector;
  MinTimestampRec  min_ts_rec;
  uint64_t next_timestamp;
  uint64_t temp_timestamp;
  uint64_t memory_added = 0;
  uint64_t items_added = 0;
  bool split_pending;
  ByteString key, value;
  bool a_locked = false;
  bool b_locked = false;
  vector<SendBackRec> send_back_vector;
  const uint8_t *send_back_ptr = 0;
  uint32_t misses = 0;

  min_ts_vector.reserve(50);

  if (Global::verbose) {
    cout << "RangeServer::update" << endl;
    cout << *table;
  }

  // TODO: Sanity check mod data (checksum validation)

  try {

    // Fetch table info
    if (!m_live_map_ptr->get(table->id, table_info_ptr)) {
      StaticBuffer ext(new uint8_t [12], 12);
      uint8_t *ptr = ext.base;
      encode_i32(&ptr, Error::RANGESERVER_TABLE_NOT_FOUND);
      encode_i32(&ptr, 0);
      encode_i32(&ptr, buffer.size);
      HT_ERRORF("Unable to find table info for table '%s'", table->name);
      if ((error = cb->response(ext)) != Error::OK)
        HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
      return;
    }

    // verify schema
    if ((error = verify_schema(table_info_ptr, table->generation, errmsg)) != Error::OK) {
      HT_ERRORF("%s", errmsg.c_str());
      cb->error(error, errmsg);
      return;
    }

    mod_end = buffer.base + buffer.size;
    mod_ptr = buffer.base;

    gomods.clear();

    m_update_mutex_a.lock();
    a_locked = true;

    send_back_ptr = 0;

    while (mod_ptr < mod_end) {

      key.ptr = mod_ptr;

      row = key.str();

      // Look for containing range, add to stop mods if not found
      if (!table_info_ptr->find_containing_range(row, min_ts_rec.range_ptr)) {
        if (send_back_ptr == 0)
          send_back_ptr = mod_ptr;
        key.next(); // skip key
        key.next(); // skip value;
        mod_ptr = (uint8_t *)key.ptr;
        misses++;
        continue;
      }

      if (send_back_ptr) {
        SendBackRec send_back;
        send_back.error = Error::RANGESERVER_OUT_OF_RANGE;
        send_back.offset = send_back_ptr - buffer.base;
        send_back.len = mod_ptr - send_back_ptr;
        send_back_vector.push_back(send_back);
        send_back_ptr = 0;
      }

      add_base_ptr = mod_ptr;

      /** Increment update count (block if maintenance in progress) **/
      min_ts_rec.range_ptr->increment_update_counter();

      // Make sure range didn't just shrink
      if (strcmp(row, (min_ts_rec.range_ptr->start_row()).c_str()) <= 0) {
        min_ts_rec.range_ptr->decrement_update_counter();
        continue;
      }

      /** Obtain the most recently seen timestamp **/
      min_timestamp = min_ts_rec.range_ptr->get_latest_timestamp();

      /** Obtain "update timestamp" **/
      update_timestamp = Global::user_log->get_timestamp();

      end_row = min_ts_rec.range_ptr->end_row();

      /** Fetch range split information **/
      split_pending = min_ts_rec.range_ptr->get_split_info(split_row, splitlog);

      splitmods.clear();
      splitsz = 0;

      next_timestamp = 0;
      min_ts_rec.timestamp.logical = 0;
      min_ts_rec.timestamp.real = update_timestamp;

      while (mod_ptr < mod_end && (end_row == "" || (strcmp(row, end_row.c_str()) <= 0))) {

        // If timestamp value is set to AUTO (zero one's compliment), then assign a timestamp
        ts_ptr = mod_ptr + key.length() - 8;
        if (!memcmp(ts_ptr, auto_ts, 8)) {
          if (next_timestamp == 0)
	    next_timestamp = update_timestamp;
	  temp_timestamp = ++next_timestamp;
          if (min_ts_rec.timestamp.logical == 0 || temp_timestamp < min_ts_rec.timestamp.logical)
            min_ts_rec.timestamp.logical = temp_timestamp;
          Key::encode_ts64(&ts_ptr, temp_timestamp);
        }
        else {
          uint8_t flag = *(ts_ptr-1);
          temp_timestamp = Key::decode_ts64((const uint8_t **)&ts_ptr);
          if (flag > FLAG_DELETE_CELL && temp_timestamp <= min_timestamp) {
            error = Error::RANGESERVER_TIMESTAMP_ORDER_ERROR;
            errmsg = (string)"Update timestamp " + temp_timestamp + " is <= previously seen timestamp of " + min_timestamp;
            min_ts_rec.range_ptr->decrement_update_counter();
            m_update_mutex_a.unlock();
            goto abort;
          }
          if (min_ts_rec.timestamp.logical == 0 || temp_timestamp < min_ts_rec.timestamp.logical)
            min_ts_rec.timestamp.logical = temp_timestamp;
        }

        update.base = mod_ptr;
        key.next(); // skip key
        key.next(); // skip value
        mod_ptr = (uint8_t *)key.ptr;
        update.len = mod_ptr - update.base;
        if (split_pending && strcmp(row, split_row.c_str()) <= 0) {
          splitmods.push_back(update);
          splitsz += update.len;
        }
        else if (min_ts_rec.range_ptr->is_root()) {
          rootmods.push_back(update);
          rootsz += update.len;
	}
	else {
          gomods.push_back(update);
          gosz += update.len;
        }
        if (mod_ptr < mod_end)
          row = key.str();
      }

      add_end_ptr = mod_ptr;

      // force scans to only see updates before the earliest time in this range
      min_ts_rec.timestamp.logical--;
      min_ts_rec.range_ptr->add_update_timestamp(min_ts_rec.timestamp);
      min_ts_vector.push_back(min_ts_rec);

      if (splitsz > 0) {
        DynamicBuffer dbuf(splitsz + table->encoded_length());

        table->encode(&dbuf.ptr);

        items_added += splitmods.size();
        memory_added += splitsz;

        for (size_t i=0; i<splitmods.size(); i++) {
          memcpy(dbuf.ptr, splitmods[i].base, splitmods[i].len);
          dbuf.ptr += splitmods[i].len;
        }

        HT_EXPECT(dbuf.fill() <= (splitsz + table->encoded_length()), Error::FAILED_EXPECTATION);

	if ((error = splitlog->write(dbuf, update_timestamp)) != Error::OK) {
	  errmsg = (string)"Problem writing " + (int)dbuf.fill() + " bytes to split log";
	  m_update_mutex_a.unlock();
	  goto abort;
	}
      }

      /**
       * Apply the modifications
       */
      min_ts_rec.range_ptr->lock();
      {
        uint8_t *ptr = (uint8_t *)add_base_ptr;
        while (ptr < add_end_ptr) {
          key.ptr = ptr;
          ptr += key.length();
          value.ptr = ptr;
          ptr += value.length();
          if ((error = min_ts_rec.range_ptr->add(key, value, update_timestamp)) != Error::OK) {
            SendBackRec send_back;
            send_back.error = error;
            send_back.offset = key.ptr - buffer.base;
            send_back.len = add_end_ptr - key.ptr;
            send_back_vector.push_back(send_back);
            break;
          }
        }
      }
      min_ts_rec.range_ptr->unlock(update_timestamp);

      /**
       * Split and Compaction processing
       */
      if (!min_ts_rec.range_ptr->maintenance_in_progress()) {
        std::vector<AccessGroup::CompactionPriorityData> priority_data_vec;
        std::vector<AccessGroup *> compactions;
        uint64_t disk_usage = 0;

        min_ts_rec.range_ptr->get_compaction_priority_data(priority_data_vec);
        for (size_t i=0; i<priority_data_vec.size(); i++) {
          disk_usage += priority_data_vec[i].disk_used;
          if (!priority_data_vec[i].in_memory && priority_data_vec[i].mem_used >= (uint32_t)Global::access_group_max_mem)
            compactions.push_back(priority_data_vec[i].ag);
        }

        if (!min_ts_rec.range_ptr->is_root() &&
            (disk_usage > min_ts_rec.range_ptr->get_size_limit() ||
             (Global::range_metadata_max_bytes && table->id == 0 && disk_usage > Global::range_metadata_max_bytes))) {
          if (!min_ts_rec.range_ptr->test_and_set_maintenance())
            Global::maintenance_queue->add(new MaintenanceTaskSplit(min_ts_rec.range_ptr));
        }
        else if (!compactions.empty()) {
          if (!min_ts_rec.range_ptr->test_and_set_maintenance()) {
            for (size_t i=0; i<compactions.size(); i++)
              compactions[i]->set_compaction_bit();
            Global::maintenance_queue->add(new MaintenanceTaskCompaction(min_ts_rec.range_ptr, false));
          }
        }
      }

      if (Global::verbose) {
        HT_INFOF("Added %d (%d split off) updates to '%s'", gomods.size()+splitmods.size(), splitmods.size(), table->name);
      }
    }

    if (send_back_ptr) {
      SendBackRec send_back;
      send_back.error = Error::RANGESERVER_OUT_OF_RANGE;
      send_back.offset = send_back_ptr - buffer.base;
      send_back.len = mod_ptr - send_back_ptr;
      send_back_vector.push_back(send_back);
      send_back_ptr = 0;
    }

    m_update_mutex_b.lock();
    m_update_mutex_a.unlock();
    b_locked = true;

    /**
     * Commit ROOT mutations
     */
    if (rootsz > 0) {
      DynamicBuffer dbuf(rootsz + table->encoded_length());

      table->encode(&dbuf.ptr);

      items_added += rootmods.size();
      memory_added += rootsz;

      for (size_t i=0; i<rootmods.size(); i++) {
        memcpy(dbuf.ptr, rootmods[i].base, rootmods[i].len);
        dbuf.ptr += rootmods[i].len;
      }

      HT_EXPECT(dbuf.fill() <= (rootsz + table->encoded_length()), Error::FAILED_EXPECTATION);

      if ((error = Global::root_log->write(dbuf, initial_timestamp)) != Error::OK) {
	errmsg = (string)"Problem writing " + (int)dbuf.fill() + " bytes to ROOT commit log";
	m_update_mutex_b.unlock();
	goto abort;
      }
    }

    /**
     * Commit valid (go) mutations
     */
    if (gosz > 0) {
      DynamicBuffer dbuf(gosz + table->encoded_length());
      CommitLog *log = (table->id == 0) ? Global::metadata_log : Global::user_log;

      table->encode(&dbuf.ptr);

      items_added += gomods.size();
      memory_added += gosz;

      for (size_t i=0; i<gomods.size(); i++) {
        memcpy(dbuf.ptr, gomods[i].base, gomods[i].len);
        dbuf.ptr += gomods[i].len;
      }

      HT_EXPECT(dbuf.fill() <= (gosz + table->encoded_length()), Error::FAILED_EXPECTATION);

      if ((error = log->write(dbuf, initial_timestamp)) != Error::OK) {
	errmsg = (string)"Problem writing " + (int)dbuf.fill() + " bytes to commit log (" + log->get_log_dir() + ")";
	m_update_mutex_b.unlock();
	goto abort;
      }
    }

    m_update_mutex_b.unlock();

    if (Global::verbose && misses) {
      HT_INFOF("Sent back %d updates because out-of-range", misses);
    }

    error = Error::OK;
  }
  catch (Exception &e) {
    HT_ERRORF("Exception caught: %s", Error::get_text(e.code()));
    error = e.code();
    errmsg = e.what();
    if (b_locked)
      m_update_mutex_b.unlock();
    else if (a_locked)
      m_update_mutex_a.unlock();
  }

  m_bytes_loaded += buffer.size;

 abort:

  Global::memory_tracker.add_memory(memory_added);
  Global::memory_tracker.add_items(items_added);

  {
    uint64_t mt_memory = Global::memory_tracker.get_memory();
    uint64_t mt_items = Global::memory_tracker.get_items();
    uint64_t vm_estimate = mt_memory + (mt_items*120);
    HT_INFOF("drj mem=%lld items=%lld vm-est%lld", mt_memory, mt_items, vm_estimate);
  }

  splitlog = 0;

  // unblock scanner timestamp and decrement update counter
  for (size_t i=0; i<min_ts_vector.size(); i++) {
    min_ts_vector[i].range_ptr->remove_update_timestamp(min_ts_vector[i].timestamp);
    min_ts_vector[i].range_ptr->decrement_update_counter();
  }

  if (error == Error::OK) {
    /**
     * Send back response
     */
    if (!send_back_vector.empty()) {
      StaticBuffer ext(new uint8_t [send_back_vector.size() * 12], send_back_vector.size() * 12);
      uint8_t *ptr = ext.base;
      for (size_t i=0; i<send_back_vector.size(); i++) {
        encode_i32(&ptr, send_back_vector[i].error);
        encode_i32(&ptr, send_back_vector[i].offset);
        encode_i32(&ptr, send_back_vector[i].len);
      }
      if ((error = cb->response(ext)) != Error::OK) {
        HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
      }
    }
    else {
      if ((error = cb->response_ok()) != Error::OK) {
        HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
      }
    }
  }
  else {
    HT_ERRORF("%s '%s'", Error::get_text(error), errmsg.c_str());
    if ((error = cb->error(error, errmsg)) != Error::OK) {
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
    }
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
      HT_ERRORF("drop_table '%s' - table not found", table->name);
    }
    mutator_ptr->flush();
  }
  catch (Hypertable::Exception &e) {
    HT_ERRORF("Problem clearing 'Location' columns of METADATA - %s", Error::get_text(e.code()));
    cb->error(e.code(), "Problem clearing 'Location' columns of METADATA");
    return;
  }

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
  String errmsg;
  int error = Error::OK;
  SchemaPtr schema_ptr;
  TableInfoPtr table_info_ptr;
  RangePtr range_ptr;
  string table_dfsdir;
  string range_dfsdir;
  bool register_table = false;
  TableScannerPtr scanner_ptr;
  TableMutatorPtr mutator_ptr;
  ScanSpec scan_spec;
  KeySpec key;
  String metadata_key_str;

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

    /**
     * Verify schema, this will create the Schema object and add it to table_info_ptr if it doesn't exist
     */
    if ((error = verify_schema(table_info_ptr, table->generation, errmsg)) != Error::OK)
      HT_THROW(error, errmsg);


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
  ByteString key, value;
  const uint8_t *ptr = data;
  const uint8_t *end_ptr = data + len;
  const uint8_t *block_end_ptr;
  uint32_t block_size;
  size_t remaining = len;
  const char *row;
  String err_msg;
  uint64_t timestamp;
  RangePtr range_ptr;
  String end_row;
  uint32_t count;
  uint64_t memory_added = 0;
  uint64_t items_added = 0;
  int error;

  if (Global::verbose) {
    HT_INFOF("replay_update - length=%ld", len);
    cout << flush;
  }

  try {

    while (ptr < end_ptr) {

      // decode key/value block size + timestamp
      block_size = decode_i32(&ptr, &remaining);
      timestamp = decode_i64(&ptr, &remaining);

      if (m_replay_log_ptr) {
	DynamicBuffer dbuf(0, false);
	dbuf.base = (uint8_t *)ptr;
	dbuf.ptr = dbuf.base + remaining;
	if ((error = m_replay_log_ptr->write(dbuf, timestamp)) != Error::OK)
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

        row = ByteString(ptr).str();

        // Look for containing range, add to stop mods if not found
        if (!table_info_ptr->find_containing_range(row, range_ptr))
          HT_THROWF(Error::RANGESERVER_RANGE_NOT_FOUND, "Unable to find "
                    "range for row '%s'", row);

        end_row = range_ptr->end_row();

	key.ptr = ptr;
        while (ptr < block_end_ptr &&
               (end_row == "" || (strcmp(row, end_row.c_str()) <= 0))) {

          // extract the key
          ptr += key.length();

          if (ptr > end_ptr)
            HT_THROW(Error::REQUEST_TRUNCATED, "Problem decoding key");

          value.ptr = ptr;
          ptr += value.length();

          if (ptr > end_ptr)
            HT_THROW(Error::REQUEST_TRUNCATED, "Problem decoding value");

          HT_EXPECT(range_ptr->replay_add(key, value, timestamp, &count) == Error::OK, Error::FAILED_EXPECTATION);

          if (count) {
            items_added += count;
            memory_added += count * (ptr - key.ptr);
          }

	  key.ptr = ptr;
	  if (ptr < block_end_ptr)
	    row = key.str();
        }
      }
    }

    Global::memory_tracker.add_memory(memory_added);
    Global::memory_tracker.add_items(items_added);

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

    if ((error = log->link_log(m_replay_log_ptr.get(), Global::user_log->get_timestamp())) != Error::OK)
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



int RangeServer::verify_schema(TableInfoPtr &table_info_ptr, int generation, String &err_msg) {
  String tablefile = (String)"/hypertable/tables/" + table_info_ptr->get_name();
  DynamicBuffer valbuf;
  HandleCallbackPtr null_handle_callback;
  int error;
  uint64_t handle;
  SchemaPtr schema_ptr = table_info_ptr->get_schema();

  if (schema_ptr.get() == 0 || schema_ptr->get_generation() < generation) {

    if ((error = m_hyperspace_ptr->open(tablefile.c_str(), OPEN_FLAG_READ, null_handle_callback, &handle)) != Error::OK) {
      HT_ERRORF("Unable to open Hyperspace file '%s' (%s)", tablefile.c_str(), Error::get_text(error));
      exit(1);
    }

    if ((error = m_hyperspace_ptr->attr_get(handle, "schema", valbuf)) != Error::OK) {
      err_msg = (String)"Problem getting 'schema' attribute for '" + tablefile + "'";
      return error;
    }

    m_hyperspace_ptr->close(handle);

    schema_ptr = Schema::new_instance((const char *)valbuf.base, valbuf.fill(), true);
    if (!schema_ptr->is_valid()) {
      err_msg = (String)"Schema Parse Error for table '" + table_info_ptr->get_name() + "' : " + schema_ptr->get_error_string();
      return Error::RANGESERVER_SCHEMA_PARSE_ERROR;
    }

    table_info_ptr->update_schema(schema_ptr);

    // Generation check ...
    if (schema_ptr->get_generation() < generation) {
      err_msg = (String)"Fetched Schema generation for table '" + table_info_ptr->get_name() + "' is " + schema_ptr->get_generation() + " but supplied is " + generation;
      return Error::RANGESERVER_GENERATION_MISMATCH;
    }
  }

  return Error::OK;
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

  m_live_map_ptr->get_all(table_vec);

  if (table_vec.empty())
    return;

  /**
   * If we've got METADATA ranges, process them first
   */
  if (table_vec[0]->get_id() == 0) {
    first_user_table = 1;
    table_vec[0]->get_range_vector(range_vec);
    schedule_log_cleanup_compactions(range_vec, Global::metadata_log, m_log_roll_limit);
  }

  range_vec.clear();
  for (size_t i=first_user_table; i<table_vec.size(); i++)
    table_vec[i]->get_range_vector(range_vec);

  // compute prune threshold
  prune_threshold = ((double)(m_bytes_loaded / m_timer_interval) / 1000000.0) * (double)Global::log_prune_threshold_max;
  if (prune_threshold < Global::log_prune_threshold_min)
    prune_threshold = Global::log_prune_threshold_min;
  else if (prune_threshold > Global::log_prune_threshold_max)
    prune_threshold = Global::log_prune_threshold_max;

  HT_INFOF("Cleaning log (threshold=%lld)", prune_threshold);
  cout << flush;

  schedule_log_cleanup_compactions(range_vec, Global::user_log, m_log_roll_limit);

  m_bytes_loaded = 0;
}


void RangeServer::schedule_log_cleanup_compactions(std::vector<RangePtr> &range_vec, CommitLog *log, uint64_t prune_threshold) {
  std::vector<AccessGroup::CompactionPriorityData> priority_data_vec;
  LogFragmentPriorityMap log_frag_map;
  uint64_t timestamp, oldest_cached_timestamp = 0;

  range_vec.clear();

  // Load up a vector of compaction priority data
  for (size_t i=0; i<range_vec.size(); i++) {
    size_t start = priority_data_vec.size();
    range_vec[i]->get_compaction_priority_data(priority_data_vec);
    for (size_t j=start; j<priority_data_vec.size(); j++) {
      priority_data_vec[j].user_data = (void *)i;
      if ((timestamp = priority_data_vec[j].ag->get_oldest_cached_timestamp()) != 0) {
	if (oldest_cached_timestamp == 0 || timestamp < oldest_cached_timestamp)
	  oldest_cached_timestamp = timestamp;
      }
    }
  }

  log->load_fragment_priority_map(log_frag_map);

  /**
   * Determine which AGs need compaction for the sake of
   * garbage collecting commit log fragments
   */
  for (size_t i=0; i<priority_data_vec.size(); i++) {

    if (priority_data_vec[i].oldest_cached_timestamp == 0)
      continue;

    LogFragmentPriorityMap::iterator map_iter = log_frag_map.lower_bound(priority_data_vec[i].oldest_cached_timestamp);

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
  if (oldest_cached_timestamp != 0)
    log->purge(oldest_cached_timestamp);

}



/**
 */
uint64_t RangeServer::get_timer_interval() {
  return m_timer_interval*1000;
}
