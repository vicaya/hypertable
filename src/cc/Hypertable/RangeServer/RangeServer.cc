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

#include <cassert>
#include <string>

#include <boost/shared_array.hpp>

extern "C" {
#include <sys/time.h>
#include <sys/resource.h>
}

#include "Common/ByteOrder.h"
#include "Common/FileUtils.h"
#include "Common/md5.h"
#include "Common/StringExt.h"
#include "Common/System.h"

#include "Hypertable/Lib/CommitLog.h"
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

using namespace Hypertable;
using namespace std;

namespace {
  const int DEFAULT_SCANBUF_SIZE = 32768;
  const int DEFAULT_PORT    = 38060;
}


/**
 * Constructor
 */
RangeServer::RangeServer(PropertiesPtr &props_ptr, ConnectionManagerPtr &conn_manager_ptr, ApplicationQueuePtr &app_queue_ptr, Hyperspace::SessionPtr &hyperspace_ptr) : m_props_ptr(props_ptr), m_verbose(false), m_conn_manager_ptr(conn_manager_ptr), m_app_queue_ptr(app_queue_ptr), m_hyperspace_ptr(hyperspace_ptr), m_last_commit_log_clean(0) {
  int error;
  uint16_t port;
  Comm *comm = conn_manager_ptr->get_comm();

  Global::rangeMaxBytes           = props_ptr->get_int64("Hypertable.RangeServer.Range.MaxBytes", 200000000LL);
  Global::localityGroupMaxFiles   = props_ptr->get_int("Hypertable.RangeServer.AccessGroup.MaxFiles", 10);
  Global::localityGroupMergeFiles = props_ptr->get_int("Hypertable.RangeServer.AccessGroup.MergeFiles", 4);
  Global::localityGroupMaxMemory  = props_ptr->get_int("Hypertable.RangeServer.AccessGroup.MaxMemory", 50000000);
  port                            = props_ptr->get_int("Hypertable.RangeServer.Port", DEFAULT_PORT);
  m_scanner_ttl                   = (time_t)props_ptr->get_int("Hypertable.RangeServer.Scanner.Ttl", 120);
  m_timer_interval                = props_ptr->get_int("Hypertable.RangeServer.Timer.Interval", 60);

  if (m_timer_interval >= 1000) {
    HT_ERROR("Hypertable.RangeServer.Timer.Interval property too large, exiting ...");
    exit(1);
  }

  if (m_scanner_ttl < (time_t)10) {
    HT_WARNF("Value %u for Hypertable.RangeServer.Scanner.ttl is too small, setting to 10", (unsigned int)m_scanner_ttl);
    m_scanner_ttl = (time_t)10;
  }

  uint64_t blockCacheMemory = props_ptr->get_int64("Hypertable.RangeServer.BlockCache.MaxMemory", 200000000LL);
  Global::blockCache = new FileBlockCache(blockCacheMemory);

  assert(Global::localityGroupMergeFiles <= Global::localityGroupMaxFiles);

  m_verbose = props_ptr->get_bool("Hypertable.Verbose", false);

  if (Global::verbose) {
    cout << "Hypertable.RangeServer.AccessGroup.MaxFiles=" << Global::localityGroupMaxFiles << endl;
    cout << "Hypertable.RangeServer.AccessGroup.MaxMemory=" << Global::localityGroupMaxMemory << endl;
    cout << "Hypertable.RangeServer.AccessGroup.MergeFiles=" << Global::localityGroupMergeFiles << endl;
    cout << "Hypertable.RangeServer.BlockCache.MaxMemory=" << blockCacheMemory << endl;
    cout << "Hypertable.RangeServer.Range.MaxBytes=" << Global::rangeMaxBytes << endl;
    cout << "Hypertable.RangeServer.Port=" << port << endl;
    //cout << "Hypertable.RangeServer.workers=" << workerCount << endl;
  }

  //m_conn_manager_ptr = new ConnectionManager(comm);
  //m_app_queue_ptr = new ApplicationQueue(workerCount);

  Global::protocol = new Hypertable::RangeServerProtocol();

  DfsBroker::Client *dfsClient = new DfsBroker::Client(m_conn_manager_ptr, props_ptr);

  if (m_verbose) {
    cout << "DfsBroker.Host=" << props_ptr->get("DfsBroker.Host", "") << endl;
    cout << "DfsBroker.Port=" << props_ptr->get("DfsBroker.Port", "") << endl;
    cout << "DfsBroker.Timeout=" << props_ptr->get("DfsBroker.Timeout", "") << endl;
  }

  if (!dfsClient->wait_for_connection(30)) {
    HT_ERROR("Unable to connect to DFS Broker, exiting...");
    exit(1);
  }

  Global::dfs = dfsClient;

  /**
   * Check for and connect to commit log DFS broker
   */
  {
    const char *logHost = props_ptr->get("Hypertable.RangeServer.CommitLog.DfsBroker.Host", 0);
    uint16_t logPort    = props_ptr->get_int("Hypertable.RangeServer.CommitLog.DfsBroker.Port", 0);
    struct sockaddr_in addr;
    if (logHost != 0) {
      InetAddr::initialize(&addr, logHost, logPort);
      dfsClient = new DfsBroker::Client(m_conn_manager_ptr, addr, 600);
      if (!dfsClient->wait_for_connection(30)) {
	HT_ERROR("Unable to connect to DFS Broker, exiting...");
	exit(1);
      }
      Global::logDfs = dfsClient;
    }
    else {
      Global::logDfs = Global::dfs;
    }
  }

  /**
   * 
   */
  {
    std::string hostStr;
    struct sockaddr_in addr;    

    InetAddr::get_hostname(hostStr);
    if (!InetAddr::initialize(&addr, hostStr.c_str(), port))
      exit(1);

    m_location = (std::string)inet_ntoa(addr.sin_addr) + "_" + (int)port;
  }

  if (initialize(props_ptr) != Error::OK)
    exit(1);

  // Create the maintenance queue
  Global::maintenance_queue = new MaintenanceQueue(1);

  // Create table info maps
  m_live_map_ptr = new TableInfoMap();
  m_replay_map_ptr = new TableInfoMap();

  fast_recover();

  /**
   * Listen for incoming connections
   */
  {
    ConnectionHandlerFactoryPtr chfPtr(new HandlerFactory(comm, m_app_queue_ptr, this));
    struct sockaddr_in addr;
    InetAddr::initialize(&addr, INADDR_ANY, port);  // Listen on any interface
    if ((error = comm->listen(addr, chfPtr)) != Error::OK) {
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

}


/**
 *
 */
RangeServer::~RangeServer() {
  delete Global::blockCache;
  delete Global::protocol;
  m_hyperspace_ptr = 0;
  delete Global::dfs;
  if (Global::logDfs != Global::dfs)
    delete Global::logDfs;
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
  std::string top_dir;
  std::string meta_log_dir;
  std::string primary_log_dir;

  /**
   * Create /hypertable/servers directory
   */
  if ((error = m_hyperspace_ptr->exists("/hypertable/servers", &exists)) != Error::OK) {
    HT_ERRORF("Problem checking existence of '/hypertable/servers' Hyperspace directory - %s", Error::get_text(error));
    return error;
  }

  if (!exists) {
    if ((error = m_hyperspace_ptr->mkdir("/hypertable/servers")) != Error::OK) {
      HT_ERRORF("Problem creating directory '/hypertable/servers' - %s", Error::get_text(error));
      return error;
    }
  }

  top_dir = (std::string)"/hypertable/servers/" + m_location;

  /**
   * Create "server existence" file in Hyperspace and obtain an exclusive lock on it
   */

  uint32_t lockStatus;
  uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_CREATE | OPEN_FLAG_CREATE | OPEN_FLAG_LOCK;
  HandleCallbackPtr nullCallbackPtr;

  if ((error = m_hyperspace_ptr->open(top_dir.c_str(), oflags, nullCallbackPtr, &m_existence_file_handle)) != Error::OK) {
    HT_ERRORF("Problem creating Hyperspace server existance file '%s' - %s", top_dir.c_str(), Error::get_text(error));
    exit(1);
  }

  while (true) {

    lockStatus = 0;

    if ((error = m_hyperspace_ptr->try_lock(m_existence_file_handle, LOCK_MODE_EXCLUSIVE, &lockStatus, &m_existence_file_sequencer)) != Error::OK) {
      HT_ERRORF("Problem obtaining exclusive lock on master Hyperspace file '%s' - %s", top_dir.c_str(), Error::get_text(error));
      exit(1);
    }
    
    if (lockStatus == LOCK_STATUS_GRANTED)
      break;

    cout << "Waiting for exclusive lock on hyperspace:/" << top_dir << " ..." << endl;
  }

  Global::logDir = top_dir + "/log";

  primary_log_dir = Global::logDir + "/primary";

  /**
   * Create /hypertable/servers/X.X.X.X_port/log/primary directory
   */
  try { Global::logDfs->mkdirs(Global::logDir); }
  catch (Exception &e) {
    HT_ERRORF("Problem creating primary log directory '%s': %s",
              Global::logDir.c_str(), e.what());
    return error;
  }

  if (Global::verbose)
    cout << "logDir=" << Global::logDir << endl;

  Global::log = new CommitLog(Global::logDfs, primary_log_dir, props_ptr);

  return Error::OK;
}



/**
 */
void RangeServer::fast_recover() {
  bool exists;
  std::string meta_log_dir = Global::logDir + "/meta";
  RangeServerMetaLogReaderPtr meta_log_reader_ptr;
  RangeStates range_states;

  try {

    /**
     * Check for existence of /hypertable/servers/X.X.X.X_port/log/meta directory
     */
    exists = Global::logDfs->exists(meta_log_dir);

    if (!exists)
      return;

    meta_log_reader_ptr = new RangeServerMetaLogReader(Global::logDfs, meta_log_dir);

    meta_log_reader_ptr->load_range_states(range_states);

  }
  catch (Exception &e) {
    HT_ERRORF("Problem attempting fast recovery - %s - %s", Error::get_text(e.code()), e.what());
    exit(1);
  }

}



/**
 * Compact
 */
void RangeServer::compact(ResponseCallback *cb, TableIdentifier *table, RangeSpec *range, uint8_t compaction_type) {
  int error = Error::OK;
  std::string errMsg;
  TableInfoPtr tableInfoPtr;
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
  if (!m_live_map_ptr->get(table->id, tableInfoPtr)) {
    error = Error::RANGESERVER_RANGE_NOT_FOUND;
    errMsg = "No ranges loaded for table '" + (std::string)table->name + "'";
    goto abort;
  }

  /**
   * Fetch range info
   */
  if (!tableInfoPtr->get_range(range, range_ptr)) {
    error = Error::RANGESERVER_RANGE_NOT_FOUND;
    errMsg = (std::string)table->name + "[" + range->startRow + ".." + range->endRow + "]";
    goto abort;
  }

  // schedule the compaction
  if (!range_ptr->test_and_set_maintenance())
    Global::maintenance_queue->add( new MaintenanceTaskCompaction(range_ptr, major) );

  if ((error = cb->response_ok()) != Error::OK) {
    HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
  }

  if (Global::verbose) {
    HT_INFOF("Compaction (%s) scheduled for table '%s' end row '%s'",
		(major ? "major" : "minor"), table->name, range->endRow);
  }

  error = Error::OK;

 abort:
  if (error != Error::OK) {
    HT_ERRORF("%s '%s'", Error::get_text(error), errMsg.c_str());
    if ((error = cb->error(error, errMsg)) != Error::OK) {
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
    }
  }
}



/** 
 *  CreateScanner
 */
void RangeServer::create_scanner(ResponseCallbackCreateScanner *cb, TableIdentifier *table, RangeSpec *range, ScanSpec *scan_spec) {
  uint8_t *kvBuffer = 0;
  uint32_t *kvLenp = 0;
  int error = Error::OK;
  std::string errMsg;
  TableInfoPtr tableInfoPtr;
  RangePtr range_ptr;
  CellListScannerPtr scannerPtr;
  bool more = true;
  uint32_t id;
  Timestamp scan_timestamp;
  SchemaPtr schemaPtr;
  ScanContextPtr scanContextPtr;

  if (Global::verbose) {
    cout << "RangeServer::create_scanner" << endl;
    cout << *table;
    cout << *range;
    cout << *scan_spec;
  }

  try {

    if (!m_live_map_ptr->get(table->id, tableInfoPtr))
      throw Hypertable::Exception(Error::RANGESERVER_RANGE_NOT_FOUND, (std::string)"unknown table '" + table->name + "'");

    if (!tableInfoPtr->get_range(range, range_ptr))
      throw Hypertable::Exception(Error::RANGESERVER_RANGE_NOT_FOUND,
				  (std::string)"(a) " + table->name + "[" + range->startRow + ".." + range->endRow + "]");

    schemaPtr = tableInfoPtr->get_schema();

    range_ptr->get_scan_timestamp(scan_timestamp);

    scanContextPtr = new ScanContext(scan_timestamp.logical, scan_spec, range, schemaPtr);
 
    scannerPtr = range_ptr->create_scanner(scanContextPtr);

    // TODO: fix this kludge (0 return above means range split)
    if (!scannerPtr)
      throw Hypertable::Exception(Error::RANGESERVER_RANGE_NOT_FOUND,
				  (std::string)"(b) " + table->name + "[" + range->startRow + ".." + range->endRow + "]");

    kvBuffer = new uint8_t [ sizeof(int32_t) + DEFAULT_SCANBUF_SIZE ];
    kvLenp = (uint32_t *)kvBuffer;

    more = FillScanBlock(scannerPtr, kvBuffer+sizeof(int32_t), DEFAULT_SCANBUF_SIZE, kvLenp);
    if (more)
      id = Global::scannerMap.put(scannerPtr, range_ptr);
    else
      id = 0;

    if (Global::verbose) {
      HT_INFOF("Successfully created scanner (id=%d) on table '%s'", id, table->name);
    }

    /**
     *  Send back data
     */
    {
      short moreFlag = more ? 0 : 1;
      ExtBufferT ext;
      ext.buf = kvBuffer;
      ext.len = sizeof(int32_t) + *kvLenp;
      if ((error = cb->response(moreFlag, id, ext)) != Error::OK) {
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


void RangeServer::destroy_scanner(ResponseCallback *cb, uint32_t scannerId) {
  HT_INFOF("destroying scanner id=%u", scannerId);
  Global::scannerMap.remove(scannerId);
  cb->response_ok();
}


void RangeServer::fetch_scanblock(ResponseCallbackFetchScanblock *cb, uint32_t scannerId) {
  string errMsg;
  int error = Error::OK;
  CellListScannerPtr scannerPtr;
  RangePtr range_ptr;
  bool more = true;
  uint8_t *kvBuffer = 0;
  uint32_t *kvLenp = 0;
  uint32_t bytesFetched = 0;

  if (Global::verbose) {
    cout << "RangeServer::fetch_scanblock" << endl;
    cout << "Scanner ID = " << scannerId << endl;
  }

  if (!Global::scannerMap.get(scannerId, scannerPtr, range_ptr)) {
    error = Error::RANGESERVER_INVALID_SCANNER_ID;
    char tbuf[32];
    sprintf(tbuf, "%d", scannerId);
    errMsg = (string)tbuf;
    goto abort;    
  }

  kvBuffer = new uint8_t [ sizeof(int32_t) + DEFAULT_SCANBUF_SIZE ];
  kvLenp = (uint32_t *)kvBuffer;

  more = FillScanBlock(scannerPtr, kvBuffer+sizeof(int32_t), DEFAULT_SCANBUF_SIZE, kvLenp);
  if (!more)
    Global::scannerMap.remove(scannerId);
  bytesFetched = *kvLenp;

  /**
   *  Send back data
   */
  {
    short moreFlag = more ? 0 : 1;
    ExtBufferT ext;
    ext.buf = kvBuffer;
    ext.len = sizeof(int32_t) + *kvLenp;
    if ((error = cb->response(moreFlag, scannerId, ext)) != Error::OK) {
      HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
    }
  }

  if (Global::verbose) {
    HT_INFOF("Successfully fetched %d bytes of scan data", bytesFetched);
  }

  error = Error::OK;

 abort:
  if (error != Error::OK) {
    HT_ERRORF("%s '%s'", Error::get_text(error), errMsg.c_str());
    if ((error = cb->error(error, errMsg)) != Error::OK) {
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
    }
  }
}


/**
 * LoadRange
 */
void RangeServer::load_range(ResponseCallback *cb, TableIdentifier *table, RangeSpec *range, const char *transfer_log_dir, uint64_t soft_limit, uint16_t flags) {
  DynamicBuffer endRowBuffer(0);
  std::string errMsg;
  int error = Error::OK;
  SchemaPtr schemaPtr;
  TableInfoPtr table_info_ptr;
  RangePtr range_ptr;
  string tableHdfsDir;
  string rangeHdfsDir;
  char md5DigestStr[33];
  bool registerTable = false;
  bool is_root = !strcmp(table->name, "METADATA") && (*range->startRow == 0) && !strcmp(range->endRow, Key::END_ROOT_ROW);
  TableScannerPtr scanner_ptr;
  TableMutatorPtr mutator_ptr;
  ScanSpec scan_spec;
  KeySpec key;
  std::string metadata_key_str;
  bool replay = (flags & RangeServerProtocol::LOAD_RANGE_FLAG_REPLAY) == RangeServerProtocol::LOAD_RANGE_FLAG_REPLAY;

  if (Global::verbose) {
    cout << "load_range" << endl;
    cout << *table;
    cout << *range;
    cout << "flags = " << flags << endl;
    cout << flush;
  }

  try {

    if (replay) {

      /** Get TableInfo from replay map, or copy it from live map, or create if doesn't exist **/
      if (!m_replay_map_ptr->get(table->id, table_info_ptr)) {
	if (!m_live_map_ptr->get(table->id, table_info_ptr)) {
	  table_info_ptr = new TableInfo(m_master_client_ptr, table, schemaPtr);
	  registerTable = true;
	}
	else
	  table_info_ptr = table_info_ptr->create_shallow_copy();
      }

    }
    else {
      /** Get TableInfo, create if doesn't exist **/
      if (!m_live_map_ptr->get(table->id, table_info_ptr)) {
	table_info_ptr = new TableInfo(m_master_client_ptr, table, schemaPtr);
	registerTable = true;
      }
    }

    /**
     * Verify schema, this will create the Schema object and add it to table_info_ptr if it doesn't exist
     */
    if ((error = verify_schema(table_info_ptr, table->generation, errMsg)) != Error::OK)
      throw Exception(error, errMsg);


    if (registerTable) {
      if (replay)
	m_replay_map_ptr->set(table->id, table_info_ptr);
      else
	m_live_map_ptr->set(table->id, table_info_ptr);
    }
	
    /**
     * Make sure this range is not already loaded
     */
    if (table_info_ptr->get_range(range, range_ptr))
      throw Exception(Error::RANGESERVER_RANGE_ALREADY_LOADED, (std::string)table->name + "[" + range->startRow + ".." + range->endRow + "]");

    /**
     * Lazily create METADATA table pointer
     */
    if (!Global::metadata_table_ptr) {
      boost::mutex::scoped_lock lock(m_mutex);
      Global::metadata_table_ptr = new Table(m_props_ptr, m_conn_manager_ptr, Global::hyperspace_ptr, "METADATA");
    }

    if (!replay) {

      /**
       * Take ownership of the range by writing the 'Location' column in the
       * METADATA table, or /hypertable/root{location} attribute of Hyperspace
       * if it is the root range.
       */
      if (!is_root) {

	metadata_key_str = std::string("") + (uint32_t)table->id + ":" + range->endRow;

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
	HandleCallbackPtr nullCallbackPtr;
	uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_CREATE;

	HT_INFO("Loading root METADATA range");

	if ((error = m_hyperspace_ptr->open("/hypertable/root", oflags, nullCallbackPtr, &handle)) != Error::OK) {
	  HT_ERRORF("Problem creating Hyperspace root file '/hypertable/root' - %s", Error::get_text(error));
	  DUMP_CORE;
	}

	if ((error = m_hyperspace_ptr->attr_set(handle, "location", m_location.c_str(), strlen(m_location.c_str()))) != Error::OK) {
	  HT_ERRORF("Problem creating attribute 'location' on Hyperspace file '/hypertable/root' - %s", Error::get_text(error));
	  DUMP_CORE;
	}

	m_hyperspace_ptr->close(handle);
      }

      schemaPtr = table_info_ptr->get_schema();

      /**
       * Check for existence of and create, if necessary, range directory (md5 of endrow)
       * under all locality group directories for this table
       */
      {
	assert(*range->endRow != 0);
	md5_string(range->endRow, md5DigestStr);
	md5DigestStr[24] = 0;
	tableHdfsDir = (string)"/hypertable/tables/" + (string)table->name;
	list<Schema::AccessGroup *> *lgList = schemaPtr->get_access_group_list();
	for (list<Schema::AccessGroup *>::iterator lgIter = lgList->begin(); lgIter != lgList->end(); lgIter++) {
	  // notice the below variables are different "range" vs. "table"
	  rangeHdfsDir = tableHdfsDir + "/" + (*lgIter)->name + "/" + md5DigestStr;
	  Global::dfs->mkdirs(rangeHdfsDir);
	}
      }
    }

    range_ptr = new Range(m_master_client_ptr, *table, schemaPtr, range, soft_limit);

    /**
     * NOTE: The range does not need to be locked in the following replay since
     * it has not been added yet and therefore no one else can find it and
     * concurrently access it.
     */
    if (!replay) {
      if (transfer_log_dir && *transfer_log_dir) {
	CommitLogReaderPtr commit_log_reader_ptr = new CommitLogReader(Global::dfs, transfer_log_dir);
	uint64_t timestamp = Global::log->get_timestamp();
	range_ptr->replay_transfer_log(commit_log_reader_ptr.get(), timestamp);
	if ((error = Global::log->link_log(commit_log_reader_ptr.get(), timestamp)) != Error::OK)
	  throw Exception(error, (std::string)"Unable to link external log '" + transfer_log_dir + "' into commit log");
      }
    }

    table_info_ptr->add_range(range_ptr);

    if ((error = cb->response_ok()) != Error::OK) {
      HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
    }
    else {
      HT_INFOF("Successfully loaded range %s[%s..%s]", table->name, range->startRow, range->endRow);
    }

  }
  catch (Hypertable::Exception &e) {
    HT_ERRORF("%s '%s'", Error::get_text(error), e.what());
    if ((error = cb->error(e.code(), e.what())) != Error::OK) {
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
    }
  }
}


namespace {

  typedef struct {
    const uint8_t *base;
    size_t len;
  } UpdateRecT;

  typedef struct {
    RangePtr range_ptr;
    Timestamp timestamp;
  } MinTimestampRecT;

}



/**
 * Update
 */
void RangeServer::update(ResponseCallbackUpdate *cb, TableIdentifier *table, BufferT &buffer) {
  const uint8_t *mod_ptr;
  const uint8_t *mod_end;
  const uint8_t *add_base_ptr;
  const uint8_t *add_end_ptr;
  uint8_t *ts_ptr;
  const uint8_t auto_ts[8] = { 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff };
  string errMsg;
  int error = Error::OK;
  TableInfoPtr table_info_ptr;
  uint64_t update_timestamp = 0;
  uint64_t min_timestamp = 0;
  const char *row;
  std::string split_row;
  vector<UpdateRecT> goMods;
  vector<UpdateRecT> splitMods;
  vector<UpdateRecT> stopMods;
  UpdateRecT update;
  ByteString32Ptr splitKeyPtr;
  CommitLogPtr splitLogPtr;
  size_t goSize = 0;
  size_t stopSize = 0;
  size_t splitSize = 0;
  std::string endRow;
  std::vector<MinTimestampRecT>  min_ts_vector;
  MinTimestampRecT  min_ts_rec;
  uint64_t next_timestamp;
  uint64_t temp_timestamp;
  uint64_t memory_added = 0;
  uint64_t items_added = 0;
  bool split_pending;

  min_ts_vector.reserve(50);

  if (Global::verbose) {
    cout << "RangeServer::update" << endl;
    cout << *table;
  }

  // TODO: Sanity check mod data (checksum validation)

  // Fetch table info
  if (!m_live_map_ptr->get(table->id, table_info_ptr)) {
    ExtBufferT ext;
    ext.buf = new uint8_t [ buffer.len ];
    memcpy(ext.buf, buffer.buf, buffer.len);
    ext.len = buffer.len;
    HT_ERRORF("Unable to find table info for table '%s'", table->name);
    if ((error = cb->response(ext)) != Error::OK) {
      HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
    }
    return;
  }

  // verify schema
  if ((error = verify_schema(table_info_ptr, table->generation, errMsg)) != Error::OK) {
    HT_ERRORF("%s", errMsg.c_str());
    cb->error(error, errMsg);
    return;
  }

  mod_end = buffer.buf + buffer.len;
  mod_ptr = buffer.buf;

  goMods.clear();

  boost::detail::thread::lock_ops<boost::mutex>::lock(m_update_mutex_a);

  while (mod_ptr < mod_end) {

    row = (const char *)((ByteString32T *)mod_ptr)->data;

    // Look for containing range, add to stop mods if not found
    if (!table_info_ptr->find_containing_range(row, min_ts_rec.range_ptr)) {
      update.base = mod_ptr;
      mod_ptr += Length((const ByteString32T *)mod_ptr); // skip key
      mod_ptr += Length((const ByteString32T *)mod_ptr); // skip value
      update.len = mod_ptr - update.base;
      stopMods.push_back(update);
      stopSize += update.len;
      continue;
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
    update_timestamp = Global::log->get_timestamp();

    endRow = min_ts_rec.range_ptr->end_row();

    /** Fetch range split information **/
    split_pending = min_ts_rec.range_ptr->get_split_info(split_row, splitLogPtr);

    splitMods.clear();
    splitSize = 0;

    next_timestamp = 0;
    min_ts_rec.timestamp.logical = 0;
    min_ts_rec.timestamp.real = update_timestamp;

    while (mod_ptr < mod_end && (endRow == "" || (strcmp(row, endRow.c_str()) <= 0))) {

      // If timestamp value is set to AUTO (zero one's compliment), then assign a timestamp
      ts_ptr = (uint8_t *)mod_ptr + Length((const ByteString32T *)mod_ptr) - 8;
      if (!memcmp(ts_ptr, auto_ts, 8)) {
	if (next_timestamp == 0) {
	  boost::xtime now;
	  boost::xtime_get(&now, boost::TIME_UTC);
	  next_timestamp = ((uint64_t)now.sec * 1000000000LL) + (uint64_t)now.nsec;
	  //HT_INFOF("fugazi TIMESTAMP %lld", next_timestamp);
	}
	temp_timestamp = ++next_timestamp;
	if (min_ts_rec.timestamp.logical == 0 || temp_timestamp < min_ts_rec.timestamp.logical)
	  min_ts_rec.timestamp.logical = temp_timestamp;
	temp_timestamp = ByteOrderSwapInt64(temp_timestamp);
	temp_timestamp = ~temp_timestamp;
	memcpy(ts_ptr, &temp_timestamp, 8);
	temp_timestamp = next_timestamp;
      }
      else {
	memcpy(&temp_timestamp, ts_ptr, 8);
	temp_timestamp = ByteOrderSwapInt64(temp_timestamp);
	temp_timestamp = ~temp_timestamp;
	if (*(ts_ptr-1) > FLAG_DELETE_CELL && temp_timestamp <= min_timestamp) {
	  error = Error::RANGESERVER_TIMESTAMP_ORDER_ERROR;
	  errMsg = (string)"Update timestamp " + temp_timestamp + " is <= previously seen timestamp of " + min_timestamp;
	  min_ts_rec.range_ptr->decrement_update_counter();
	  boost::detail::thread::lock_ops<boost::mutex>::unlock(m_update_mutex_a);
	  goto abort;
	}
	if (min_ts_rec.timestamp.logical == 0 || temp_timestamp < min_ts_rec.timestamp.logical)
	  min_ts_rec.timestamp.logical = temp_timestamp;
      }

      update.base = mod_ptr;
      mod_ptr += Length((const ByteString32T *)mod_ptr); // skip key
      mod_ptr += Length((const ByteString32T *)mod_ptr); // skip value
      update.len = mod_ptr - update.base;
      if (split_pending && strcmp(row, split_row.c_str()) <= 0) {
	splitMods.push_back(update);
	splitSize += update.len;
      }
      else {
	goMods.push_back(update);
	goSize += update.len;
      }
      row = (const char *)((ByteString32T *)mod_ptr)->data;
    }

    add_end_ptr = mod_ptr;

    //HT_INFOF("fugazi TIMESTAMP --last-- %lld", next_timestamp);

    // force scans to only see updates before the earliest time in this range
    min_ts_rec.timestamp.logical--;
    min_ts_rec.range_ptr->add_update_timestamp( min_ts_rec.timestamp );
    min_ts_vector.push_back(min_ts_rec);

    if (splitSize > 0) {
      boost::shared_array<uint8_t> bufPtr( new uint8_t [ splitSize + table->encoded_length() ] );
      uint8_t *base = bufPtr.get();
      uint8_t *ptr = base;

      table->encode(&ptr);

      items_added += splitMods.size();
      memory_added += splitSize;

      for (size_t i=0; i<splitMods.size(); i++) {
	memcpy(ptr, splitMods[i].base, splitMods[i].len);
	ptr += splitMods[i].len;
      }

      HT_EXPECT((ptr-base) <= (long)(splitSize + table->encoded_length()), Error::FAILED_EXPECTATION);

      if ((error = splitLogPtr->write(base, ptr-base, update_timestamp)) != Error::OK) {
	errMsg = (string)"Problem writing " + (int)(ptr-base) + " bytes to split log";
	boost::detail::thread::lock_ops<boost::mutex>::unlock(m_update_mutex_a);
	goto abort;
      }
    }

    /**
     * Apply the modifications
     */
    min_ts_rec.range_ptr->lock();
    {
      const uint8_t *ptr = add_base_ptr;
      const ByteString32T *key, *value;
      while (ptr < add_end_ptr) {
	key = (const ByteString32T *)ptr;
	ptr += Length(key);
	value = (const ByteString32T *)ptr;
	ptr += Length(value);
	HT_EXPECT(min_ts_rec.range_ptr->add(key, value, update_timestamp) == Error::OK, Error::FAILED_EXPECTATION);
      }
    }
    min_ts_rec.range_ptr->unlock(update_timestamp);

    /**
     * Split and Compaction processing
     */
    {
      std::vector<AccessGroup::CompactionPriorityDataT> priority_data_vec;
      std::vector<AccessGroup *> compactions;
      uint64_t disk_usage = 0;

      min_ts_rec.range_ptr->get_compaction_priority_data(priority_data_vec);
      for (size_t i=0; i<priority_data_vec.size(); i++) {
	disk_usage += priority_data_vec[i].disk_used;
	if (!priority_data_vec[i].in_memory && priority_data_vec[i].mem_used >= (uint32_t)Global::localityGroupMaxMemory)
	  compactions.push_back(priority_data_vec[i].ag);
      }

      if (!min_ts_rec.range_ptr->is_root() &&
	  (disk_usage > min_ts_rec.range_ptr->get_size_limit() || 
	   (Global::range_metadata_max_bytes && table->id == 0 && disk_usage > Global::range_metadata_max_bytes))) {
	if (!min_ts_rec.range_ptr->test_and_set_maintenance())
	  Global::maintenance_queue->add( new MaintenanceTaskSplit(min_ts_rec.range_ptr) );
      }
      else if (!compactions.empty()) {
	if (!min_ts_rec.range_ptr->test_and_set_maintenance()) {
	  for (size_t i=0; i<compactions.size(); i++)
	    compactions[i]->set_compaction_bit();
	  Global::maintenance_queue->add( new MaintenanceTaskCompaction(min_ts_rec.range_ptr, false) );
	}
      }
    }

    if (Global::verbose) {
      HT_INFOF("Added %d (%d split off) updates to '%s'", goMods.size()+splitMods.size(), splitMods.size(), table->name);
    }
  }

  boost::detail::thread::lock_ops<boost::mutex>::lock(m_update_mutex_b);
  boost::detail::thread::lock_ops<boost::mutex>::unlock(m_update_mutex_a);

  /**
   * Commit valid (go) mutations
   */
  if (goSize > 0) {
    boost::shared_array<uint8_t> bufPtr( new uint8_t [ goSize + table->encoded_length() ] );
    uint8_t *base = bufPtr.get();
    uint8_t *ptr = base;

    table->encode(&ptr);

    items_added += goMods.size();
    memory_added += goSize;

    for (size_t i=0; i<goMods.size(); i++) {
      memcpy(ptr, goMods[i].base, goMods[i].len);
      ptr += goMods[i].len;
    }

    HT_EXPECT((ptr-base) <= (long)(goSize + table->encoded_length()), Error::FAILED_EXPECTATION);

    if ((error = Global::log->write(base, ptr-base, update_timestamp)) != Error::OK) {
      errMsg = (string)"Problem writing " + (int)(ptr-base) + " bytes to commit log";
      boost::detail::thread::lock_ops<boost::mutex>::unlock(m_update_mutex_b);
      goto abort;
    }
  }

  boost::detail::thread::lock_ops<boost::mutex>::unlock(m_update_mutex_b);

  if (Global::verbose) {
    HT_INFOF("Dropped %d updates (out-of-range)", stopMods.size());
  }

  error = Error::OK;

 abort:

  Global::memory_tracker.add_memory(memory_added);
  Global::memory_tracker.add_items(items_added);

  {
    uint64_t mt_memory = Global::memory_tracker.get_memory();
    uint64_t mt_items = Global::memory_tracker.get_items();
    uint64_t vm_estimate = mt_memory + (mt_items*120);
    HT_INFOF("drj mem=%lld items=%lld vm-est%lld", mt_memory, mt_items, vm_estimate);
  }

  splitLogPtr = 0;

  // unblock scanner timestamp and decrement update counter
  for (size_t i=0; i<min_ts_vector.size(); i++) {
    min_ts_vector[i].range_ptr->remove_update_timestamp(min_ts_vector[i].timestamp);
    min_ts_vector[i].range_ptr->decrement_update_counter();
  }

  if (error == Error::OK) {
    /**
     * Send back response
     */
    if (stopSize > 0) {
      ExtBufferT ext;
      ext.buf = new uint8_t [ stopSize ];
      uint8_t *ptr = ext.buf;
      for (size_t i=0; i<stopMods.size(); i++) {
	memcpy(ptr, stopMods[i].base, stopMods[i].len);
	ptr += stopMods[i].len;
      }
      ext.len = ptr - ext.buf;
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
    HT_ERRORF("%s '%s'", Error::get_text(error), errMsg.c_str());
    if ((error = cb->error(error, errMsg)) != Error::OK) {
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
    }
  }
}



void RangeServer::drop_table(ResponseCallback *cb, TableIdentifier *table) {
  TableInfoPtr table_info_ptr;
  std::vector<RangePtr> range_vector;
  std::string metadata_prefix;
  std::string metadata_key; // ***********  = std::string("") + (uint32_t)m_table_identifier.id + ":" + m_end_row;
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
      metadata_prefix = std::string("") + table_info_ptr->get_id() + ":";
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
void RangeServer::replay_start(ResponseCallback *cb) {
  String replay_log_dir = format("/hypertable/servers/%s/log/replay",
                                 m_location.c_str());
  if (Global::verbose) {
    HT_INFO("replay_start");
    cout << flush;
  }

  m_replay_log_ptr = 0;

  m_replay_map_ptr->clear_ranges();

  /**
   * Remove old replay log directory
   */
  try { Global::logDfs->rmdir(replay_log_dir); }
  catch (Exception &e) {
    HT_ERRORF("Problem removing replay log directory: %s", e.what());
    cb->error(e.code(), format("Problem removing replay log directory: %s",
              e.what()));
    return;
  }

  /**
   * Create new replay log directory
   */
  try { Global::logDfs->mkdirs(replay_log_dir); }
  catch (Exception &e) {
    HT_ERRORF("Problem creating replay log directory: %s ", e.what());
    cb->error(e.code(), format("Problem creating replay log directory: %s",
              e.what()));
    return;
  }

  m_replay_log_ptr = new CommitLog(Global::logDfs, replay_log_dir, m_props_ptr);

  cb->response_ok();
}


void RangeServer::replay_update(ResponseCallback *cb, const uint8_t *data, size_t len) {
  if (Global::verbose) {
    HT_INFOF("replay_update - length=%ld", len);
    cout << flush;
  }
  (void)data;
  (void)len;
  cb->response_ok();
}


void RangeServer::replay_commit(ResponseCallback *cb) {

  HT_EXPECT(m_replay_log_ptr, Error::FAILED_EXPECTATION);

  if (Global::verbose) {
    HT_INFO("replay_commit");
    cout << flush;
  }

  try {
    m_live_map_ptr->atomic_merge(m_replay_map_ptr, m_replay_log_ptr.get());
  }
  catch (Hypertable::Exception &e) {
    HT_ERRORF("%s - %s", e.what(), Error::get_text(e.code()));
    cb->error(e.code(), e.what());
    return;
  }

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
    cb->error(Error::RANGESERVER_RANGE_NOT_FOUND, std::string("No ranges loaded for table '") + table->name + "'");
    return;
  }

  /** Remove the range **/
  if (!table_info_ptr->remove_range(range, range_ptr)) {
    cb->error(Error::RANGESERVER_RANGE_NOT_FOUND, (std::string)table->name + "[" + range->startRow + ".." + range->endRow + "]");
    return;
  }

  cb->response_ok();
}



int RangeServer::verify_schema(TableInfoPtr &table_info_ptr, int generation, std::string &errMsg) {
  std::string tableFile = (std::string)"/hypertable/tables/" + table_info_ptr->get_name();
  DynamicBuffer valueBuf(0);
  HandleCallbackPtr nullHandleCallback;
  int error;
  uint64_t handle;
  SchemaPtr schemaPtr = table_info_ptr->get_schema();

  if (schemaPtr.get() == 0 || schemaPtr->get_generation() < generation) {

    if ((error = m_hyperspace_ptr->open(tableFile.c_str(), OPEN_FLAG_READ, nullHandleCallback, &handle)) != Error::OK) {
      HT_ERRORF("Unable to open Hyperspace file '%s' (%s)", tableFile.c_str(), Error::get_text(error));
      exit(1);
    }

    if ((error = m_hyperspace_ptr->attr_get(handle, "schema", valueBuf)) != Error::OK) {
      errMsg = (std::string)"Problem getting 'schema' attribute for '" + tableFile + "'";
      return error;
    }

    m_hyperspace_ptr->close(handle);

    schemaPtr = Schema::new_instance((const char *)valueBuf.buf, valueBuf.fill(), true);
    if (!schemaPtr->is_valid()) {
      errMsg = (std::string)"Schema Parse Error for table '" + table_info_ptr->get_name() + "' : " + schemaPtr->get_error_string();
      return Error::RANGESERVER_SCHEMA_PARSE_ERROR;
    }

    table_info_ptr->update_schema(schemaPtr);

    // Generation check ...
    if ( schemaPtr->get_generation() < generation ) {
      errMsg = (std::string)"Fetched Schema generation for table '" + table_info_ptr->get_name() + "' is " + schemaPtr->get_generation() + " but supplied is " + generation;
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
  Global::scannerMap.purge_expired(m_scanner_ttl);

  /**
   * Schedule log cleanup
   */
  gettimeofday(&tval, 0);
  if ((tval.tv_sec - m_last_commit_log_clean) >= (int)(m_timer_interval*4)/5) {
    // schedule log cleanup
    Global::maintenance_queue->add( new MaintenanceTaskLogCleanup(this) );
    m_last_commit_log_clean = tval.tv_sec;
  }
}



/**
 *
 */
void RangeServer::log_cleanup() {
  std::vector<TableInfoPtr> table_vec;
  std::vector<RangePtr> range_vec;
  std::vector<AccessGroup::CompactionPriorityDataT> priority_data_vec;
  LogFragmentPriorityMap log_frag_map;
  std::set<size_t> compaction_set;
  uint64_t timestamp, oldest_cached_timestamp = 0;
  uint64_t prune_threshold = 2 * Global::log->get_max_fragment_size();

  m_live_map_ptr->get_all(table_vec);
  for (size_t i=0; i<table_vec.size(); i++)
    table_vec[i]->get_range_vector(range_vec);

  /**
   * Load up a vector of compaction priority data
   */
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

  Global::log->load_fragment_priority_map(log_frag_map);

  /**
   * Determine which AGs need compaction for the sake of
   * garbage collecting commit log fragments
   */
  for (size_t i=0; i<priority_data_vec.size(); i++) {
    LogFragmentPriorityMap::iterator map_iter = log_frag_map.lower_bound( priority_data_vec[i].oldest_cached_timestamp );
    size_t rangei = (size_t)priority_data_vec[i].user_data;

    // this should never happen
    if (map_iter == log_frag_map.end())
      continue;

    if ( priority_data_vec[i].oldest_cached_timestamp > 0 && (*map_iter).second.cumulative_size > prune_threshold ) {
      std::string range_str = range_vec[rangei]->table_name() + "[" + range_vec[rangei]->start_row() + ".." + range_vec[rangei]->end_row() + "]." + priority_data_vec[i].ag->get_name();
      priority_data_vec[i].ag->set_compaction_bit();
      compaction_set.insert( rangei );
      HT_INFOF("Compacting %s because cumulative log size of %lld exceeds threshold %lld",
		  range_str.c_str(), (*map_iter).second.cumulative_size, prune_threshold);
    }
  }

  /**
   * Schedule the compactions
   */
  for (std::set<size_t>::iterator iter = compaction_set.begin(); iter != compaction_set.end(); iter++) {
    if (!range_vec[*iter]->test_and_set_maintenance())
      Global::maintenance_queue->add( new MaintenanceTaskCompaction(range_vec[*iter], false) );
  }

  /**
   * Purge the commit log
   */
  if (oldest_cached_timestamp != 0)
    Global::log->purge(oldest_cached_timestamp);

}



/**
 */
uint64_t RangeServer::get_timer_interval() {
  return m_timer_interval*1000;
}
