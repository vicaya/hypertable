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

#include <cassert>
#include <string>

#include <boost/shared_array.hpp>

#include "Common/ByteOrder.h"
#include "Common/FileUtils.h"
#include "Common/md5.h"
#include "Common/StringExt.h"
#include "Common/System.h"

#include "Hypertable/Lib/RangeServerProtocol.h"

#include "DfsBroker/Lib/Client.h"

#include "CommitLog.h"
#include "FillScanBlock.h"
#include "Global.h"
#include "HandlerFactory.h"
#include "HyperspaceSessionHandler.h"
#include "MaintenanceThread.h"
#include "RangeServer.h"
#include "ScanContext.h"

using namespace Hypertable;
using namespace std;

namespace {
  const int DEFAULT_SCANBUF_SIZE = 32768;
  const int DEFAULT_WORKERS = 20;
  const int DEFAULT_PORT    = 38060;
}


/**
 * Constructor
 */
RangeServer::RangeServer(Comm *comm, PropertiesPtr &propsPtr) : m_mutex(), m_verbose(false), m_comm(comm) {
  int workerCount;
  int error;
  uint16_t port;

  Global::rangeMaxBytes           = propsPtr->getPropertyInt64("Hypertable.RangeServer.Range.MaxBytes", 200000000LL);
  Global::localityGroupMaxFiles   = propsPtr->getPropertyInt("Hypertable.RangeServer.AccessGroup.MaxFiles", 10);
  Global::localityGroupMergeFiles = propsPtr->getPropertyInt("Hypertable.RangeServer.AccessGroup.MergeFiles", 4);
  Global::localityGroupMaxMemory  = propsPtr->getPropertyInt("Hypertable.RangeServer.AccessGroup.MaxMemory", 4000000);
  workerCount                     = propsPtr->getPropertyInt("Hypertable.RangeServer.workers", DEFAULT_WORKERS);
  port                            = propsPtr->getPropertyInt("Hypertable.RangeServer.port", DEFAULT_PORT);

  uint64_t blockCacheMemory = propsPtr->getPropertyInt64("Hypertable.RangeServer.BlockCache.MaxMemory", 200000000LL);
  Global::blockCache = new FileBlockCache(blockCacheMemory);

  assert(Global::localityGroupMergeFiles <= Global::localityGroupMaxFiles);

  m_verbose = propsPtr->getPropertyBool("verbose", false);

  if (Global::verbose) {
    cout << "Hypertable.RangeServer.AccessGroup.MaxFiles=" << Global::localityGroupMaxFiles << endl;
    cout << "Hypertable.RangeServer.AccessGroup.MaxMemory=" << Global::localityGroupMaxMemory << endl;
    cout << "Hypertable.RangeServer.AccessGroup.MergeFiles=" << Global::localityGroupMergeFiles << endl;
    cout << "Hypertable.RangeServer.BlockCache.MaxMemory=" << blockCacheMemory << endl;
    cout << "Hypertable.RangeServer.Range.MaxBytes=" << Global::rangeMaxBytes << endl;
    cout << "Hypertable.RangeServer.port=" << port << endl;
    cout << "Hypertable.RangeServer.workers=" << workerCount << endl;
  }

  m_conn_manager_ptr = new ConnectionManager(comm);
  m_app_queue_ptr = new ApplicationQueue(workerCount);

  Global::protocol = new Hypertable::RangeServerProtocol();

  /**
   * Connect to Hyperspace
   */
  m_hyperspace_ptr = new Hyperspace::Session(comm, propsPtr, new HyperspaceSessionHandler(this));
  if (!m_hyperspace_ptr->wait_for_connection(30)) {
    LOG_ERROR("Unable to connect to hyperspace, exiting...");
    exit(1);
  }

  DfsBroker::Client *dfsClient = new DfsBroker::Client(m_conn_manager_ptr, propsPtr);

  if (m_verbose) {
    cout << "DfsBroker.host=" << propsPtr->getProperty("DfsBroker.host", "") << endl;
    cout << "DfsBroker.port=" << propsPtr->getProperty("DfsBroker.port", "") << endl;
    cout << "DfsBroker.timeout=" << propsPtr->getProperty("DfsBroker.timeout", "") << endl;
  }

  if (!dfsClient->wait_for_connection(30)) {
    LOG_ERROR("Unable to connect to DFS Broker, exiting...");
    exit(1);
  }

  Global::dfs = dfsClient;

  /**
   * Check for and connect to commit log DFS broker
   */
  {
    const char *logHost = propsPtr->getProperty("Hypertable.RangeServer.CommitLog.DfsBroker.host", 0);
    uint16_t logPort    = propsPtr->getPropertyInt("Hypertable.RangeServer.CommitLog.DfsBroker.port", 0);
    struct sockaddr_in addr;
    if (logHost != 0) {
      InetAddr::initialize(&addr, logHost, logPort);
      dfsClient = new DfsBroker::Client(m_conn_manager_ptr, addr, 30);
      if (!dfsClient->wait_for_connection(30)) {
	LOG_ERROR("Unable to connect to DFS Broker, exiting...");
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
    struct timeval tval;
    std::string hostStr;
    struct sockaddr_in addr;    

    InetAddr::get_hostname(hostStr);
    InetAddr::initialize(&addr, hostStr.c_str(), port);

    gettimeofday(&tval, 0);

    m_location = (std::string)inet_ntoa(addr.sin_addr) + "_" + (int)port + "_" + (uint32_t)tval.tv_sec;
  }

  if (directory_initialize(propsPtr.get()) != Error::OK)
    exit(1);

  // start commpaction thread
  {
    MaintenanceThread maintenanceThread;
    Global::maintenanceThreadPtr = new boost::thread(maintenanceThread);
  }


  /**
   * Listen for incoming connections
   */
  {
    ConnectionHandlerFactoryPtr chfPtr(new HandlerFactory(comm, m_app_queue_ptr, this));
    struct sockaddr_in addr;
    InetAddr::initialize(&addr, INADDR_ANY, port);  // Listen on any interface
    if ((error = comm->listen(addr, chfPtr)) != Error::OK) {
      LOG_VA_ERROR("Listen error address=%s:%d - %s", inet_ntoa(addr.sin_addr), port, Error::get_text(error));
      exit(1);
    }
  }

  /**
   * Open METADATA table
   */
  Global::metadata_table_ptr = new Table(m_conn_manager_ptr, m_hyperspace_ptr, "METADATA");

  /**
   * Create Master client
   */
  m_master_client_ptr = new MasterClient(m_conn_manager_ptr, m_hyperspace_ptr, 30, m_app_queue_ptr);
  m_master_connection_handler = new ConnectionHandler(m_comm, m_app_queue_ptr, this, m_master_client_ptr);
  m_master_client_ptr->initiate_connection(m_master_connection_handler);

}


/**
 *
 */
RangeServer::~RangeServer() {
  m_app_queue_ptr->shutdown();
}



/**
 * - Figure out and create range server directory
 * - Clear any Range server state (including any partially created commit logs)
 * - Open the commit log
 */
int RangeServer::directory_initialize(Properties *props) {
  int error;
  bool exists;
  std::string topDir;

  /**
   * Create /hypertable/servers directory
   */
  if ((error = m_hyperspace_ptr->exists("/hypertable/servers", &exists)) != Error::OK) {
    LOG_VA_ERROR("Problem checking existence of '/hypertable/servers' Hyperspace directory - %s", Error::get_text(error));
    return error;
  }

  if (!exists) {
    LOG_ERROR("Hyperspace directory '/hypertable/servers' does not exist, try running 'Hypertable.master --initialize' first");
    return error;
  }

  topDir = (std::string)"/hypertable/servers/" + m_location;

  /**
   * Create "server existence" file in Hyperspace and obtain an exclusive lock on it
   */

  uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_CREATE | OPEN_FLAG_EXCL | OPEN_FLAG_LOCK_EXCLUSIVE;
  HandleCallbackPtr nullCallbackPtr;

  if ((error = m_hyperspace_ptr->open(topDir.c_str(), oflags, nullCallbackPtr, &m_existence_file_handle)) != Error::OK) {
    LOG_VA_ERROR("Problem creating Hyperspace server existance file '%s' - %s", topDir.c_str(), Error::get_text(error));
    exit(1);
  }

  if ((error = m_hyperspace_ptr->get_sequencer(m_existence_file_handle, &m_existence_file_sequencer)) != Error::OK) {
    LOG_VA_ERROR("Problem obtaining lock sequencer for file '%s' - %s", topDir.c_str(), Error::get_text(error));
    exit(1);
  }

  Global::logDir = (string)topDir.c_str() + "/commit/primary";

  /**
   * Create /hypertable/servers/X.X.X.X_port_nnnnn/commit/primary directory
   */
  if ((error = Global::logDfs->mkdirs(Global::logDir)) != Error::OK) {
    LOG_VA_ERROR("Problem creating local log directory '%s'", Global::logDir.c_str());
    return error;
  }

  int64_t logFileSize = props->getPropertyInt64("Hypertable.RangeServer.logFileSize", 0x100000000LL);
  if (Global::verbose) {
    cout << "Hypertable.RangeServer.logFileSize=" << logFileSize << endl;
    cout << "logDir=" << Global::logDir << endl;
  }

  Global::log = new CommitLog(Global::logDfs, Global::logDir, logFileSize);    

  return Error::OK;
}



/**
 * Compact
 */
void RangeServer::compact(ResponseCallback *cb, TableIdentifierT *table, RangeT *range, uint8_t compaction_type) {
  int error = Error::OK;
  std::string errMsg;
  TableInfoPtr tableInfoPtr;
  RangePtr rangePtr;
  MaintenanceThread::WorkType workType = MaintenanceThread::COMPACTION_MINOR;
  bool major = false;

  // Check for major compaction
  if (compaction_type == 1) {
    major = true;
    workType = MaintenanceThread::COMPACTION_MAJOR;
  }

  if (Global::verbose) {
    cout << *table;
    cout << *range;
    cout << "Compaction type = " << (major ? "major" : "minor") << endl;
  }

  /**
   * Fetch table info
   */
  if (!get_table_info(table->name, tableInfoPtr)) {
    error = Error::RANGESERVER_RANGE_NOT_FOUND;
    errMsg = "No ranges loaded for table '" + (std::string)table->name + "'";
    goto abort;
  }

  /**
   * Fetch range info
   */
  if (!tableInfoPtr->get_range(range, rangePtr)) {
    error = Error::RANGESERVER_RANGE_NOT_FOUND;
    errMsg = (std::string)table->name + "[" + range->startRow + ".." + range->endRow + "]";
    goto abort;
  }

  MaintenanceThread::schedule_compaction(rangePtr.get(), workType);

  if ((error = cb->response_ok()) != Error::OK) {
    LOG_VA_ERROR("Problem sending OK response - %s", Error::get_text(error));
  }

  if (Global::verbose) {
    LOG_VA_INFO("Compaction (%s) scheduled for table '%s' end row '%s'",
		(major ? "major" : "minor"), table->name, range->endRow);
  }

  error = Error::OK;

 abort:
  if (error != Error::OK) {
    LOG_VA_ERROR("%s '%s'", Error::get_text(error), errMsg.c_str());
    if ((error = cb->error(error, errMsg)) != Error::OK) {
      LOG_VA_ERROR("Problem sending error response - %s", Error::get_text(error));
    }
  }
}



/** 
 *  CreateScanner
 */
void RangeServer::create_scanner(ResponseCallbackCreateScanner *cb, TableIdentifierT *table, RangeT *range, ScanSpecificationT *scan_spec) {
  uint8_t *kvBuffer = 0;
  uint32_t *kvLenp = 0;
  int error = Error::OK;
  std::string errMsg;
  TableInfoPtr tableInfoPtr;
  RangePtr rangePtr;
  CellListScannerPtr scannerPtr;
  bool more = true;
  uint32_t id;
  uint64_t scan_timestamp;
  SchemaPtr schemaPtr;
  ScanContextPtr scanContextPtr;

  if (Global::verbose) {
    cout << "RangeServer::create_scanner" << endl;
    cout << *table;
    cout << *range;
    cout << *scan_spec;
  }

  if (!get_table_info(table->name, tableInfoPtr)) {
    error = Error::RANGESERVER_RANGE_NOT_FOUND;
    errMsg = (std::string)table->name + "[" + range->startRow + ".." + range->endRow + "]";
    goto abort;
  }

  if (!tableInfoPtr->get_range(range, rangePtr)) {
    error = Error::RANGESERVER_RANGE_NOT_FOUND;
    errMsg = (std::string)table->name + "[" + range->startRow + ".." + range->endRow + "]";
    goto abort;
  }

  schemaPtr = tableInfoPtr->get_schema();

  scan_timestamp = rangePtr->get_timestamp() + 1;

  scanContextPtr = new ScanContext(scan_timestamp, scan_spec, range, schemaPtr);
  if (scanContextPtr->error != Error::OK) {
    errMsg = "Problem initializing scan context";
    goto abort;
  }
 
  scannerPtr = rangePtr->create_scanner(scanContextPtr);

  // TODO: fix this kludge (0 return above means range split)
  if (!scannerPtr) {
    error = Error::RANGESERVER_RANGE_NOT_FOUND;
    errMsg = (std::string)table->name + "[" + range->startRow + ".." + range->endRow + "]";
    goto abort;
  }

  kvBuffer = new uint8_t [ sizeof(int32_t) + DEFAULT_SCANBUF_SIZE ];
  kvLenp = (uint32_t *)kvBuffer;

  more = FillScanBlock(scannerPtr, kvBuffer+sizeof(int32_t), DEFAULT_SCANBUF_SIZE, kvLenp);
  if (more)
    id = Global::scannerMap.put(scannerPtr, rangePtr);
  else
    id = 0;

  if (Global::verbose) {
    LOG_VA_INFO("Successfully created scanner on table '%s'", table->name);
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
      LOG_VA_ERROR("Problem sending OK response - %s", Error::get_text(error));
    }
  }

  error = Error::OK;

 abort:
  if (error != Error::OK) {
    LOG_VA_ERROR("%s '%s'", Error::get_text(error), errMsg.c_str());
    if ((error = cb->error(error, errMsg)) != Error::OK) {
      LOG_VA_ERROR("Problem sending error response - %s", Error::get_text(error));
    }
  }
}


void RangeServer::fetch_scanblock(ResponseCallbackFetchScanblock *cb, uint32_t scannerId) {
  string errMsg;
  int error = Error::OK;
  CellListScannerPtr scannerPtr;
  RangePtr rangePtr;
  bool more = true;
  uint8_t *kvBuffer = 0;
  uint32_t *kvLenp = 0;
  uint32_t bytesFetched = 0;

  if (Global::verbose) {
    cout << "RangeServer::fetch_scanblock" << endl;
    cout << "Scanner ID = " << scannerId << endl;
  }

  if (!Global::scannerMap.get(scannerId, scannerPtr, rangePtr)) {
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
      LOG_VA_ERROR("Problem sending OK response - %s", Error::get_text(error));
    }
  }

  if (Global::verbose) {
    LOG_VA_INFO("Successfully fetched %d bytes of scan data", bytesFetched);
  }

  error = Error::OK;

 abort:
  if (error != Error::OK) {
    LOG_VA_ERROR("%s '%s'", Error::get_text(error), errMsg.c_str());
    if ((error = cb->error(error, errMsg)) != Error::OK) {
      LOG_VA_ERROR("Problem sending error response - %s", Error::get_text(error));
    }
  }
}


/**
 * LoadRange
 */
void RangeServer::load_range(ResponseCallback *cb, TableIdentifierT *table, RangeT *range, uint16_t flags) {
  DynamicBuffer endRowBuffer(0);
  std::string errMsg;
  int error = Error::OK;
  SchemaPtr schemaPtr;
  TableInfoPtr tableInfoPtr;
  RangePtr rangePtr;
  string tableHdfsDir;
  string rangeHdfsDir;
  char md5DigestStr[33];
  bool registerTable = false;
  bool is_root = !strcmp(table->name, "METADATA") && (*range->startRow == 0) && !strcmp(range->endRow, Key::END_ROOT_ROW);
  TableScannerPtr scanner_ptr;
  MutatorPtr mutator_ptr;
  ScanSpecificationT scan_spec;
  CellT cell;
  KeySpec key;
  std::string metadata_key_str;
  std::string start_row, split_log_dir;

  if (Global::verbose) {
    cout << "load_range" << endl;
    cout << *table;
    cout << *range;
    cout << "flags = " << flags << endl;
    cout << flush;
  }

  /**
   * Get TableInfo, create if doesn't exist
   */
  if (!get_table_info(table->name, tableInfoPtr)) {
    tableInfoPtr = new TableInfo(m_master_client_ptr, table, schemaPtr);
    registerTable = true;
  }

  /**
   * Verify schema, this will create the Schema object and add it to tableInfoPtr if it doesn't exist
   */
  if ((error = verify_schema(tableInfoPtr, table->generation, errMsg)) != Error::OK)
    goto abort;

  if (registerTable)
    set_table_info(table->name, tableInfoPtr);

  /**
   * Make sure this range is not already loaded
   */
  if (tableInfoPtr->get_range(range, rangePtr)) {
    error = Error::RANGESERVER_RANGE_ALREADY_LOADED;
    errMsg = (std::string)table->name + "[" + range->startRow + ".." + range->endRow + "]";
    goto abort;
  }

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
    if ((error = Global::metadata_table_ptr->create_mutator(mutator_ptr)) != Error::OK) {
      errMsg = "Problem creating mutator on METADATA table";
      goto abort;
    }
    key.row = metadata_key_str.c_str();
    key.row_len = strlen(metadata_key_str.c_str());
    key.column_family = "Location";
    key.column_qualifier = 0;
    key.column_qualifier_len = 0;
    try {
      mutator_ptr->set(0, key, (uint8_t *)m_location.c_str(), strlen(m_location.c_str()));
      mutator_ptr->flush();
    }
    catch (Hypertable::Exception &e) {
      error = e.code();
      errMsg = e.what();
      goto abort;
    }

    bool got_start_row = false;

    scan_spec.rowLimit = 1;
    scan_spec.max_versions = 1;
    scan_spec.startRow = metadata_key_str.c_str();
    scan_spec.startRowInclusive = true;
    scan_spec.endRow = metadata_key_str.c_str();
    scan_spec.endRowInclusive = true;
    scan_spec.interval.first = 0;
    scan_spec.interval.second = 0;

    scan_spec.columns.clear();
    scan_spec.columns.push_back("StartRow");
    scan_spec.columns.push_back("SplitLogDir");

    if ((error = Global::metadata_table_ptr->create_scanner(scan_spec, scanner_ptr)) != Error::OK) {
      errMsg = "Problem creating scanner on METADATA table";
      goto abort;
    }

    try {
      while (scanner_ptr->next(cell)) {
	if (!strcmp(cell.column_family, "StartRow")) {
	  if (cell.value_len > 0)
	    start_row = std::string((const char *)cell.value, cell.value_len);
	  got_start_row = true;
	}
	else if (!strcmp(cell.column_family, "SplitLogDir")) {
	  if (cell.value_len > 0)
	    split_log_dir = std::string((const char *)cell.value, cell.value_len);
	}
	else {
	  // should never happen
	  LOG_ERROR("Scanner returned column not requested.");
	}
      }
    }
    catch (Hypertable::Exception &e) {
      LOG_VA_ERROR("Caught exception in load_range trying to scan METADATA table (start_row,end_row=%s) - %s",
		   metadata_key_str.c_str(), e.what());
      error = e.code();
      errMsg = e.what();
      goto abort;
    }

    if (!got_start_row) {
      error = Error::RANGESERVER_NO_METADATA_FOR_RANGE;
      errMsg = (std::string)"StartRow for METADATA[" + metadata_key_str + "] not found";
      goto abort;
    }

    // make sure StartRow matches
    if (*range->startRow == 0 && start_row != "" || strcmp(range->startRow, start_row.c_str())) {
      error = Error::RANGESERVER_RANGE_MISMATCH;
      errMsg = (std::string)"StartRow '" + std::string((const char *)cell.value, cell.value_len) + "' does not match '" + range->startRow + "'";
      goto abort;
    }

  }
  else {  //root
    uint64_t handle;
    HandleCallbackPtr nullCallbackPtr;
    uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_CREATE;

    LOG_INFO("Loading root METADATA range");

    if ((error = m_hyperspace_ptr->open("/hypertable/root", oflags, nullCallbackPtr, &handle)) != Error::OK) {
      LOG_VA_ERROR("Problem creating Hyperspace root file '/hypertable/root' - %s", Error::get_text(error));
      DUMP_CORE;
    }

    if ((error = m_hyperspace_ptr->attr_set(handle, "location", m_location.c_str(), strlen(m_location.c_str()))) != Error::OK) {
      LOG_VA_ERROR("Problem creating attribute 'location' on Hyperspace file '/hypertable/root' - %s", Error::get_text(error));
      DUMP_CORE;
    }

    m_hyperspace_ptr->close(handle);
  }

  schemaPtr = tableInfoPtr->get_schema();

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
      if ((error = Global::dfs->mkdirs(rangeHdfsDir)) != Error::OK) {
	errMsg = (string)"Problem creating range directory '" + rangeHdfsDir + "'";
	goto abort;
      }
    }
  }

  tableInfoPtr->add_range(range, rangePtr);

  // TODO: if (flags & RangeServerProtocol::LOAD_RANGE_FLAG_PHANTOM), do the following in 'go live'
  if (split_log_dir != "") {
    uint64_t timestamp = Global::log->get_timestamp();
    if ((error = Global::log->link_log(table->name, split_log_dir.c_str(), timestamp)) != Error::OK) {
      errMsg = (string)"Unable to link external log '" + split_log_dir + "' into commit log";
      goto abort;
    }
    if ((error = rangePtr->replay_split_log(split_log_dir)) != Error::OK) {
      errMsg = (string)"Problem replaying split log '" + split_log_dir + "'";
      goto abort;
    }
  }

  /**
   * Update LogDir
   */
  if (!is_root) {
    key.column_family = "LogDir";
    try {
      mutator_ptr->set(0, key, (uint8_t *)Global::log->get_log_dir().c_str(), strlen(Global::log->get_log_dir().c_str()));
      mutator_ptr->flush();
    }
    catch (Hypertable::Exception &e) {
      error = e.code();
      errMsg = e.what();
      goto abort;
    }
  }
  else {
    // TODO: update hyperspace
  }

  if ((error = cb->response_ok()) != Error::OK) {
    LOG_VA_ERROR("Problem sending OK response - %s", Error::get_text(error));
  }
  else {
    LOG_VA_INFO("Successfully loaded range %s[%s..%s]", table->name, range->startRow, range->endRow);
  }

  error = Error::OK;

 abort:
  if (error != Error::OK) {
    LOG_VA_ERROR("%s '%s'", Error::get_text(error), errMsg.c_str());
    if ((error = cb->error(error, errMsg)) != Error::OK) {
      LOG_VA_ERROR("Problem sending error response - %s", Error::get_text(error));
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
    uint64_t timestamp;
  } MinTimestampRecT;

}



/**
 * Update
 */
void RangeServer::update(ResponseCallbackUpdate *cb, TableIdentifierT *table, BufferT &buffer) {
  const uint8_t *modPtr;
  const uint8_t *modEnd;
  uint8_t *ts_ptr;
  const uint8_t auto_ts[8] = { 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff };
  string errMsg;
  int error = Error::OK;
  TableInfoPtr tableInfoPtr;
  uint64_t updateTimestamp = 0;
  uint64_t clientTimestamp = 0;
  const char *row;
  std::string split_row;
  vector<UpdateRecT> goMods;
  vector<UpdateRecT> splitMods;
  vector<UpdateRecT> stopMods;
  UpdateRecT update;
  ByteString32Ptr splitKeyPtr;
  CommitLogPtr splitLogPtr;
  uint64_t splitStartTime;
  size_t goSize = 0;
  size_t goBase = 0;
  size_t stopSize = 0;
  size_t splitSize = 0;
  std::string endRow;
  std::vector<MinTimestampRecT>  min_ts_vector;
  MinTimestampRecT  min_ts_rec;
  uint64_t next_timestamp;
  uint64_t temp_timestamp;

  min_ts_vector.reserve(50);

  if (Global::verbose) {
    cout << "RangeServer::update" << endl;
    cout << *table;
  }

  // TODO: Sanity check mod data (checksum validation)

  /**
   * Fetch table info
   */
  if (!get_table_info(table->name, tableInfoPtr)) {
    ExtBufferT ext;
    ext.buf = new uint8_t [ buffer.len ];
    memcpy(ext.buf, buffer.buf, buffer.len);
    ext.len = buffer.len;
    LOG_VA_ERROR("Unable to find table info for table '%s'", table->name);
    if ((error = cb->response(ext)) != Error::OK) {
      LOG_VA_ERROR("Problem sending OK response - %s", Error::get_text(error));
    }
    return;
  }

  // verify schema
  if ((error = verify_schema(tableInfoPtr, table->generation, errMsg)) != Error::OK)
    goto abort;

  modEnd = buffer.buf + buffer.len;
  modPtr = buffer.buf;

  goMods.clear();

  while (modPtr < modEnd) {

    row = (const char *)((ByteString32T *)modPtr)->data;

    if (!tableInfoPtr->find_containing_range(row, min_ts_rec.range_ptr)) {
      update.base = modPtr;
      modPtr += Length((const ByteString32T *)modPtr); // skip key
      modPtr += Length((const ByteString32T *)modPtr); // skip value
      update.len = modPtr - update.base;
      stopMods.push_back(update);
      stopSize += update.len;
      continue;
    }
    
    /** Increment update count (block if maintenance in progress) **/
    min_ts_rec.range_ptr->increment_update_counter();

    /** Obtain "update timestamp" **/
    updateTimestamp = Global::log->get_timestamp();

    endRow = min_ts_rec.range_ptr->end_row();

    /** Fetch range split information **/
    min_ts_rec.range_ptr->get_split_info(split_row, splitLogPtr, &splitStartTime);

    splitMods.clear();
    splitSize = 0;

    next_timestamp = 0;
    min_ts_rec.timestamp = 0;

    while (modPtr < modEnd && (endRow == "" || (strcmp(row, endRow.c_str()) <= 0))) {

      // If timestamp value is set to AUTO (zero one's compliment), then assign a timestamp
      ts_ptr = (uint8_t *)modPtr + Length((const ByteString32T *)modPtr) - 8;
      if (!memcmp(ts_ptr, auto_ts, 8)) {
	if (next_timestamp == 0) {
	  boost::xtime now;
	  boost::xtime_get(&now, boost::TIME_UTC);
	  next_timestamp = ((uint64_t)now.sec * 1000000000LL) + (uint64_t)now.nsec;
	}
	temp_timestamp = next_timestamp++;
	if (min_ts_rec.timestamp == 0 || temp_timestamp < min_ts_rec.timestamp)
	  min_ts_rec.timestamp = temp_timestamp;
	temp_timestamp = ByteOrderSwapInt64(temp_timestamp);
	temp_timestamp = ~temp_timestamp;
	memcpy(ts_ptr, &temp_timestamp, 8);
      }
      else {
	memcpy(&temp_timestamp, ts_ptr, 8);
	temp_timestamp = ByteOrderSwapInt64(temp_timestamp);
	temp_timestamp = ~temp_timestamp;
	if (min_ts_rec.timestamp == 0 || temp_timestamp < min_ts_rec.timestamp)
	  min_ts_rec.timestamp = temp_timestamp;
      }

      update.base = modPtr;
      modPtr += Length((const ByteString32T *)modPtr); // skip key
      modPtr += Length((const ByteString32T *)modPtr); // skip value
      update.len = modPtr - update.base;
      if (splitStartTime != 0 && updateTimestamp > splitStartTime && strcmp(row, split_row.c_str()) <= 0) {
	splitMods.push_back(update);
	splitSize += update.len;
      }
      else {
	goMods.push_back(update);
	goSize += update.len;
      }
      row = (const char *)((ByteString32T *)modPtr)->data;
    }

    // force scans to only see updates before the earliest time in this range
    min_ts_rec.timestamp--;
    min_ts_rec.range_ptr->add_update_timestamp( min_ts_rec.timestamp );
    min_ts_vector.push_back(min_ts_rec);

    if (splitSize > 0) {
      boost::shared_array<uint8_t> bufPtr( new uint8_t [ splitSize ] );
      uint8_t *base = bufPtr.get();
      uint8_t *ptr = base;
      
      for (size_t i=0; i<splitMods.size(); i++) {
	memcpy(ptr, splitMods[i].base, splitMods[i].len);
	ptr += splitMods[i].len;
      }

      if ((error = splitLogPtr->write(table->name, base, ptr-base, clientTimestamp)) != Error::OK) {
	errMsg = (string)"Problem writing " + (int)(ptr-base) + " bytes to split log";
	goto abort;
      }
    }

    /**
     * Apply the modifications
     */
    min_ts_rec.range_ptr->lock();
    /** Apply the GO mods **/
    for (size_t i=goBase; i<goMods.size(); i++) {
      ByteString32T *key = (ByteString32T *)goMods[i].base;
      ByteString32T *value = (ByteString32T *)(goMods[i].base + Length(key));
      min_ts_rec.range_ptr->add(key, value);
    }
    goBase = goMods.size();
    /** Apply the SPLIT mods **/
    for (size_t i=0; i<splitMods.size(); i++) {
      ByteString32T *key = (ByteString32T *)splitMods[i].base;
      ByteString32T *value = (ByteString32T *)(splitMods[i].base + Length(key));
      min_ts_rec.range_ptr->add(key, value);
    }
    min_ts_rec.range_ptr->unlock();

    /**
     * Split and Compaction processing
     */
    min_ts_rec.range_ptr->schedule_maintenance();

    if (Global::verbose) {
      LOG_VA_INFO("Added %d (%d split off) updates to '%s'", goMods.size()+splitMods.size(), splitMods.size(), table->name);
    }
  }

  /**
   * Commit valid (go) mutations
   */
  if (goSize > 0) {
    boost::shared_array<uint8_t> bufPtr( new uint8_t [ goSize ] );
    uint8_t *base = bufPtr.get();
    uint8_t *ptr = base;

    for (size_t i=0; i<goMods.size(); i++) {
      memcpy(ptr, goMods[i].base, goMods[i].len);
      ptr += goMods[i].len;
    }
    if ((error = Global::log->write(table->name, base, ptr-base, clientTimestamp)) != Error::OK) {
      errMsg = (string)"Problem writing " + (int)(ptr-base) + " bytes to commit log";
      goto abort;
    }
  }

  if (Global::verbose) {
    LOG_VA_INFO("Dropped %d updates (out-of-range)", stopMods.size());
  }

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
      LOG_VA_ERROR("Problem sending OK response - %s", Error::get_text(error));
    }
  }
  else {
    if ((error = cb->response_ok()) != Error::OK) {
      LOG_VA_ERROR("Problem sending OK response - %s", Error::get_text(error));
    }
  }

  error = Error::OK;

 abort:

  // unblock scanner timestamp and decrement update counter
  for (size_t i=0; i<min_ts_vector.size(); i++) {
    min_ts_vector[i].range_ptr->remove_update_timestamp(min_ts_vector[i].timestamp);
    min_ts_vector[i].range_ptr->decrement_update_counter();
  }

  if (error != Error::OK) {
    LOG_VA_ERROR("%s '%s'", Error::get_text(error), errMsg.c_str());
    if ((error = cb->error(error, errMsg)) != Error::OK) {
      LOG_VA_ERROR("Problem sending error response - %s", Error::get_text(error));
    }
  }
}



/**
 *
 */
bool RangeServer::get_table_info(std::string name, TableInfoPtr &info) {
  boost::mutex::scoped_lock lock(m_mutex);
  TableInfoMapT::iterator iter = m_table_info_map.find(name);
  if (iter == m_table_info_map.end())
    return false;
  info = (*iter).second;
  return true;
}


/**
 *
 */
void RangeServer::set_table_info(std::string name, TableInfoPtr &info) {
  boost::mutex::scoped_lock lock(m_mutex);
  TableInfoMapT::iterator iter = m_table_info_map.find(name);
  if (iter != m_table_info_map.end())
    m_table_info_map.erase(iter);
  m_table_info_map[name] = info;
}


int RangeServer::verify_schema(TableInfoPtr &tableInfoPtr, int generation, std::string &errMsg) {
  std::string tableFile = (std::string)"/hypertable/tables/" + tableInfoPtr->get_name();
  DynamicBuffer valueBuf(0);
  HandleCallbackPtr nullHandleCallback;
  int error;
  uint64_t handle;
  SchemaPtr schemaPtr = tableInfoPtr->get_schema();

  if (schemaPtr.get() == 0 || schemaPtr->get_generation() < generation) {

    if ((error = m_hyperspace_ptr->open(tableFile.c_str(), OPEN_FLAG_READ, nullHandleCallback, &handle)) != Error::OK) {
      LOG_VA_ERROR("Unable to open Hyperspace file '%s' (%s)", tableFile.c_str(), Error::get_text(error));
      exit(1);
    }

    if ((error = m_hyperspace_ptr->attr_get(handle, "schema", valueBuf)) != Error::OK) {
      errMsg = (std::string)"Problem getting 'schema' attribute for '" + tableFile + "'";
      return error;
    }

    m_hyperspace_ptr->close(handle);

    schemaPtr = Schema::new_instance((const char *)valueBuf.buf, valueBuf.fill(), true);
    if (!schemaPtr->is_valid()) {
      errMsg = (std::string)"Schema Parse Error for table '" + tableInfoPtr->get_name() + "' : " + schemaPtr->get_error_string();
      return Error::RANGESERVER_SCHEMA_PARSE_ERROR;
    }

    tableInfoPtr->update_schema(schemaPtr);

    // Generation check ...
    if ( schemaPtr->get_generation() < generation ) {
      errMsg = (std::string)"Fetched Schema generation for table '" + tableInfoPtr->get_name() + "' is " + schemaPtr->get_generation() + " but supplied is " + generation;
      return Error::RANGESERVER_GENERATION_MISMATCH;
    }
  }
  
  return Error::OK;
}
