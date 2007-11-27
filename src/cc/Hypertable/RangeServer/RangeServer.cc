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
  const char *metadataFile = 0;
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

  metadataFile = propsPtr->getProperty("metadata");
  assert(metadataFile != 0);

  m_verbose = propsPtr->getPropertyBool("verbose", false);

  if (Global::verbose) {
    cout << "Hypertable.RangeServer.AccessGroup.MaxFiles=" << Global::localityGroupMaxFiles << endl;
    cout << "Hypertable.RangeServer.AccessGroup.MaxMemory=" << Global::localityGroupMaxMemory << endl;
    cout << "Hypertable.RangeServer.AccessGroup.MergeFiles=" << Global::localityGroupMergeFiles << endl;
    cout << "Hypertable.RangeServer.BlockCache.MaxMemory=" << blockCacheMemory << endl;
    cout << "Hypertable.RangeServer.Range.MaxBytes=" << Global::rangeMaxBytes << endl;
    cout << "Hypertable.RangeServer.port=" << port << endl;
    cout << "Hypertable.RangeServer.workers=" << workerCount << endl;
    cout << "METADATA file = '" << metadataFile << "'" << endl;
  }

  m_conn_manager_ptr = new ConnectionManager(comm);
  m_app_queue_ptr = new ApplicationQueue(workerCount);

  /**
   * Load METADATA simulation data
   */
  Global::metadata = Metadata::new_instance(metadataFile);

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
    const char *logHost = propsPtr->getProperty("Hypertable.RangeServer.commit_log.dfs_broker.host", 0);
    uint16_t logPort    = propsPtr->getPropertyInt("Hypertable.RangeServer.commit_log.dfs_broker.port", 0);
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

    InetAddr::get_hostname(hostStr);
    InetAddr::initialize(&Global::local_addr, hostStr.c_str(), port);

    gettimeofday(&tval, 0);

    m_location = (std::string)inet_ntoa(Global::local_addr.sin_addr) + "_" + (int)port + "_" + (uint32_t)tval.tv_sec;
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

  /**
   *  Register with Master
   */

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
    errMsg = (std::string)table->name + "[" + range->startRow + ":" + range->endRow + "]";
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
  std::set<uint8_t> columnFamilies;
  uint32_t id;
  uint64_t scanTimestamp;
  SchemaPtr schemaPtr;
  ScanContextPtr scanContextPtr;

  if (Global::verbose) {
    cout << *table << endl;
    cout << *range << endl;
    cout << *scan_spec << endl;
  }

  if (!get_table_info(table->name, tableInfoPtr)) {
    error = Error::RANGESERVER_RANGE_NOT_FOUND;
    errMsg = (std::string)table->name + "[" + range->startRow + ":" + range->endRow + "]";
    goto abort;
  }

  if (!tableInfoPtr->get_range(range, rangePtr)) {
    error = Error::RANGESERVER_RANGE_NOT_FOUND;
    errMsg = (std::string)table->name + "[" + range->startRow + ":" + range->endRow + "]";
    goto abort;
  }

  scanTimestamp = rangePtr->get_timestamp();

  schemaPtr = tableInfoPtr->get_schema();

  /**
   * Load column families set
   */
  for (std::vector<const char *>::const_iterator iter = scan_spec->columns.begin(); iter != scan_spec->columns.end(); iter++) {
    Schema::ColumnFamily *cf = schemaPtr->get_column_family(*iter);
    if (cf == 0) {
      error = Error::RANGESERVER_INVALID_COLUMNFAMILY;
      errMsg = (std::string)*iter;
      goto abort;
    }
    columnFamilies.insert((uint8_t)cf->id);
  }


  kvBuffer = new uint8_t [ sizeof(int32_t) + DEFAULT_SCANBUF_SIZE ];
  kvLenp = (uint32_t *)kvBuffer;

  scanContextPtr = new ScanContext(scanTimestamp, scan_spec, schemaPtr);
  if (scanContextPtr->error != Error::OK) {
    errMsg = "Problem initializing scan context";
    goto abort;
  }
 
  scannerPtr = rangePtr->create_scanner(scanContextPtr);

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

  if (!Global::scannerMap.get(scannerId, scannerPtr, rangePtr)) {
    error = Error::RANGESERVER_INVALID_SCANNER_ID;
    char tbuf[32];
    sprintf(tbuf, "%d", scannerId);
    errMsg = (string)tbuf;
    goto abort;
    
  }

  if (Global::verbose)
    cout << "Scanner ID = " << scannerId << endl;

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
  RangeInfoPtr rangeInfoPtr;
  string tableHdfsDir;
  string rangeHdfsDir;
  char md5DigestStr[33];
  bool registerTable = false;
  const char metadata1_start_row_chars[5] = { '0', ':', 0xff, 0xff, 0 };

  if (Global::verbose)
    cout << *range << endl;

  /**
   * 1. Read METADATA entry for this range and make sure it exists
   */
  if ((error = Global::metadata->get_range_info(table->name, range->endRow, rangeInfoPtr)) != Error::OK) {
    errMsg = (std::string)"Unable to locate range of table '" + table->name + "' with end row '" + range->endRow + "' in METADATA table";
    goto abort;
  }
  else {
    std::string startRow;
    rangeInfoPtr->get_start_row(startRow);
    if (startRow != (std::string)range->startRow) {
      errMsg = (std::string)"Unable to locate range " + table->name + "[" + range->startRow + ":" + range->endRow + "] in METADATA table";
      goto abort;
    }
  }

  /**
   * Take ownership of the range
   */
  rangeInfoPtr->set_location(m_location);
  Global::metadata->sync();

#ifdef METADATA_TEST
  if (Global::metadata_range_server) {
    DynamicBuffer send_buf(0);
    std::string row_key = std::string("") + (uint32_t)table->id + ":" + range->endRow;
    CreateKeyAndAppend(send_buf, FLAG_INSERT, row_key.c_str(), Global::metadata_column_family_Location, 0, Global::log->get_timestamp());
    CreateAndAppend(send_buf, m_location.c_str());
    if ((error = Global::metadata_range_server->update(Global::local_addr, Global::metadata_identifier, send_buf.buf, send_buf.fill())) != Error::OK) {
      LOG_VA_ERROR("Problem updating METADATA Location: column - %s", Error::get_text(error));
      DUMP_CORE;
    }
    send_buf.release();
  }
#endif


  if (!get_table_info(table->name, tableInfoPtr)) {
    tableInfoPtr = new TableInfo(m_master_client_ptr, table, schemaPtr);
    registerTable = true;
  }

  if ((error = verify_schema(tableInfoPtr, table->generation, errMsg)) != Error::OK)
    goto abort;

  if (registerTable)
    set_table_info(table->name, tableInfoPtr);

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

  if (tableInfoPtr->get_range(range, rangePtr)) {
    error = Error::RANGESERVER_RANGE_ALREADY_LOADED;
    errMsg = (std::string)table->name + "[" + range->startRow + ":" + range->endRow + "]";
    goto abort;
  }

  tableInfoPtr->add_range(rangeInfoPtr, rangePtr);

  // TODO: if (flags & RangeServerProtocol::LOAD_RANGE_FLAG_PHANTOM), do the following in 'go live'
  {
    std::string split_log_dir;
    uint64_t timestamp = Global::log->get_timestamp();
    rangeInfoPtr->get_split_log_dir(split_log_dir);
    if (split_log_dir != "") {
      if ((error = Global::log->link_log(table->name, split_log_dir.c_str(), timestamp)) != Error::OK) {
	errMsg = (string)"Unable to link external log '" + split_log_dir + "' into commit log";
	goto abort;
      }
      if ((error = rangePtr->replay_split_log(split_log_dir)) != Error::OK) {
	errMsg = (string)"Problem replaying split log '" + split_log_dir + "'";
	goto abort;
      }
    }
  }

  rangeInfoPtr->set_log_dir(Global::log->get_log_dir());
  Global::metadata->sync();

#ifdef METADATA_TEST
  if (Global::metadata_range_server) {
    DynamicBuffer send_buf(0);
    std::string row_key = std::string("") + (uint32_t)table->id + ":" + range->endRow;
    CreateKeyAndAppend(send_buf, FLAG_INSERT, row_key.c_str(), Global::metadata_column_family_LogDir, 0, Global::log->get_timestamp());
    CreateAndAppend(send_buf, Global::log->get_log_dir().c_str());
    if ((error = Global::metadata_range_server->update(Global::local_addr, Global::metadata_identifier, send_buf.buf, send_buf.fill())) != Error::OK) {
      LOG_VA_ERROR("Problem updating METADATA LogDir: column - %s", Error::get_text(error));
      DUMP_CORE;
    }
    send_buf.release();
  }
#endif

  if ((error = cb->response_ok()) != Error::OK) {
    LOG_VA_ERROR("Problem sending OK response - %s", Error::get_text(error));
  }

#ifdef METADATA_TEST
  if (!strcmp(table->name, "METADATA") &&
      !strcmp(range->startRow, metadata1_start_row_chars) &&
      !strcmp(range->endRow, Key::END_ROW_MARKER)) {
    LOG_INFO("Loading second-level METADATA range");
    m_conn_manager_ptr->add(Global::local_addr, 15, "METADATA1");
    Global::metadata_identifier.id = table->id;
    Global::metadata_identifier.generation = table->generation;
    Global::metadata_identifier.name = new char [ strlen(table->name) + 1 ];
    strcpy((char *)Global::metadata_identifier.name, table->name);
    Global::metadata_range_server = new RangeServerClient(m_comm, 30);
    Schema::ColumnFamily *cf;

    cf = schemaPtr->get_column_family("LogDir");
    Global::metadata_column_family_LogDir = (uint8_t)cf->id;
    cf = schemaPtr->get_column_family("SplitLogDir");
    Global::metadata_column_family_SplitLogDir = (uint8_t)cf->id;
    cf = schemaPtr->get_column_family("SplitPoint");
    Global::metadata_column_family_SplitPoint = (uint8_t)cf->id;
    cf = schemaPtr->get_column_family("Files");
    Global::metadata_column_family_Files = (uint8_t)cf->id;
    cf = schemaPtr->get_column_family("StartRow");
    Global::metadata_column_family_StartRow = (uint8_t)cf->id;
    cf = schemaPtr->get_column_family("Location");
    Global::metadata_column_family_Location = (uint8_t)cf->id;
    cf = schemaPtr->get_column_family("Event");
    Global::metadata_column_family_Event = (uint8_t)cf->id;
  }
#endif

  /**
   *  If this is the root METADATA range, write our location into hyperspace
   */
  if (!strcmp(table->name, "METADATA") &&
      *range->startRow == 0 && !strcmp(range->endRow, metadata1_start_row_chars)) {
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

}



/**
 * Update
 */
void RangeServer::update(ResponseCallbackUpdate *cb, TableIdentifierT *table, BufferT &buffer) {
  const uint8_t *modPtr;
  const uint8_t *modEnd;
  string errMsg;
  int error = Error::OK;
  TableInfoPtr tableInfoPtr;
  uint64_t nextTimestamp = 0;
  uint64_t updateTimestamp = 0;
  uint64_t clientTimestamp = 0;
  struct timeval tval;
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
  size_t stopSize = 0;
  size_t splitSize = 0;
  std::string endRow;
  RangePtr rangePtr;

  /**
  if (Global::verbose)
    cout << *range << endl;
  */

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

  // timestamp
  gettimeofday(&tval, 0);
  nextTimestamp = ((uint64_t)tval.tv_sec * 1000000LL) + tval.tv_usec;

  while (modPtr < modEnd) {

    row = (const char *)((ByteString32T *)modPtr)->data;

    if (!tableInfoPtr->find_containing_range(row, rangePtr)) {
      update.base = modPtr;
      modPtr += Length((const ByteString32T *)modPtr); // skip key
      modPtr += Length((const ByteString32T *)modPtr); // skip value
      update.len = modPtr - update.base;
      stopMods.push_back(update);
      stopSize += update.len;
      continue;
    }

    /** Increment update count (block if maintenance in progress) **/
    rangePtr->increment_update_counter();

    /** Obtain "update timestamp" **/
    updateTimestamp = Global::log->get_timestamp();

    endRow = rangePtr->end_row();

    /** Fetch range split information **/
    rangePtr->get_split_info(split_row, splitLogPtr, &splitStartTime);

    splitMods.clear();
    splitSize = 0;
    goMods.clear();
    goSize = 0;

    while (modPtr < modEnd && (endRow == "" || (strcmp(row, endRow.c_str()) <= 0))) {
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
	rangePtr->decrement_update_counter();
	goto abort;
      }
    }

    /**
     * Commit mutations that are destined for this range
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
	rangePtr->decrement_update_counter();
	goto abort;
      }
    }

    /**
     * Apply the modifications
     */
    rangePtr->lock();
    /** Apply the GO mods **/
    for (size_t i=0; i<goMods.size(); i++) {
      ByteString32T *key = (ByteString32T *)goMods[i].base;
      ByteString32T *value = (ByteString32T *)(goMods[i].base + Length(key));
      rangePtr->add(key, value);
    }
    /** Apply the SPLIT mods **/
    for (size_t i=0; i<splitMods.size(); i++) {
      ByteString32T *key = (ByteString32T *)splitMods[i].base;
      ByteString32T *value = (ByteString32T *)(splitMods[i].base + Length(key));
      rangePtr->add(key, value);
    }
    rangePtr->unlock();

    /**
     * Split and Compaction processing
     */
    rangePtr->schedule_maintenance();

    rangePtr->decrement_update_counter();

    if (Global::verbose) {
      LOG_VA_INFO("Added %d (%d split off) updates to '%s'", goMods.size()+splitMods.size(), splitMods.size(), table->name);
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
bool RangeServer::get_table_info(string &name, TableInfoPtr &info) {
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
void RangeServer::set_table_info(string &name, TableInfoPtr &info) {
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
