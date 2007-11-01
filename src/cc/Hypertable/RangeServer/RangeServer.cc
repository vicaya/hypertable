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
#include "HyperspaceSessionHandler.h"
#include "MaintenanceThread.h"
#include "RangeServer.h"
#include "ScanContext.h"

using namespace hypertable;
using namespace std;

namespace {
  const int DEFAULT_SCANBUF_SIZE = 32768;
  const int DEFAULT_WORKERS = 20;
  const int DEFAULT_PORT    = 38549;
}


/**
 * Constructor
 */
RangeServer::RangeServer(Comm *comm, PropertiesPtr &propsPtr) : mMutex(), mVerbose(false), mComm(comm), mMasterClient(0) {
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

  /**
  const char *dir = propsPtr->getProperty("Hypertable.RangeServer.logDirRoot", 0);
  if (dir == 0) {
    cerr << "Hypertable.RangeServer.logDirRoot property not specified." << endl;
    exit(1);
  }
  Global::logDirRoot = (dir[0] == '/') ? "" : System::installDir + "/";
  if (dir[strlen(dir)-1] == '/')
    Global::logDirRoot += string(dir, strlen(dir)-1);
  else
    Global::logDirRoot += dir;
  */

  metadataFile = propsPtr->getProperty("metadata");
  assert(metadataFile != 0);

  mVerbose = propsPtr->getPropertyBool("verbose", false);

  if (Global::verbose) {
    cout << "Hypertable.RangeServer.AccessGroup.MaxFiles=" << Global::localityGroupMaxFiles << endl;
    cout << "Hypertable.RangeServer.AccessGroup.MaxMemory=" << Global::localityGroupMaxMemory << endl;
    cout << "Hypertable.RangeServer.AccessGroup.MergeFiles=" << Global::localityGroupMergeFiles << endl;
    cout << "Hypertable.RangeServer.BlockCache.MaxMemory=" << blockCacheMemory << endl;
    cout << "Hypertable.RangeServer.Range.MaxBytes=" << Global::rangeMaxBytes << endl;
    //cout << "Hypertable.RangeServer.logDirRoot=" << Global::logDirRoot << endl;
    cout << "Hypertable.RangeServer.port=" << port << endl;
    cout << "Hypertable.RangeServer.workers=" << workerCount << endl;
    cout << "METADATA file = '" << metadataFile << "'" << endl;
  }

  mConnManager = new ConnectionManager(comm);
  mAppQueue = new ApplicationQueue(workerCount);
  mHandlerFactory = new HandlerFactory(comm, mAppQueue, this);

  /**
   * Load METADATA simulation data
   */
  Global::metadata = Metadata::NewInstance(metadataFile);

  Global::protocol = new hypertable::RangeServerProtocol();

  /**
   * Connect to Hyperspace
   */
  mHyperspace = new Hyperspace::Session(comm, propsPtr, new HyperspaceSessionHandler(this));
  if (!mHyperspace->WaitForConnection(30)) {
    LOG_ERROR("Unable to connect to hyperspace, exiting...");
    exit(1);
  }

  DfsBroker::Client *dfsClient = new DfsBroker::Client(mConnManager, propsPtr);

  if (mVerbose) {
    cout << "DfsBroker.host=" << propsPtr->getProperty("DfsBroker.host", "") << endl;
    cout << "DfsBroker.port=" << propsPtr->getProperty("DfsBroker.port", "") << endl;
    cout << "DfsBroker.timeout=" << propsPtr->getProperty("DfsBroker.timeout", "") << endl;
  }

  if (!dfsClient->WaitForConnection(30)) {
    LOG_ERROR("Unable to connect to DFS Broker, exiting...");
    exit(1);
  }

  Global::dfs = dfsClient;

  /**
   * 
   */
  {
    struct sockaddr_in localAddr;
    struct timeval tval;
    std::string hostStr;

    InetAddr::GetHostname(hostStr);
    InetAddr::Initialize(&localAddr, hostStr.c_str(), port);

    gettimeofday(&tval, 0);

    mServerIdStr = (std::string)inet_ntoa(localAddr.sin_addr) + ":" + (int)port + "_" + (uint32_t)tval.tv_sec;
  }

  if (DirectoryInitialize(propsPtr.get()) != Error::OK)
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
    struct sockaddr_in addr;
    InetAddr::Initialize(&addr, INADDR_ANY, port);  // Listen on any interface
    if ((error = comm->Listen(addr, mHandlerFactory)) != Error::OK) {
      LOG_VA_ERROR("Listen error address=%s:%d - %s", inet_ntoa(addr.sin_addr), port, Error::GetText(error));
      exit(1);
    }
  }

  /**
   * Create Master client
   */
  mMasterClient = new MasterClient(mConnManager, mHyperspace, 20, mAppQueue);
  mMasterConnectionHandler = new ConnectionHandler(mComm, mAppQueue, this, mMasterClient);
  mMasterClient->InitiateConnection(mMasterConnectionHandler);

}


/**
 *
 */
RangeServer::~RangeServer() {
  mAppQueue->Shutdown();
  delete mAppQueue;
  delete mHyperspace;
  delete mConnManager;
}



/**
 * - Figure out and create range server directory
 * - Clear any Range server state (including any partially created commit logs)
 * - Open the commit log
 */
int RangeServer::DirectoryInitialize(Properties *props) {
  int error;
  bool exists;
  std::string topDir;

  /**
   * Create /hypertable/servers directory
   */
  if ((error = mHyperspace->Exists("/hypertable/servers", &exists)) != Error::OK) {
    LOG_VA_ERROR("Problem checking existence of '/hypertable/servers' Hyperspace directory - %s", Error::GetText(error));
    return error;
  }

  if (!exists) {
    LOG_ERROR("Hyperspace directory '/hypertable/servers' does not exist, try running 'Hypertable.Master --initialize' first");
    return error;
  }

  topDir = (std::string)"/hypertable/servers/" + mServerIdStr;

  /**
   * Create "server existence" file in Hyperspace and obtain an exclusive lock on it
   */

  uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_CREATE | OPEN_FLAG_EXCL | OPEN_FLAG_LOCK_EXCLUSIVE;
  HandleCallbackPtr nullCallbackPtr;

  if ((error = mHyperspace->Open(topDir.c_str(), oflags, nullCallbackPtr, &mExistenceFileHandle)) != Error::OK) {
    LOG_VA_ERROR("Problem creating Hyperspace server existance file '%s' - %s", topDir.c_str(), Error::GetText(error));
    exit(1);
  }

  if ((error = mHyperspace->GetSequencer(mExistenceFileHandle, &mExistenceFileSequencer)) != Error::OK) {
    LOG_VA_ERROR("Problem obtaining lock sequencer for file '%s' - %s", topDir.c_str(), Error::GetText(error));
    exit(1);
  }

  Global::logDir = (string)topDir.c_str() + "/commit/primary";

  /**
   * Create /hypertable/servers/X.X.X.X:p_nnnnn/commit/primary directory
   */
  if ((error = Global::dfs->Mkdirs(Global::logDir)) != Error::OK) {
    LOG_VA_ERROR("Problem creating local log directory '%s'", Global::logDir.c_str());
    return error;
  }

  int64_t logFileSize = props->getPropertyInt64("Hypertable.RangeServer.logFileSize", 0x100000000LL);
  if (Global::verbose) {
    cout << "Hypertable.RangeServer.logFileSize=" << logFileSize << endl;
    cout << "logDir=" << Global::logDir << endl;
  }

  Global::log = new CommitLog(Global::dfs, Global::logDir, logFileSize);    

  /**
   *  Register with Master
   */

  return Error::OK;
}



/**
 * Compact
 */
void RangeServer::Compact(ResponseCallback *cb, RangeSpecificationT *rangeSpec, uint8_t compactionType) {
  int error = Error::OK;
  std::string errMsg;
  TableInfoPtr tableInfoPtr;
  RangePtr rangePtr;
  MaintenanceThread::WorkType workType = MaintenanceThread::COMPACTION_MINOR;
  bool major = false;

  // Check for major compaction
  if (compactionType == 1) {
    major = true;
    workType = MaintenanceThread::COMPACTION_MAJOR;
  }

  if (Global::verbose) {
    cout << *rangeSpec;
    cout << "Compaction type = " << (major ? "major" : "minor") << endl;
  }

  /**
   * Fetch table info
   */
  if (!GetTableInfo(rangeSpec->tableName, tableInfoPtr)) {
    error = Error::RANGESERVER_RANGE_NOT_FOUND;
    errMsg = "No ranges loaded for table '" + (std::string)rangeSpec->tableName + "'";
    goto abort;
  }

  /**
   * Fetch range info
   */
  if (!tableInfoPtr->GetRange(rangeSpec, rangePtr)) {
    error = Error::RANGESERVER_RANGE_NOT_FOUND;
    errMsg = (std::string)rangeSpec->tableName + "[" + rangeSpec->startRow + ":" + rangeSpec->endRow + "]";
    goto abort;
  }

  MaintenanceThread::ScheduleCompaction(rangePtr.get(), workType);

  if ((error = cb->response_ok()) != Error::OK) {
    LOG_VA_ERROR("Problem sending OK response - %s", Error::GetText(error));
  }

  if (Global::verbose) {
    LOG_VA_INFO("Compaction (%s) scheduled for table '%s' end row '%s'",
		(major ? "major" : "minor"), rangeSpec->tableName, rangeSpec->endRow);
  }

  error = Error::OK;

 abort:
  if (error != Error::OK) {
    LOG_VA_ERROR("%s '%s'", Error::GetText(error), errMsg.c_str());
    if ((error = cb->error(error, errMsg)) != Error::OK) {
      LOG_VA_ERROR("Problem sending error response - %s", Error::GetText(error));
    }
  }
}



/** 
 *  CreateScanner
 */
void RangeServer::CreateScanner(ResponseCallbackCreateScanner *cb, RangeSpecificationT *rangeSpec, ScanSpecificationT *scanSpec) {
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
    cout << *rangeSpec << endl;
    cout << *scanSpec << endl;
  }

  if (!GetTableInfo(rangeSpec->tableName, tableInfoPtr)) {
    error = Error::RANGESERVER_RANGE_NOT_FOUND;
    errMsg = (std::string)rangeSpec->tableName + "[" + rangeSpec->startRow + ":" + rangeSpec->endRow + "]";
    goto abort;
  }

  if (!tableInfoPtr->GetRange(rangeSpec, rangePtr)) {
    error = Error::RANGESERVER_RANGE_NOT_FOUND;
    errMsg = (std::string)rangeSpec->tableName + "[" + rangeSpec->startRow + ":" + rangeSpec->endRow + "]";
    goto abort;
  }

  scanTimestamp = rangePtr->GetTimestamp();

  schemaPtr = tableInfoPtr->GetSchema();

  /**
   * Load column families set
   */
  for (std::vector<const char *>::const_iterator iter = scanSpec->columns.begin(); iter != scanSpec->columns.end(); iter++) {
    Schema::ColumnFamily *cf = schemaPtr->GetColumnFamily(*iter);
    if (cf == 0) {
      error = Error::RANGESERVER_INVALID_COLUMNFAMILY;
      errMsg = (std::string)*iter;
      goto abort;
    }
    columnFamilies.insert((uint8_t)cf->id);
  }


  kvBuffer = new uint8_t [ sizeof(int32_t) + DEFAULT_SCANBUF_SIZE ];
  kvLenp = (uint32_t *)kvBuffer;

  scanContextPtr.reset( new ScanContext(scanTimestamp, scanSpec, schemaPtr) );
  if (scanContextPtr->error != Error::OK) {
    errMsg = "Problem initializing scan context";
    goto abort;
  }
 
  scannerPtr.reset( rangePtr->CreateScanner(scanContextPtr));

  more = FillScanBlock(scannerPtr, kvBuffer+sizeof(int32_t), DEFAULT_SCANBUF_SIZE, kvLenp);
  if (more)
    id = Global::scannerMap.Put(scannerPtr, rangePtr);
  else
    id = 0;

  if (Global::verbose) {
    LOG_VA_INFO("Successfully created scanner on table '%s'", rangeSpec->tableName);
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
      LOG_VA_ERROR("Problem sending OK response - %s", Error::GetText(error));
    }
  }

  error = Error::OK;

 abort:
  if (error != Error::OK) {
    LOG_VA_ERROR("%s '%s'", Error::GetText(error), errMsg.c_str());
    if ((error = cb->error(error, errMsg)) != Error::OK) {
      LOG_VA_ERROR("Problem sending error response - %s", Error::GetText(error));
    }
  }
}


void RangeServer::FetchScanblock(ResponseCallbackFetchScanblock *cb, uint32_t scannerId) {
  string errMsg;
  int error = Error::OK;
  CellListScannerPtr scannerPtr;
  RangePtr rangePtr;
  bool more = true;
  uint8_t *kvBuffer = 0;
  uint32_t *kvLenp = 0;
  uint32_t bytesFetched = 0;

  if (!Global::scannerMap.Get(scannerId, scannerPtr, rangePtr)) {
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
    Global::scannerMap.Remove(scannerId);
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
      LOG_VA_ERROR("Problem sending OK response - %s", Error::GetText(error));
    }
  }

  if (Global::verbose) {
    LOG_VA_INFO("Successfully fetched %d bytes of scan data", bytesFetched);
  }

  error = Error::OK;

 abort:
  if (error != Error::OK) {
    LOG_VA_ERROR("%s '%s'", Error::GetText(error), errMsg.c_str());
    if ((error = cb->error(error, errMsg)) != Error::OK) {
      LOG_VA_ERROR("Problem sending error response - %s", Error::GetText(error));
    }
  }
}


/**
 * LoadRange
 */
void RangeServer::LoadRange(ResponseCallback *cb, RangeSpecificationT *rangeSpec) {
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

  if (Global::verbose)
    cout << *rangeSpec << endl;

  /**
   * 1. Read METADATA entry for this range and make sure it exists
   */
  if ((error = Global::metadata->GetRangeInfo(rangeSpec->tableName, rangeSpec->endRow, rangeInfoPtr)) != Error::OK) {
    errMsg = (std::string)"Unable to locate range of table '" + rangeSpec->tableName + "' with end row '" + rangeSpec->endRow + "' in METADATA table";
    goto abort;
  }
  else {
    std::string startRow;
    rangeInfoPtr->GetStartRow(startRow);
    if (startRow != (std::string)rangeSpec->startRow) {
      errMsg = (std::string)"Unable to locate range " + rangeSpec->tableName + "[" + rangeSpec->startRow + ":" + rangeSpec->endRow + "] in METADATA table";
      goto abort;
    }
  }

  if (!GetTableInfo(rangeSpec->tableName, tableInfoPtr)) {
    tableInfoPtr.reset( new TableInfo(rangeSpec->tableName, schemaPtr) );
    registerTable = true;
  }

  if ((error = VerifySchema(tableInfoPtr, rangeSpec->generation, errMsg)) != Error::OK)
    goto abort;

  if (registerTable)
    SetTableInfo(rangeSpec->tableName, tableInfoPtr);

  schemaPtr = tableInfoPtr->GetSchema();

  /**
   * Check for existence of and create, if necessary, range directory (md5 of endrow)
   * under all locality group directories for this table
   */
  {
    if (*rangeSpec->endRow == 0)
      memset(md5DigestStr, '0', 24);
    else
      md5_string(rangeSpec->endRow, md5DigestStr);
    md5DigestStr[24] = 0;
    tableHdfsDir = (string)"/hypertable/tables/" + (string)rangeSpec->tableName;
    list<Schema::AccessGroup *> *lgList = schemaPtr->GetAccessGroupList();
    for (list<Schema::AccessGroup *>::iterator lgIter = lgList->begin(); lgIter != lgList->end(); lgIter++) {
      // notice the below variables are different "range" vs. "table"
      rangeHdfsDir = tableHdfsDir + "/" + (*lgIter)->name + "/" + md5DigestStr;
      if ((error = Global::dfs->Mkdirs(rangeHdfsDir)) != Error::OK) {
	errMsg = (string)"Problem creating range directory '" + rangeHdfsDir + "'";
	goto abort;
      }
    }
  }

  if (tableInfoPtr->GetRange(rangeSpec, rangePtr)) {
    error = Error::RANGESERVER_RANGE_ALREADY_LOADED;
    errMsg = (std::string)rangeSpec->tableName + "[" + rangeSpec->startRow + ":" + rangeSpec->endRow + "]";
    goto abort;
  }

  tableInfoPtr->AddRange(rangeInfoPtr);

  rangeInfoPtr->SetLogDir(Global::log->GetLogDir());
  Global::metadata->Sync();

  if ((error = cb->response_ok()) != Error::OK) {
    LOG_VA_ERROR("Problem sending OK response - %s", Error::GetText(error));
  }

  error = Error::OK;

 abort:
  if (error != Error::OK) {
    LOG_VA_ERROR("%s '%s'", Error::GetText(error), errMsg.c_str());
    if ((error = cb->error(error, errMsg)) != Error::OK) {
      LOG_VA_ERROR("Problem sending error response - %s", Error::GetText(error));
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
void RangeServer::Update(ResponseCallbackUpdate *cb, const char *tableName, uint32_t generation, BufferT &buffer) {
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
  const char *splitRow;
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
    cout << *rangeSpec << endl;
  */

  // TODO: Sanity check mod data (checksum validation)

  /**
   * Fetch table info
   */
  if (!GetTableInfo(tableName, tableInfoPtr)) {
    ExtBufferT ext;
    ext.buf = new uint8_t [ buffer.len ];
    memcpy(ext.buf, buffer.buf, buffer.len);
    ext.len = buffer.len;
    LOG_VA_ERROR("Unable to find table info for table '%s'", tableName);
    if ((error = cb->response(ext)) != Error::OK) {
      LOG_VA_ERROR("Problem sending OK response - %s", Error::GetText(error));
    }
    return;
  }

  // verify schema
  if ((error = VerifySchema(tableInfoPtr, generation, errMsg)) != Error::OK)
    goto abort;

  modEnd = buffer.buf + buffer.len;
  modPtr = buffer.buf;

  // timestamp
  gettimeofday(&tval, 0);
  nextTimestamp = ((uint64_t)tval.tv_sec * 1000000LL) + tval.tv_usec;

  while (modPtr < modEnd) {

    row = (const char *)((ByteString32T *)modPtr)->data;

    if (!tableInfoPtr->FindContainingRange(row, rangePtr)) {
      update.base = modPtr;
      modPtr += Length((const ByteString32T *)modPtr); // skip key
      modPtr += Length((const ByteString32T *)modPtr); // skip value
      update.len = modPtr - update.base;
      stopMods.push_back(update);
      stopSize += update.len;
      continue;
    }

    /** Increment update count (block if maintenance in progress) **/
    rangePtr->IncrementUpdateCounter();

    /** Obtain "update timestamp" **/
    updateTimestamp = Global::log->GetTimestamp();

    endRow = rangePtr->EndRow();

    /** Fetch range split information **/
    if (rangePtr->GetSplitInfo(splitKeyPtr, splitLogPtr, &splitStartTime))
      splitRow = (const char *)(splitKeyPtr.get())->data;
    else
      splitRow = 0;

    splitMods.clear();
    splitSize = 0;
    goMods.clear();
    goSize = 0;

    while (modPtr < modEnd && (endRow == "" || (strcmp(row, endRow.c_str()) <= 0))) {
      update.base = modPtr;
      modPtr += Length((const ByteString32T *)modPtr); // skip key
      modPtr += Length((const ByteString32T *)modPtr); // skip value
      update.len = modPtr - update.base;
      if (splitStartTime != 0 && updateTimestamp > splitStartTime && strcmp(row, splitRow) <= 0) {
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

      if ((error = splitLogPtr->Write(tableName, base, ptr-base, clientTimestamp)) != Error::OK) {
	errMsg = (string)"Problem writing " + (int)(ptr-base) + " bytes to split log";
	rangePtr->DecrementUpdateCounter();
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
      if ((error = Global::log->Write(tableName, base, ptr-base, clientTimestamp)) != Error::OK) {
	errMsg = (string)"Problem writing " + (int)(ptr-base) + " bytes to commit log";
	rangePtr->DecrementUpdateCounter();
	goto abort;
      }
    }

    /**
     * Apply the modifications
     */
    rangePtr->Lock();
    /** Apply the GO mods **/
    for (size_t i=0; i<goMods.size(); i++) {
      ByteString32T *key = (ByteString32T *)goMods[i].base;
      ByteString32T *value = (ByteString32T *)(goMods[i].base + Length(key));
      rangePtr->Add(key, value);
    }
    /** Apply the SPLIT mods **/
    for (size_t i=0; i<splitMods.size(); i++) {
      ByteString32T *key = (ByteString32T *)splitMods[i].base;
      ByteString32T *value = (ByteString32T *)(splitMods[i].base + Length(key));
      rangePtr->Add(key, value);
    }
    rangePtr->Unlock();

    /**
     * Split and Compaction processing
     */
    rangePtr->ScheduleMaintenance();

    rangePtr->DecrementUpdateCounter();

    if (Global::verbose) {
      LOG_VA_INFO("Added %d (%d split off) updates to '%s'", goMods.size()+splitMods.size(), splitMods.size(), tableName);
    }
  }

  if (Global::verbose) {
    LOG_VA_INFO("Dropped %d updates since they didn't lie within any of this server's ranges", stopMods.size());
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
      LOG_VA_ERROR("Problem sending OK response - %s", Error::GetText(error));
    }
  }
  else {
    if ((error = cb->response_ok()) != Error::OK) {
      LOG_VA_ERROR("Problem sending OK response - %s", Error::GetText(error));
    }
  }

  error = Error::OK;

 abort:

  if (error != Error::OK) {
    LOG_VA_ERROR("%s '%s'", Error::GetText(error), errMsg.c_str());
    if ((error = cb->error(error, errMsg)) != Error::OK) {
      LOG_VA_ERROR("Problem sending error response - %s", Error::GetText(error));
    }
  }
}



/**
 *
 */
bool RangeServer::GetTableInfo(string &name, TableInfoPtr &info) {
  boost::mutex::scoped_lock lock(mMutex);
  TableInfoMapT::iterator iter = mTableInfoMap.find(name);
  if (iter == mTableInfoMap.end())
    return false;
  info = (*iter).second;
  return true;
}



/**
 *
 */
void RangeServer::SetTableInfo(string &name, TableInfoPtr &info) {
  boost::mutex::scoped_lock lock(mMutex);
  TableInfoMapT::iterator iter = mTableInfoMap.find(name);
  if (iter != mTableInfoMap.end())
    mTableInfoMap.erase(iter);
  mTableInfoMap[name] = info;
}



int RangeServer::VerifySchema(TableInfoPtr &tableInfoPtr, int generation, std::string &errMsg) {
  std::string tableFile = (std::string)"/hypertable/tables/" + tableInfoPtr->GetName();
  DynamicBuffer valueBuf(0);
  HandleCallbackPtr nullHandleCallback;
  int error;
  uint64_t handle;
  SchemaPtr schemaPtr = tableInfoPtr->GetSchema();

  if (schemaPtr.get() == 0 || schemaPtr->GetGeneration() < generation) {

    if ((error = mHyperspace->Open(tableFile.c_str(), OPEN_FLAG_READ, nullHandleCallback, &handle)) != Error::OK) {
      LOG_VA_ERROR("Unable to open Hyperspace file '%s' (%s)", tableFile.c_str(), Error::GetText(error));
      exit(1);
    }

    if ((error = mHyperspace->AttrGet(handle, "schema", valueBuf)) != Error::OK) {
      errMsg = (std::string)"Problem getting 'schema' attribute for '" + tableFile + "'";
      return error;
    }

    mHyperspace->Close(handle);

    schemaPtr.reset( Schema::NewInstance((const char *)valueBuf.buf, valueBuf.fill(), true) );
    if (!schemaPtr->IsValid()) {
      errMsg = "Schema Parse Error for table '" + tableInfoPtr->GetName() + "' : " + schemaPtr->GetErrorString();
      return Error::RANGESERVER_SCHEMA_PARSE_ERROR;
    }

    tableInfoPtr->UpdateSchema(schemaPtr);

    // Generation check ...
    if ( schemaPtr->GetGeneration() < generation ) {
      errMsg = "Fetched Schema generation for table '" + tableInfoPtr->GetName() + "' is " + schemaPtr->GetGeneration() + " but supplied is " + generation;
      return Error::RANGESERVER_GENERATION_MISMATCH;
    }
  }
  
  return Error::OK;
}
