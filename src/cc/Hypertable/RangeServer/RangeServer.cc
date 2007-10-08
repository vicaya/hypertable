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
#include "Common/System.h"

#include "Hypertable/Lib/RangeServerProtocol.h"

#include "DfsBroker/Lib/Client.h"

#include "CommitLogLocal.h"
#include "FillScanBlock.h"
#include "Global.h"
#include "MaintenanceThread.h"
#include "RangeServer.h"
#include "ScanContext.h"
#include "VerifySchema.h"

using namespace hypertable;
using namespace std;

namespace {
  const int DEFAULT_SCANBUF_SIZE = 32768;
}


/**
 * Constructor
 */
RangeServer::RangeServer(ConnectionManager *connManager, PropertiesPtr &propsPtr) : mMutex(), mVerbose(false) {
  const char *metadataFile = 0;

  Global::rangeMaxBytes           = propsPtr->getPropertyInt64("Hypertable.RangeServer.Range.MaxBytes", 200000000LL);
  Global::localityGroupMaxFiles   = propsPtr->getPropertyInt("Hypertable.RangeServer.AccessGroup.MaxFiles", 10);
  Global::localityGroupMergeFiles = propsPtr->getPropertyInt("Hypertable.RangeServer.AccessGroup.MergeFiles", 4);
  Global::localityGroupMaxMemory  = propsPtr->getPropertyInt("Hypertable.RangeServer.AccessGroup.MaxMemory", 4000000);

  uint64_t blockCacheMemory = propsPtr->getPropertyInt64("Hypertable.RangeServer.BlockCache.MaxMemory", 200000000LL);
  Global::blockCache = new FileBlockCache(blockCacheMemory);

  assert(Global::localityGroupMergeFiles <= Global::localityGroupMaxFiles);

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

  metadataFile = propsPtr->getProperty("metadata");
  assert(metadataFile != 0);

  mVerbose = propsPtr->getPropertyBool("verbose", false);

  if (Global::verbose) {
    cout << "Hypertable.RangeServer.logDirRoot=" << Global::logDirRoot << endl;
    cout << "Hypertable.RangeServer.Range.MaxBytes=" << Global::rangeMaxBytes << endl;
    cout << "Hypertable.RangeServer.AccessGroup.MaxFiles=" << Global::localityGroupMaxFiles << endl;
    cout << "Hypertable.RangeServer.AccessGroup.MergeFiles=" << Global::localityGroupMergeFiles << endl;
    cout << "Hypertable.RangeServer.AccessGroup.MaxMemory=" << Global::localityGroupMaxMemory << endl;
    cout << "Hypertable.RangeServer.BlockCache.MaxMemory=" << blockCacheMemory << endl;
    cout << "METADATA file = '" << metadataFile << "'" << endl;
  }

  /**
   * Load METADATA simulation data
   */
  Global::metadata = Metadata::NewInstance(metadataFile);

  Global::protocol = new hypertable::RangeServerProtocol();

  Global::hyperspace = new Hyperspace::Session(connManager->GetComm(), propsPtr, 0);

  if (!Global::hyperspace->WaitForConnection(30)) {
    LOG_ERROR("Unable to connect to hyperspace, exiting...");
    exit(1);
  }

  DfsBroker::Client *dfsClient = new DfsBroker::Client(connManager, propsPtr);

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

  if (DirectoryInitialize(propsPtr.get()) != Error::OK)
    exit(1);

  // start commpaction thread
  {
    MaintenanceThread maintenanceThread;
    Global::maintenanceThreadPtr = new boost::thread(maintenanceThread);
  }

}


/**
 * - Figure out and create range server directory
 * - Clear any Range server state (including any partially created commit logs)
 * - Open the commit log
 */
int RangeServer::DirectoryInitialize(Properties *props) {
  char hostname[256];
  char ipStr[32];
  int error;
  struct timeval tval;
  bool exists;

  if (gethostname(hostname, 256) != 0) {
    LOG_VA_ERROR("gethostname() failed - %s", strerror(errno));
    exit(1);
  }

  struct hostent *he = gethostbyname(hostname);
  if (he == 0) {
    herror("gethostbyname()");
    exit(1);
  }

  strcpy(ipStr, inet_ntoa(*(struct in_addr *)he->h_addr_list[0]));

  /**
   * Create /hypertable/servers directory
   */
  if ((error = Global::hyperspace->Exists("/hypertable/servers", &exists)) != Error::OK) {
    LOG_VA_ERROR("Problem checking existence of '/hypertable/servers' hypertablefs directory - %s", Error::GetText(error));
    return error;
  }

  if (!exists) {
    LOG_ERROR("Hypertablefs directory '/hypertable/servers' does not exist, try running 'Hypertable.Master --initialize' first");
    return error;
  }

  gettimeofday(&tval, 0);

  boost::shared_array<char> topDirPtr( new char [ 64 + strlen(ipStr) ] );
  char *topDir = topDirPtr.get();
  sprintf(topDir, "/hypertable/servers/%s_%ld", ipStr, tval.tv_sec);

  /**
   * Create "server existence" file in Hyperspace and obtain an exclusive lock on it
   */

  uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_CREATE | OPEN_FLAG_EXCL | OPEN_FLAG_LOCK;
  HandleCallbackPtr nullCallbackPtr;

  if ((error = Global::hyperspace->Open(topDir, oflags, nullCallbackPtr, &mExistenceFileHandle)) != Error::OK) {
    LOG_VA_ERROR("Problem creating Hyperspace server existance file '%s' - %s", topDir, Error::GetText(error));
    exit(1);
  }

  if ((error = Global::hyperspace->Lock(mExistenceFileHandle, LOCK_MODE_EXCLUSIVE, &mExistenceFileSequencer)) != Error::OK) {
    LOG_VA_ERROR("Problem obtaining exclusive lock on server existance file '%s' - %s", topDir, Error::GetText(error));
    exit(1);
  }

  Global::logDir = (string)topDir + "/commit/primary";

  /**
   * Create /hypertable/servers/X.X.X.X_nnnnn/commit/primary directory
   */
  string logDir = Global::logDirRoot + Global::logDir;

  if (!FileUtils::Mkdirs(logDir.c_str())) {
    LOG_VA_ERROR("Problem creating local log directory '%s'", logDir.c_str());
    return -1;
  }

  int64_t logFileSize = props->getPropertyInt64("Hypertable.RangeServer.logFileSize", 0x100000000LL);
  if (Global::verbose) {
    cout << "Hypertable.RangeServer.logFileSize=" << logFileSize << endl;
    cout << "logDir=" << Global::logDir << endl;
  }

  Global::log = new CommitLogLocal(Global::logDirRoot, Global::logDir, logFileSize);    

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
  if ((error = Global::metadata->GetRangeInfo(rangeSpec->tableName, rangeSpec->startRow, rangeSpec->endRow, rangeInfoPtr)) != Error::OK) {
    errMsg = (std::string)"Unable to locate range" + rangeSpec->tableName + "[" + rangeSpec->startRow + ":" + rangeSpec->endRow + "] in METADATA table";
    goto abort;
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
void RangeServer::Update(ResponseCallbackUpdate *cb, RangeSpecificationT *rangeSpec, BufferT &buffer) {
  const uint8_t *modPtr;
  const uint8_t *modEnd;
  string errMsg;
  int error = Error::OK;
  string tableFile;
  TableInfoPtr tableInfoPtr;
  TableInfo *tableInfo = 0;
  string rowkey;
  vector<UpdateRecT> goMods;
  vector<UpdateRecT> stopMods;
  vector<UpdateRecT> splitMods;
  RangePtr rangePtr;
  UpdateRecT update;
  size_t goSize = 0;
  size_t stopSize = 0;
  size_t splitSize = 0;
  Key keyComps;
  uint64_t nextTimestamp = 0;
  uint64_t updateTimestamp = 0;
  uint64_t clientTimestamp = 0;
  ByteString32Ptr splitKeyPtr;
  CommitLogPtr splitLogPtr;
  uint64_t splitStartTime;
  std::string  splitRow;
  bool needDecrement = false;
  struct timeval tval;

  /**
  if (Global::verbose)
    cout << *rangeSpec << endl;
  */

  // TODO: Sanity check mod data (checksum validation)

  /**
   * Fetch table info
   */
  if (GetTableInfo(rangeSpec->tableName, tableInfoPtr))
    tableInfo = tableInfoPtr.get();
  else {
    ExtBufferT ext;
    ext.buf = new uint8_t [ buffer.len ];
    memcpy(ext.buf, buffer.buf, buffer.len);
    ext.len = buffer.len;
    LOG_VA_ERROR("Unable to find table info for table '%s'", rangeSpec->tableName);
    if ((error = cb->response(ext)) != Error::OK) {
      LOG_VA_ERROR("Problem sending OK response - %s", Error::GetText(error));
    }
    return;
  }

  /**
   * Fetch range
   */
  if (!tableInfoPtr->GetRange(rangeSpec, rangePtr)) {
    error = Error::RANGESERVER_RANGE_NOT_FOUND;
    errMsg = (std::string)rangeSpec->tableName + "[" + rangeSpec->startRow + ":" + rangeSpec->endRow + "]";
    goto abort;
  }

  // timestamp
  gettimeofday(&tval, 0);
  nextTimestamp = ((uint64_t)tval.tv_sec * 1000000LL) + tval.tv_usec;

  /** Increment update count (block if maintenance in progress) **/
  rangePtr->IncrementUpdateCounter();
  needDecrement = true;

  /** Obtain "update timestamp" **/
  updateTimestamp = Global::log->GetTimestamp();

  /** Fetch range split information **/
  if (rangePtr->GetSplitInfo(splitKeyPtr, splitLogPtr, &splitStartTime)) {
    splitRow = (const char *)(splitKeyPtr.get())->data;
  }

  /**
   * Figure out destination for each mutations
   */
  modEnd = buffer.buf + buffer.len;
  modPtr = buffer.buf;
  while (modPtr < modEnd) {
    if (!keyComps.load((ByteString32T *)modPtr)) {
      error = Error::PROTOCOL_ERROR;
      errMsg = "Problem de-serializing key/value pair";
      goto abort;
    }

    if (keyComps.timestamp == -1LL)
      keyComps.updateTimestamp(nextTimestamp++);

    if (clientTimestamp < keyComps.timestamp)
      clientTimestamp = keyComps.timestamp;

    rowkey = keyComps.rowKey;
    update.base = modPtr;
    modPtr = keyComps.endPtr + Length((const ByteString32T *)keyComps.endPtr);
    update.len = modPtr - update.base;

    if (rowkey > rangePtr->StartRow() && (rangePtr->EndRow() == "" || rowkey <= rangePtr->EndRow())) {
      if (splitStartTime != 0 && updateTimestamp > splitStartTime && rowkey <= splitRow) {
	splitMods.push_back(update);
	splitSize += update.len;
      }
      else {
	goMods.push_back(update);
	goSize += update.len;
      }
    }
    else {
      stopMods.push_back(update);
      stopSize += update.len;
    }
  }

  /**
     cout << "goMods.size() = " << goMods.size() << endl;
     cout << "stopMods.size() = " << stopMods.size() << endl;
  **/

  if (splitSize > 0) {
    boost::shared_array<uint8_t> bufPtr( new uint8_t [ splitSize ] );
    uint8_t *base = bufPtr.get();
    uint8_t *ptr = base;
      
    for (size_t i=0; i<splitMods.size(); i++) {
      memcpy(ptr, splitMods[i].base, splitMods[i].len);
      ptr += splitMods[i].len;
    }

    if ((error = splitLogPtr->Write(rangeSpec->tableName, base, ptr-base, clientTimestamp)) != Error::OK) {
      errMsg = (string)"Problem writing " + (int)(ptr-base) + " bytes to split log";
      goto abort;
    }
  }

  /**
   * Commit mutations that are destined for this range server
   */
  if (goSize > 0) {
    boost::shared_array<uint8_t> bufPtr( new uint8_t [ goSize ] );
    uint8_t *base = bufPtr.get();
    uint8_t *ptr = base;

    for (size_t i=0; i<goMods.size(); i++) {
      memcpy(ptr, goMods[i].base, goMods[i].len);
      ptr += goMods[i].len;
    }
    if ((error = Global::log->Write(rangeSpec->tableName, base, ptr-base, clientTimestamp)) != Error::OK) {
      errMsg = (string)"Problem writing " + (int)(ptr-base) + " bytes to commit log";
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

  // Get table info
  // For each K/V pair
  //  - Get range
  //  - If OK, add to goBuf
  //  - If no range, add to stopBuf
  // If goBuf non-empty, add goBuf to commit log
  // If stopBuf non-empty, return Error::PARTIAL_UPDATE with stopBuf
  // otherwise, return Error::OK
  // Add goBuf K/V pairs to CellCaches
  // If range size exceeds threshold, schedule split
  // else if CellCache exceeds threshold, schedule compaction

  if (Global::verbose) {
    LOG_VA_INFO("Added %d (%d split off) and dropped %d updates to '%s'",
		goMods.size() + splitMods.size(), splitMods.size(), stopMods.size(), rangeSpec->tableName);
  }

  error = Error::OK;

 abort:

  if (needDecrement)
    rangePtr->DecrementUpdateCounter();

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

