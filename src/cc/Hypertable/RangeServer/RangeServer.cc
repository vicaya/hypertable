/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * This program is distributed in the hope that it will be useful,
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

#include "Common/FileUtils.h"

#include "CommitLogLocal.h"
#include "Global.h"
#include "MaintenanceThread.h"
#include "RangeServer.h"
#include "RangeServerProtocol.h"

using namespace hypertable;
using namespace std;


/**
 *  Constructor
 */
RangeServer::RangeServer(Comm *comm, Properties *props) : mComm(comm) {
  const char *metadataFile = 0;

  Global::rangeMaxBytes           = props->getPropertyInt64("Hypertable.RangeServer.Range.MaxBytes", 200000000LL);
  Global::localityGroupMaxFiles   = props->getPropertyInt("Hypertable.RangeServer.AccessGroup.MaxFiles", 10);
  Global::localityGroupMergeFiles = props->getPropertyInt("Hypertable.RangeServer.AccessGroup.MergeFiles", 4);
  Global::localityGroupMaxMemory  = props->getPropertyInt("Hypertable.RangeServer.AccessGroup.MaxMemory", 4000000);

  uint64_t blockCacheMemory = props->getPropertyInt64("Hypertable.RangeServer.BlockCache.MaxMemory", 200000000LL);
  Global::blockCache = new FileBlockCache(blockCacheMemory);

  assert(Global::localityGroupMergeFiles <= Global::localityGroupMaxFiles);

  const char *dir = props->getProperty("Hypertable.RangeServer.logDirRoot", 0);
  if (dir == 0) {
    cerr << "Hypertable.RangeServer.logDirRoot property not specified." << endl;
    exit(1);
  }
  if (dir[strlen(dir)-1] == '/')
    Global::logDirRoot = string(dir, strlen(dir)-1);
  else
    Global::logDirRoot = dir;

  metadataFile = props->getProperty("metadata");
  assert(metadataFile != 0);

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

  /**
   * Create HYPERSPACE Client connection
   */
  Global::hyperspace = new HyperspaceClient(mComm, props);
  if (!Global::hyperspace->WaitForConnection())
    exit(1);

  /**
   * Create HDFS Client connection
   */
  {
    struct sockaddr_in addr;
    const char *host = props->getProperty("HdfsBroker.Server.host", 0);
    if (host == 0) {
      LOG_ERROR("HdfsBroker.Server.host property not specified");
      exit(1);
    }

    int port = props->getPropertyInt("HdfsBroker.Server.port", 0);
    if (port == 0) {
      LOG_ERROR("HdfsBroker.Server.port property not specified");
      exit(1);
    }

    if (Global::verbose) {
      cout << "HdfsBroker.host=" << host << endl;
      cout << "HdfsBroker.port=" << port << endl;
    }

    memset(&addr, 0, sizeof(struct sockaddr_in));
    {
      struct hostent *he = gethostbyname(host);
      if (he == 0) {
	herror("gethostbyname()");
	exit(1);
      }
      memcpy(&addr.sin_addr.s_addr, he->h_addr_list[0], sizeof(uint32_t));
    }
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    Global::hdfsClient = new HdfsClient(mComm, addr, 20);
    if (!Global::hdfsClient->WaitForConnection(30))
      exit(1);
  }

  if (DirectoryInitialize(props) != Error::OK)
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
  error = Global::hyperspace->Exists("/hypertable/servers");
  if (error == Error::HYPERTABLEFS_FILE_NOT_FOUND) {
    LOG_ERROR("Hypertablefs directory '/hypertable/servers' does not exist, try running 'Hypertable.Master --initialize' first");
    return error;
  }
  else if (error != Error::OK) {
    LOG_VA_ERROR("Problem checking existence of '/hypertable/servers' hypertablefs directory - %s", Error::GetText(error));
    return error;
  }

  gettimeofday(&tval, 0);

  boost::shared_array<char> topDirPtr( new char [ 64 + strlen(ipStr) ] );
  char *topDir = topDirPtr.get();
  sprintf(topDir, "/hypertable/servers/%s_%ld", ipStr, tval.tv_sec);

  /**
   * Create /hypertable/servers/X.X.X.X_nnnnn directory
   */
  error = Global::hyperspace->Exists(topDir);
  if (error == Error::HYPERTABLEFS_FILE_NOT_FOUND) {
    error = Global::hyperspace->Mkdirs(topDir);
    if (error != Error::OK) {
      LOG_VA_ERROR("Problem creating hypertablefs directory '%s' - %s", topDir, Error::GetText(error));
      return error;
    }
  }
  else if (error != Error::OK) {
    LOG_VA_ERROR("Problem checking existence of '%s' hypertablefs directory - %s", topDir, Error::GetText(error));
    return error;
  }
  else {
    LOG_VA_ERROR("Hypertablefs directory '%s' already exists.", topDir);
    return -1;
  }

  Global::logDir = (string)topDir + "/logs/primary";

  /**
   * Create /hypertable/servers/X.X.X.X/logs/primary directory
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
 *
 */
void RangeServer::Compact(ResponseCallback *cb, TabletIdentifierT *tablet, uint8_t compactionType, const char *accessGroup) {
  std::string tableFile = (std::string)"/hypertable/tables/" + tablet->tableName;
  int error = Error::OK;
  std::string errMsg;
  std::string tableName = tablet->tableName;
  TableInfoPtr tableInfoPtr;
  RangePtr rangePtr;
  uint64_t commitTime = 0;
  string startRow = tablet->startRow;
  MaintenanceThread::WorkType workType = MaintenanceThread::COMPACTION_MINOR;
  bool major = false;

  // Check for major compaction
  if (compactionType == 1) {
    major = true;
    workType = MaintenanceThread::COMPACTION_MAJOR;
  }

  if (Global::verbose) {
    cout << "Table file = " << tableFile << endl;
    cout << "Compaction type = " << (major ? "major" : "minor") << endl;
    cout << "Access group = \"" << accessGroup << "\"" << endl;
    cout << "Start row = \"" << tablet->startRow << "\"" << endl;
    cout << "End row = \"" << tablet->endRow << "\"" << endl;
  }

  /**
   * Fetch table info
   */
  if (!Global::GetTableInfo(tableName, tableInfoPtr)) {
    error = Error::RANGESERVER_RANGE_NOT_FOUND;
    errMsg = "No ranges loaded for table '" + tableName + "'";
    goto abort;
  }

  /**
   * Fetch range info
   */
  if (!tableInfoPtr->GetRange(startRow, rangePtr)) {
    error = Error::RANGESERVER_RANGE_NOT_FOUND;
    errMsg = "No range with start row '" + startRow + "' found for table '" + tableName + "'";
    goto abort;
  }

  {
    boost::read_write_mutex::scoped_write_lock lock(Global::log->ReadWriteMutex());
    commitTime = Global::log->GetLastTimestamp();
  }

  MaintenanceThread::ScheduleCompaction(rangePtr.get(), workType, commitTime, accessGroup);

#if 0
  if (accessGroup != "") {
    MaintenanceThread::ScheduleCompaction(lg, commitTime, major);
    lg = rangePtr->GetAccessGroup(accessGroup);
    MaintenanceThread::ScheduleCompaction(lg, commitTime, major);
  }
  else {
    vector<AccessGroup *> accessGroups = rangePtr->AccessGroupVector();
    for (size_t i=0; i< accessGroups.size(); i++)
      MaintenanceThread::ScheduleCompaction(accessGroups[i], commitTime, major);
  }
#endif

  if ((error = cb->response()) != Error::OK) {
    LOG_VA_ERROR("Problem sending OK response - %s", Error::GetText(error));
  }

  if (Global::verbose) {
    if (accessGroup == 0) {
      LOG_VA_INFO("Compaction (%s) scheduled for table '%s' start row '%s', all locality groups", 
		  (major ? "major" : "minor"), tablet->tableName, tablet->startRow);
    }
    else {
      LOG_VA_INFO("Compaction (%s) scheduled for table '%s' start row '%s' access group '%s'",
		  (major ? "major" : "minor"), tablet->tableName, tablet->startRow, accessGroup);
    }
  }

  return;

 abort:
  LOG_VA_ERROR("%s '%s'", Error::GetText(error), errMsg.c_str());
  if ((error = cb->error(error, errMsg)) != Error::OK) {
    LOG_VA_ERROR("Problem sending error response - %s", Error::GetText(error));
  }

  return;
}



void RangeServer::CreateScanner(ResponseCallbackCreateScanner *cb, TabletIdentifierT *tablet, ScannerSpecT *spec) {
#if 0
  cout << "Table name = " << tableName << endl;
  cout << "Start row  = " << startRow << endl;
  cout << "End row  = " << endRow << endl;
  cout << "Table generation = " << generation << endl;
  for (int32_t i=0; i<columnCount; i++) {
    cout << "Column = " << (int)columns[i] << endl;
  }
  if (startKey != 0) {
    rowkey = string((const char *)startKey->data, startKey->len);
    cout << "Start Key = " << rowkey << endl;
  }
  else
    cout << "Start Key = \"\"" << endl;

  if (endKey != 0) {
    rowkey = string((const char *)endKey->data, endKey->len);
    cout << "End Key = " << rowkey << endl;
  }
  else
    cout << "End Key = \"\"" << endl;
  cout << "Start Time = " << startTime << endl;
  cout << "End Time = " << endTime << endl;

  if (!Global::GetTableInfo(tableName, tableInfoPtr)) {
    error = Error::RANGESERVER_RANGE_NOT_FOUND;
    errMsg = "No ranges for table '" + tableName + "' loaded";
    goto abort;
  }

  if (!tableInfoPtr->GetRange(startRow, rangePtr)) {
    error = Error::RANGESERVER_RANGE_NOT_FOUND;
    errMsg = startRow;
    goto abort;
  }

  kvBuffer = new uint8_t [ sizeof(int32_t) + DEFAULT_SCANBUF_SIZE ];
  kvLenp = (uint32_t *)kvBuffer;

  cerr << "About to scan ..." << endl;

  rangePtr->LockShareable();
  if (columnFamilies.size() > 0) 
    scannerPtr.reset( rangePtr->CreateScanner(columnFamilies, true) );
  else
    scannerPtr.reset( rangePtr->CreateScanner(true) );
  scannerPtr->RestrictRange(startKey, endKey);
  if (columnCount > 0)
    scannerPtr->RestrictColumns(columns, columnCount);
  scannerPtr->Reset();
  more = FillScanBlock(scannerPtr, kvBuffer+sizeof(int32_t), DEFAULT_SCANBUF_SIZE, kvLenp);
  if (more)
    id = Global::scannerMap.Put(scannerPtr, rangePtr);

  rangePtr->UnlockShareable();

  cerr << "Finished to scan ..." << endl;

  /**
   *  Send back data
   */

  cbuf = new CommBuf(mbuilder.HeaderLength() + 12);
  cbuf->SetExt(kvBuffer, sizeof(int32_t) + *kvLenp);
  cbuf->PrependInt(id);   // scanner ID
  if (more)
    cbuf->PrependShort(0);
  else
    cbuf->PrependShort(1);
  cbuf->PrependShort(RangeServerProtocol::COMMAND_CREATE_SCANNER);
  cbuf->PrependInt(Error::OK);
  mbuilder.LoadFromMessage(mEvent.header);
  mbuilder.Encapsulate(cbuf);
  if ((error = Global::comm->SendResponse(mEvent.addr, cbuf)) != Error::OK) {
    LOG_VA_ERROR("Comm.SendResponse returned '%s'", Error::GetText(error));
  }

  error = Error::OK;

  if (Global::verbose) {
    LOG_VA_INFO("Successfully created scanner on table '%s'", tableName.c_str());
  }

 abort:
  if (error != Error::OK) {
    if (Global::verbose) {
      LOG_VA_ERROR("%s '%s'", Error::GetText(error), errMsg.c_str());
    }
    cbuf = Global::protocol->CreateErrorMessage(RangeServerProtocol::COMMAND_CREATE_SCANNER, error,
						errMsg.c_str(), mbuilder.HeaderLength());
    mbuilder.LoadFromMessage(mEvent.header);
    mbuilder.Encapsulate(cbuf);
    if ((error = Global::comm->SendResponse(mEvent.addr, cbuf)) != Error::OK) {
      LOG_VA_ERROR("Comm.SendResponse returned '%s'", Error::GetText(error));
    }
  }
  return;
#endif  
}


void RangeServer::FetchScanblock(ResponseCallbackFetchScanblock *cb, uint32_t scannerId) {
}

void RangeServer::LoadRange(ResponseCallback *cb, TabletIdentifierT *tablet) {
}

void RangeServer::Update(ResponseCallbackUpdate *cb, TabletIdentifierT *tablet, BufferT &buffer) {
}
