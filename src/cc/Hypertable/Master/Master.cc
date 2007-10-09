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

extern "C" {
#include <poll.h>
}

#include "DfsBroker/Lib/Client.h"
#include "Hypertable/Lib/Schema.h"
#include "Hyperspace/DirEntry.h"

#include "Master.h"
#include "ServersDirectoryHandler.h"
#include "ServerLockFileHandler.h"
#include "RangeServerState.h"

using namespace Hyperspace;
using namespace hypertable;
using namespace hypertable::DfsBroker;
using namespace std;

Master::Master(ConnectionManager *connManager, PropertiesPtr &propsPtr, ApplicationQueue *appQueue) : mAppQueue(appQueue), mVerbose(false), mHyperspace(0), mDfsClient(0) {
  int error;
  Client *dfsClient;

  mHyperspace = new Hyperspace::Session(connManager->GetComm(), propsPtr, &mHyperspaceSessionHandler);

  if (!mHyperspace->WaitForConnection(30)) {
    LOG_ERROR("Unable to connect to hyperspace, exiting...");
    exit(1);
  }

  mVerbose = propsPtr->getPropertyBool("verbose", false);

  /**
   * Create DFS Client connection
   */
  dfsClient = new Client(connManager, propsPtr);

  if (mVerbose) {
    cout << "DfsBroker.host=" << propsPtr->getProperty("DfsBroker.host", "") << endl;
    cout << "DfsBroker.port=" << propsPtr->getProperty("DfsBroker.port", "") << endl;
    cout << "DfsBroker.timeout=" << propsPtr->getProperty("DfsBroker.timeout", "") << endl;
  }

  if (!dfsClient->WaitForConnection(30)) {
    LOG_ERROR("Unable to connect to DFS Broker, exiting...");
    exit(1);
  }

  mDfsClient = dfsClient;

  /* Read Last Table ID */
  {
    DynamicBuffer valueBuf(0);
    HandleCallbackPtr nullHandleCallback;
    int ival;
    uint32_t lockStatus;
    uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_LOCK;

    if ((error = mHyperspace->Open("/hypertable/master", oflags, nullHandleCallback, &mMasterFileHandle)) != Error::OK) {
      LOG_VA_ERROR("Unable to open Hyperspace file '/hypertable/master' (%s)  Try re-running with --initialize",
		   Error::GetText(error));
      exit(1);
    }

    if ((error = mHyperspace->TryLock(mMasterFileHandle, LOCK_MODE_EXCLUSIVE, &lockStatus, &mMasterFileSequencer)) != Error::OK) {
      LOG_VA_ERROR("Problem obtaining exclusive lock on master Hyperspace file '/hypertable/master' - %s", Error::GetText(error));
      exit(1);
    }

    if (lockStatus != LOCK_STATUS_GRANTED) {
      LOG_ERROR("Unable to obtain lock on '/hypertable/master' - conflict");
      exit(1);
    }

    if ((error = mHyperspace->AttrGet(mMasterFileHandle, "last_table_id", valueBuf)) != Error::OK) {
      LOG_VA_ERROR("Problem getting attribute 'last_table_id' from file /hypertable/master - %s", Error::GetText(error));
      exit(1);
    }

    assert(valueBuf.fill() == sizeof(int32_t));

    memcpy(&ival, valueBuf.buf, sizeof(int32_t));

    atomic_set(&mLastTableId, ival);
    if (mVerbose)
      cout << "Last Table ID: " << ival << endl;
  }

  /**
   * Locate tablet servers
   */
  ScanServersDirectory();

}



/**
 * 
 */
void Master::ScanServersDirectory() {
  boost::mutex::scoped_lock lock(mMutex);
  int error;
  HandleCallbackPtr lockFileHandler;
  std::vector<struct DirEntryT> listing;
  uint32_t lockStatus;
  struct LockSequencerT lockSequencer;
  RangeServerStatePtr statePtr;
  uint32_t oflags;

  /**
   * Open /hyperspace/servers directory and scan for range servers
   */
  mServersDirCallbackPtr = new ServersDirectoryHandler(mAppQueue);

  if ((error = mHyperspace->Open("/hypertable/servers", OPEN_FLAG_READ, mServersDirCallbackPtr, &mServersDirHandle)) != Error::OK) {
    LOG_VA_ERROR("Unable to open Hyperspace directory '/hypertable/servers' (%s)  Try re-running with --initialize",
		 Error::GetText(error));
    exit(1);
  }

  if ((error = mHyperspace->Readdir(mServersDirHandle, listing)) != Error::OK) {
    LOG_VA_ERROR("Problem scanning Hyperspace directory '/hypertable/servers' - %s", Error::GetText(error));
    exit(1);
  }

  poll(0, 0, 1000);  // hack to wait for servers to obtain locks since we don't have an atomic open/lock command.

  oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_LOCK;

  for (size_t i=0; i<listing.size(); i++) {

    statePtr = new RangeServerState();
    statePtr->hyperspaceFileName = "/hypertable/servers/" + listing[i].name;

    lockFileHandler = new ServerLockFileHandler(mAppQueue);

    if ((error = mHyperspace->Open(statePtr->hyperspaceFileName, oflags, lockFileHandler, &statePtr->hyperspaceHandle)) != Error::OK) {
      LOG_VA_ERROR("Problem opening discovered server file %s - %s", statePtr->hyperspaceFileName.c_str(), Error::GetText(error));
      continue;
    }

    if ((error = mHyperspace->TryLock(statePtr->hyperspaceHandle, LOCK_MODE_EXCLUSIVE, &lockStatus, &lockSequencer)) != Error::OK) {
      LOG_VA_ERROR("Problem obtaining exclusive lock on master Hyperspace file '/hypertable/master' - %s", Error::GetText(error));
      continue;
    }

    if (lockStatus == LOCK_STATUS_GRANTED) {
      LOG_VA_INFO("Obtained lock on servers file %s, removing...", statePtr->hyperspaceFileName.c_str());
      if ((error = mHyperspace->Delete(statePtr->hyperspaceFileName)) != Error::OK)
	LOG_VA_INFO("Problem deleting Hyperspace file %s", statePtr->hyperspaceFileName.c_str());
      if ((error = mHyperspace->Close(statePtr->hyperspaceHandle)) != Error::OK)
	LOG_VA_INFO("Problem closing handle on deleting Hyperspace file %s", statePtr->hyperspaceFileName.c_str());
    }
    else {
      mServerMap[statePtr->hyperspaceFileName] = statePtr;
    }
  }

}

Master::~Master() {
  delete mHyperspace;
  delete mDfsClient;
}

void Master::CreateTable(ResponseCallback *cb, const char *tableName, const char *schemaString) {
  int error = Error::OK;
  std::string finalSchema = "";
  std::string tableFile = (std::string)"/hypertable/tables/" + tableName;
  std::string errMsg;
  string tableBaseDir;
  string lgDir;
  Schema *schema = 0;
  list<Schema::AccessGroup *> *lgList;
  bool exists;
  HandleCallbackPtr nullHandleCallback;
  uint64_t handle;

  /**
   * Check for table existence
   */
  if ((error = mHyperspace->Exists(tableFile, &exists)) != Error::OK) {
    errMsg = (std::string)"Problem checking for existence of table file '" + tableFile + "'";
    goto abort;
  }
  if (exists) {
    errMsg = tableName;
    error = Error::MASTER_TABLE_EXISTS;
    goto abort;
  }

  /**
   *  Parse Schema and assign Generation number and Column ids
   */
  schema = Schema::NewInstance(schemaString, strlen(schemaString));
  if (!schema->IsValid()) {
    errMsg = schema->GetErrorString();
    error = Error::MASTER_BAD_SCHEMA;
    return;
  }
  schema->AssignIds();
  schema->Render(finalSchema);

  /**
   * Create table file
   */
  if ((error = mHyperspace->Open(tableFile, OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE, nullHandleCallback, &handle)) != Error::OK) {
    errMsg = "Unable to create Hyperspace table file '" + tableFile + "' (" + Error::GetText(error) + ")";
    goto abort;
  }

  /**
   * Write 'table_id' attribute of table file and 'last_table_id' attribute of /hypertable/master
   */
  {
    uint32_t tableId = (uint32_t)atomic_inc_return(&mLastTableId);

    if ((error = mHyperspace->AttrSet(mMasterFileHandle, "last_table_id", &tableId, sizeof(int32_t))) != Error::OK) {
      errMsg = (std::string)"Problem setting attribute 'last_table_id' of file /hypertable/master - " + Error::GetText(error);
      goto abort;
    }

    if ((error = mHyperspace->AttrSet(handle, "table_id", &tableId, sizeof(int32_t))) != Error::OK) {
      errMsg = (std::string)"Problem setting attribute 'table_id' of file " + tableFile + " - " + Error::GetText(error);
      goto abort;
    }
  }


  /**
   * Write schema attribute
   */
  if ((error = mHyperspace->AttrSet(handle, "schema", finalSchema.c_str(), strlen(finalSchema.c_str()))) != Error::OK) {
    errMsg = (std::string)"Problem creating attribute 'schema' for table file '" + tableFile + "'";
    goto abort;
  }

  mHyperspace->Close(handle);

  /**
   * Create /hypertable/tables/<table>/<accessGroup> directories for this table in HDFS
   */
  tableBaseDir = (string)"/hypertable/tables/" + tableName + "/";
  lgList = schema->GetAccessGroupList();
  for (list<Schema::AccessGroup *>::iterator lgIter = lgList->begin(); lgIter != lgList->end(); lgIter++) {
    lgDir = tableBaseDir + (*lgIter)->name;
    if ((error = mDfsClient->Mkdirs(lgDir)) != Error::OK) {
      errMsg = (string)"Problem creating table directory '" + lgDir + "'";
      goto abort;
    }
  }

  cb->response_ok();

  if (mVerbose) {
    LOG_VA_INFO("Successfully created table '%s'", tableName);
  }

 abort:
  delete schema;
  if (error != Error::OK) {
    if (mVerbose) {
      LOG_VA_ERROR("%s '%s'", Error::GetText(error), errMsg.c_str());
    }
    cb->error(error, errMsg);
  }
}

void Master::GetSchema(ResponseCallbackGetSchema *cb, const char *tableName) {
  int error = Error::OK;
  std::string tableFile = (std::string)"/hypertable/tables/" + tableName;
  std::string errMsg;
  DynamicBuffer schemaBuf(0);
  uint64_t handle;
  bool exists;

  /**
   * Check for table existence
   */
  if ((error = mHyperspace->Exists(tableFile, &exists)) != Error::OK) {
    errMsg = (std::string)"Problem checking for existence of table '" + tableName + "' - " + Error::GetText(error);
    goto abort;
  }

  /**
   * Get schema attribute
   */
  if ((error = mHyperspace->AttrGet(handle, "schema", schemaBuf)) != Error::OK) {
    errMsg = "Problem getting attribute 'schema' for table file '" + tableFile + "'";
    goto abort;
  }

  mHyperspace->Close(handle);
  
  cb->response((char *)schemaBuf.buf);

  if (mVerbose) {
    LOG_VA_INFO("Successfully fetched schema (length=%d) for table '%s'", strlen((char *)schemaBuf.buf), tableName);
  }

 abort:
  if (error != Error::OK) {
    if (mVerbose) {
      LOG_VA_ERROR("%s '%s'", Error::GetText(error), errMsg.c_str());
    }
    cb->error(error, errMsg);
  }
  return;
}
