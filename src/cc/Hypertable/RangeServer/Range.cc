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
#include <vector>

extern "C" {
#include <poll.h>
#include <string.h>
}

#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/md5.h"

#include "CellStoreV0.h"
#include "CommitLogLocal.h"
#include "CommitLogReaderLocal.h"
#include "Global.h"
#include "MaintenanceThread.h"
#include "MergeScanner.h"
#include "Range.h"

using namespace hypertable;
using namespace std;



Range::Range(SchemaPtr &schemaPtr, RangeInfoPtr &rangeInfoPtr) : CellList(), mMutex(), mSchema(schemaPtr), mAccessGroupMap(), mAccessGroupVector(), mColumnFamilyVector(), mMaintenanceInProgress(false), mLatestTimestamp(0), mSplitStartTime(0), mSplitKeyPtr(), mSplitLogPtr(), mMaintenanceMutex(), mMaintenanceFinishedCond(), mUpdateQuiesceCond(), mHoldUpdates(false), mUpdateCounter(0) {
  CellStorePtr cellStorePtr;
  std::string accessGroupName;
  AccessGroup *ag;
  uint64_t minLogCutoff = 0;
  uint32_t storeId;
  KeyT *startKey=0, *endKey=0;
  int32_t len;

  rangeInfoPtr->GetTableName(mTableName);
  rangeInfoPtr->GetStartRow(mStartRow);
  rangeInfoPtr->GetEndRow(mEndRow);

  // set up start key
  if (mStartRow != "") {
    len = strlen(mStartRow.c_str()) + 1;
    startKey = (KeyT *)new uint8_t [ sizeof(int32_t) + len ];
    strcpy((char *)startKey->data, mStartRow.c_str());
    startKey->len = len;
    mStartKeyPtr.reset(startKey);
  }

  // set up end key
  if (mEndRow != "") {
    len = strlen(mEndRow.c_str()) + 1;
    endKey = (KeyT *)new uint8_t [ sizeof(int32_t) + len ];
    strcpy((char *)endKey->data, mEndRow.c_str());
    endKey->len = len;
    mEndKeyPtr.reset(endKey);
  }

  mColumnFamilyVector.resize( mSchema->GetMaxColumnFamilyId() + 1 );

  list<Schema::AccessGroup *> *agList = mSchema->GetAccessGroupList();

  for (list<Schema::AccessGroup *>::iterator agIter = agList->begin(); agIter != agList->end(); agIter++) {
    ag = new AccessGroup((*agIter), rangeInfoPtr);
    mAccessGroupMap[(*agIter)->name] = ag;
    mAccessGroupVector.push_back(ag);
    for (list<Schema::ColumnFamily *>::iterator cfIter = (*agIter)->columns.begin(); cfIter != (*agIter)->columns.end(); cfIter++)
      mColumnFamilyVector[(*cfIter)->id] = ag;
  }

  /**
   * Load CellStores
   */
  vector<string> cellStoreVector;
  rangeInfoPtr->GetTables(cellStoreVector);
  for (size_t i=0; i<cellStoreVector.size(); i++) {
    cellStorePtr.reset( new CellStoreV0(Global::hdfsClient) );
    if (!ExtractAccessGroupFromPath(cellStoreVector[i], accessGroupName, &storeId)) {
      LOG_VA_ERROR("Unable to extract locality group name from path '%s'", cellStoreVector[i].c_str());
      continue;
    }
    if (cellStorePtr->Open(cellStoreVector[i].c_str(), startKey, endKey) != Error::OK)
      continue;
    if (cellStorePtr->LoadIndex() != Error::OK)
      continue;
    ag = mAccessGroupMap[accessGroupName];
    if (ag == 0) {
      LOG_VA_ERROR("Unrecognized locality group name '%s' in path '%s'", accessGroupName.c_str(), cellStoreVector[i].c_str());
      continue;
    }
    if (minLogCutoff == 0 || cellStorePtr->GetLogCutoffTime() < minLogCutoff)
      minLogCutoff = cellStorePtr->GetLogCutoffTime();
    ag->AddCellStore(cellStorePtr, storeId);
    //cout << "Just added " << cellStorePtrVector[i].c_str() << endl;
  }

  /**
   * Replay commit log
   */
  string logDir;
  rangeInfoPtr->GetLogDir(logDir);
  if (logDir != "")
    ReplayCommitLog(logDir, minLogCutoff);

  return;
}

bool Range::ExtractAccessGroupFromPath(std::string &path, std::string &name, uint32_t *storeIdp) {
  const char *base = strstr(path.c_str(), mTableName.c_str());
  const char *endptr;

  if (base == 0)
    return false;

  base += strlen(mTableName.c_str());
  if (*base++ != '/')
    return false;
  endptr = strchr(base, '/');
  if (endptr == 0)
    return false;
  name = string(base, endptr-base);

  if ((base = strrchr(path.c_str(), '/')) == 0 || strncmp(base, "/cs", 3))
    *storeIdp = 0;
  else
    *storeIdp = atoi(base+3);
  
  return true;
}


/**
 * TODO: Make this more robust
 */
int Range::Add(const KeyT *key, const ByteString32T *value) {
  KeyComponentsT keyComps;

  if (!Load(key, keyComps)) {
    LOG_ERROR("Problem parsing key!!");
    return 0;
  }

  if (keyComps.columnFamily >= mColumnFamilyVector.size()) {
    LOG_VA_ERROR("Bad column family (%d)", keyComps.columnFamily);
    return 0;
  }

  if (keyComps.timestamp > mLatestTimestamp)
    mLatestTimestamp = keyComps.timestamp;

  if (keyComps.flag == FLAG_DELETE_ROW) {
    for (AccessGroupMapT::iterator iter = mAccessGroupMap.begin(); iter != mAccessGroupMap.end(); iter++) {
      (*iter).second->Add(key, value);
    }
  }
  else if (keyComps.flag == FLAG_DELETE_CELL || keyComps.flag == FLAG_INSERT) {
    mColumnFamilyVector[keyComps.columnFamily]->Add(key, value);
  }
  else {
    LOG_VA_ERROR("Bad flag value (%d)", keyComps.flag);
  }
  return 0;
}


CellListScanner *Range::CreateScanner(bool suppressDeleted) {
  MergeScanner *scanner = new MergeScanner(suppressDeleted);
  for (AccessGroupMapT::iterator iter = mAccessGroupMap.begin(); iter != mAccessGroupMap.end(); iter++)
    scanner->AddScanner((*iter).second->CreateScanner(suppressDeleted));
  return scanner;
}


CellListScanner *Range::CreateScanner(std::set<uint8_t> &columns, bool suppressDeleted) {
  MergeScanner *scanner = new MergeScanner(suppressDeleted);
  for (AccessGroupMapT::iterator iter = mAccessGroupMap.begin(); iter != mAccessGroupMap.end(); iter++) {
    if ((*iter).second->FamiliesIntersect(columns)) {
      scanner->AddScanner((*iter).second->CreateScanner(suppressDeleted));
    }
  }
  return scanner;
}


uint64_t Range::DiskUsage() {
  uint64_t usage = 0;
  for (size_t i=0; i<mAccessGroupVector.size(); i++)
    usage += mAccessGroupVector[i]->DiskUsage();
  return usage;
}

KeyT *Range::GetSplitKey() {
  AccessGroup::SplitKeyQueueT splitKeyHeap;
  ltKey sortObj;
  vector<KeyT *> splitKeys;
  for (size_t i=0; i<mAccessGroupVector.size(); i++)
    mAccessGroupVector[i]->GetSplitKeys(splitKeyHeap);
  for (int32_t i=0; i<Global::localityGroupMaxFiles && !splitKeyHeap.empty(); i++) {
    const AccessGroup::SplitKeyInfoT  &splitKeyInfo = splitKeyHeap.top();
    splitKeys.push_back( splitKeyInfo.key );
    splitKeyHeap.pop();
  }
  sort(splitKeys.begin(), splitKeys.end(), sortObj);
  return splitKeys[splitKeys.size()/2];
}


uint64_t Range::GetLogCutoffTime() {
  uint64_t cutoffTime = mAccessGroupVector[0]->GetLogCutoffTime();
  for (size_t i=1; i<mAccessGroupVector.size(); i++) {
    if (mAccessGroupVector[i]->GetLogCutoffTime() < cutoffTime)
      cutoffTime = mAccessGroupVector[i]->GetLogCutoffTime();
  }
  return cutoffTime;
}


/**
 * 
 */
void Range::ScheduleMaintenance() {
  boost::mutex::scoped_lock lock(mMutex);

  if (mMaintenanceInProgress)
    return;

  // Need split?
  if (DiskUsage() > Global::rangeMaxBytes) {
    mMaintenanceInProgress = true;
    MaintenanceThread::ScheduleMaintenance(this);
  }
  else {
    // Need compaction?
    for (size_t i=0; i<mAccessGroupVector.size(); i++) {
      if (mAccessGroupVector[i]->NeedsCompaction()) {
	mMaintenanceInProgress = true;
	MaintenanceThread::ScheduleMaintenance(this);
	break;
      }
    }
  }
}


void Range::DoMaintenance() {
  assert(mMaintenanceInProgress);
  if (DiskUsage() > Global::rangeMaxBytes) {
    KeyT *key;
    std::string splitPoint;
    std::string splitLogDir;
    char md5DigestStr[33];
    RangeInfoPtr rangeInfoPtr;
    int error;

    key = GetSplitKey();
    assert(key);

    splitPoint = (const char *)key->data;

    /**
     * Create Split LOG
     */
    md5_string(splitPoint.c_str(), md5DigestStr);
    md5DigestStr[24] = 0;
    std::string::size_type pos = Global::logDir.rfind("primary", Global::logDir.length());
    assert (pos != std::string::npos);
    splitLogDir = Global::logDir.substr(0, pos) + md5DigestStr;

    // Create split log dir
    string logDir = Global::logDirRoot + splitLogDir;
    if (!FileUtils::Mkdirs(logDir.c_str())) {
      LOG_VA_ERROR("Problem creating local log directory '%s'", logDir.c_str());
      exit(1);
    }

    /**
     *  Update METADATA with split information
     */
    if ((error = Global::metadata->GetRangeInfo(mTableName, mStartRow, mEndRow, rangeInfoPtr)) != Error::OK) {
      LOG_VA_ERROR("Unable to find range (table='%s' startRow='%s' endRow='%s') in metadata - %s",
		   mTableName.c_str(), mStartRow.c_str(), mEndRow.c_str(), Error::GetText(error));
      exit(1);
    }
    rangeInfoPtr->SetSplitPoint(splitPoint);
    rangeInfoPtr->SetSplitLogDir(splitLogDir);
    Global::metadata->Sync();


    /**
     * Atomically obtain timestamp and install split log
     */
    {
      boost::mutex::scoped_lock lock(mMaintenanceMutex);

      /** block updates **/
      mHoldUpdates = true;
      while (mUpdateCounter > 0)
	mUpdateQuiesceCond.wait(lock);

      mSplitStartTime = mLatestTimestamp;
      mSplitKeyPtr.reset( CreateCopy(key) );
      mSplitLogPtr.reset( new CommitLogLocal(Global::logDirRoot, splitLogDir, 0x100000000LL) );

      /** unblock updates **/
      mHoldUpdates = false;
      mMaintenanceFinishedCond.notify_all();
    }

    /**
     * Perform major compaction
     */
    for (size_t i=0; i<mAccessGroupVector.size(); i++) {
      mAccessGroupVector[i]->RunCompaction(mSplitStartTime, true);  // verify the timestamp
    }

    /**
     * Create METADATA entry for new range
     */
    {
      std::vector<std::string> stores;
      RangeInfoPtr newRangePtr( new RangeInfo() );
      newRangePtr->SetTableName(mTableName);
      newRangePtr->SetStartRow(splitPoint);
      newRangePtr->SetEndRow(mEndRow);
      newRangePtr->SetSplitLogDir(splitLogDir);
      rangeInfoPtr->GetTables(stores);
      for (std::vector<std::string>::iterator iter = stores.begin(); iter != stores.end(); iter++)
	newRangePtr->AddCellStore(*iter);
      Global::metadata->AddRangeInfo(newRangePtr);
      Global::metadata->Sync();
    }

    /**
     *  Shrink range and remove pending split info from METADATA for existing range
     */
    {
      boost::mutex::scoped_lock lock(mMutex);      
      mEndRow = splitPoint;
      splitLogDir = "";
      rangeInfoPtr->SetEndRow(mEndRow);
      rangeInfoPtr->SetSplitLogDir(splitLogDir);
      Global::metadata->Sync();
    }

    /**
     *  Do the split
     */
    {
      boost::mutex::scoped_lock lock(mMaintenanceMutex);

      {
	boost::mutex::scoped_lock lock(mMutex);
	mSplitStartTime = 0;
	mSplitKeyPtr.reset(0);
      }

      /** block updates **/
      mHoldUpdates = true;
      while (mUpdateCounter > 0)
	mUpdateQuiesceCond.wait(lock);

      // at this point, there are no running updates

      // TBD: Do the actual split

      /** unblock updates **/
      mHoldUpdates = false;
      mMaintenanceFinishedCond.notify_all();
    }

    if ((error = mSplitLogPtr->Close(Global::log->GetTimestamp())) != Error::OK) {
      LOG_VA_ERROR("Problem closing split log '%s' - %s", mSplitLogPtr->GetLogDir().c_str(), Error::GetText(error));
    }
    mSplitLogPtr.reset();

    /**
     *  TBD:  Notify Master of split
     */
    LOG_VA_INFO("Split Complete.  New Range: start=%s", splitPoint.c_str());

  }
  else
    RunCompaction(false);

  mMaintenanceInProgress = false;
}


void Range::DoCompaction(bool major) {
  RunCompaction(major);
  mMaintenanceInProgress = false;
}


uint64_t Range::RunCompaction(bool major) {
  uint64_t timestamp;
  
  {
    boost::mutex::scoped_lock lock(mMaintenanceMutex);

    /** block updates **/
    mHoldUpdates = true;
    while (mUpdateCounter > 0)
      mUpdateQuiesceCond.wait(lock);

    timestamp = mLatestTimestamp;

    /** unblock updates **/
    mHoldUpdates = false;
    mMaintenanceFinishedCond.notify_all();
  }

  for (size_t i=0; i<mAccessGroupVector.size(); i++)
    mAccessGroupVector[i]->RunCompaction(timestamp, major);

  return timestamp;
}



void Range::Lock() {
  for (AccessGroupMapT::iterator iter = mAccessGroupMap.begin(); iter != mAccessGroupMap.end(); iter++)
    (*iter).second->Lock();
}


void Range::Unlock() {
  for (AccessGroupMapT::iterator iter = mAccessGroupMap.begin(); iter != mAccessGroupMap.end(); iter++)
    (*iter).second->Unlock();
}


/**
 * This whole thing needs to be optimized.  Instead of having each range load
 * read the entire log file, the following should happen:
 * 1. When a range server dies, the master should do the following:
 *   - Determine the new range assignment
 *   - Sort the commit log of the dead range server
 *   - Ask the new range servers to laod the ranges and provide the
 *     the new range servers with the portions of the logs that are
 *     relevant to the range being loaded.
 *
 * TODO: This method should make sure replayed log updates are successfully commited before inserting
 *       them into the memtables
 *
 * FIX ME!!!!
 */
void Range::ReplayCommitLog(string &logDir, uint64_t minLogCutoff) {
  string dummy = "";
  CommitLogReader *clogReader = new CommitLogReaderLocal(logDir, dummy);
  CommitLogHeaderT *header;  
  string tableName;
  const uint8_t *modPtr, *modEnd, *modBase;
  int32_t keyLen, valueLen;
  string rowkey;
  DynamicBuffer goBuffer(65536);
  uint8_t *goNext = goBuffer.buf;
  uint8_t *goEnd = goBuffer.buf + 65536;
  size_t len;
  size_t count = 0;
  size_t nblocks = 0;
  pair<KeyT *, ByteString32T *>  kvPair;
  queue< pair<KeyT *, ByteString32T *> >  insertQueue;
  KeyComponentsT keyComps;
  uint64_t cutoffTime;
  
  clogReader->InitializeRead(0);

  while (clogReader->NextBlock(&header)) {
    tableName = (const char *)&header[1];
    nblocks++;

    if (mTableName == tableName) {
      modPtr = (uint8_t *)&header[1] + strlen((const char *)&header[1]) + 1;
      modEnd = (uint8_t *)header + header->length;
    
      while (modPtr < modEnd) {
	modBase = modPtr;
	if (!Load((KeyT *)modBase, keyComps)) {
	  LOG_ERROR("Problem deserializing key/value pair from commit log, skipping block...");
	  break;
	}
	memcpy(&keyLen, modPtr, sizeof(int32_t));
	rowkey = keyComps.rowKey;
	modPtr += sizeof(int32_t) + keyLen;
	memcpy(&valueLen, modPtr, sizeof(int32_t));
	modPtr += sizeof(int32_t) + valueLen;

	// TODO: Check for valid column family!!!
	if (keyComps.columnFamily == 0 || keyComps.columnFamily >= mColumnFamilyVector.size())
	  cutoffTime = minLogCutoff;
	else
	  cutoffTime = mColumnFamilyVector[keyComps.columnFamily]->GetLogCutoffTime();

	if (rowkey < mEndRow && mStartRow <= rowkey && keyComps.timestamp > cutoffTime) {
	  len = modPtr - modBase;
	  if (len > (size_t)(goEnd-goNext)) {
	    // Commit them to current log
	    if (Global::log->Write(mTableName.c_str(), goBuffer.buf, goNext-goBuffer.buf, header->timestamp) != Error::OK)
	      return;
	    // Replay them into memory
	    while (!insertQueue.empty()) {
	      Add(insertQueue.front().first, insertQueue.front().second);
	      insertQueue.pop();
	    }
	    goNext = goBuffer.buf;
	  }
	  assert(len <= (size_t)(goEnd-goNext));
	  memcpy(goNext, modBase, len);
	  kvPair.first = (KeyT *)goNext;
	  kvPair.second = (ByteString32T *)(goNext + sizeof(int32_t) + keyLen);
	  goNext += len;
	  insertQueue.push(kvPair);
	  count++;
	}
      }
    }
  }

  if (goNext > goBuffer.buf) {
    if (Global::log->Write(mTableName.c_str(), goBuffer.buf, goNext-goBuffer.buf, header->timestamp) != Error::OK)
      return;
    // Replay them into memory
    while (!insertQueue.empty()) {
      Add(insertQueue.front().first, insertQueue.front().second);
      insertQueue.pop();
    }
  }

  LOG_VA_INFO("LOAD RANGE replayed %d updates (%d blocks) from commit log '%s'", count, nblocks, logDir.c_str());

}


/**
 *
 */
void Range::IncrementUpdateCounter() {
  boost::mutex::scoped_lock lock(mMaintenanceMutex);
  while (mHoldUpdates)
    mMaintenanceFinishedCond.wait(lock);
  mUpdateCounter++;
}


/**
 *
 */
void Range::DecrementUpdateCounter() {
  boost::mutex::scoped_lock lock(mMaintenanceMutex);
  mUpdateCounter--;
  if (mHoldUpdates && mUpdateCounter == 0)
    mUpdateQuiesceCond.notify_one();
}

