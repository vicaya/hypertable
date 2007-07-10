/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <algorithm>
#include <iterator>
#include <vector>

#include <boost/shared_ptr.hpp>

#include "Common/Error.h"
#include "Common/md5.h"

#include "AccessGroup.h"
#include "CellCache.h"
#include "CellStoreV0.h"
#include "Global.h"
#include "MergeScanner.h"


AccessGroup::AccessGroup(Schema::AccessGroup *lg, RangeInfoPtr &rangeInfoPtr) : CellList(), mRwMutex(), mName(lg->name), mStores(), mNextTableId(0), mLogCutoffTime(0), mBusy(false), mDiskUsage(0), mSplitKeys() {
  rangeInfoPtr->GetTableName(mTableName);
  rangeInfoPtr->GetStartRow(mStartRow);
  rangeInfoPtr->GetEndRow(mEndRow);
  mCellCache = new CellCache();
  for (list<Schema::ColumnFamily *>::iterator iter = lg->columns.begin(); iter != lg->columns.end(); iter++) {
    mColumnFamilies.insert((uint8_t)(*iter)->id);
  }
}


AccessGroup::~AccessGroup() {
  boost::read_write_mutex::scoped_write_lock lock(mRwMutex);
  for (size_t i=0; i<mStores.size(); i++)
    delete mStores[i];
  delete mCellCache;
}

int AccessGroup::Add(const KeyT *key, const ByteString32T *value) {
  boost::read_write_mutex::scoped_write_lock lock(mRwMutex);  
  return mCellCache->Add(key, value);
}

CellListScanner *AccessGroup::CreateScanner(bool suppressDeleted) {
  boost::read_write_mutex::scoped_read_lock lock(mRwMutex);
  MergeScanner *scanner = new MergeScanner(suppressDeleted);
  scanner->AddScanner( mCellCache->CreateScanner(false) );
  for (size_t i=0; i<mStores.size(); i++)
    scanner->AddScanner(mStores[i]->CreateScanner(false));
  return scanner;
}

bool AccessGroup::FamiliesIntersect(std::set<uint8_t> &families) {
  std::set<uint8_t> commonFamilies;
  set_intersection(families.begin(), families.end(), mColumnFamilies.begin(), mColumnFamilies.end(), inserter(commonFamilies, commonFamilies.begin()));
  return commonFamilies.size() > 0;
}


void AccessGroup::GetSplitKeys(SplitKeyQueueT &keyHeap) {
  SplitKeyInfoT ski;
  for (size_t i=0; i<mStores.size(); i++) {
    ski.timestamp = mStores[i]->GetLogCutoffTime();
    ski.key = mStores[i]->GetSplitKey();
    keyHeap.push(ski);
  }
}


uint64_t AccessGroup::DiskUsage() {
  boost::read_write_mutex::scoped_read_lock lock(mRwMutex);
  return mDiskUsage + mCellCache->MemoryUsed();
}



/**
 * This method
KeyT *AccessGroup::GetSplitKey() {
  boost::read_write_mutex::scoped_read_lock lock(mRwMutex);
  uint64_t  chosenTime = 0;
  uint64_t  time;
  KeyT     *chosenKey = 0;
  KeyT     *key;

  for (size_t i=0; i<mStores.size(); i++) {
    time = mStores[i]->GetLogCutoffTime();
    if (chosenTime == 0 || time > chosenTime) {
      chosenKey = mStores[i]->GetSplitKey();
      chosenTime = time;
    }
  }

  return chosenKey;
}
*/

bool AccessGroup::NeedsCompaction() {
  boost::read_write_mutex::scoped_read_lock lock(mRwMutex);
  // should we also lock the memtable?
  if (!mBusy && mCellCache->MemoryUsed() >= (uint32_t)Global::localityGroupMaxMemory)
    return true;
  return false;
}

void AccessGroup::MarkBusy() {
  boost::read_write_mutex::scoped_write_lock lock(mRwMutex);
  mBusy = true;
}

void AccessGroup::UnmarkBusy() {
  boost::read_write_mutex::scoped_write_lock lock(mRwMutex);
  mBusy = false;
}

void AccessGroup::AddCellStore(CellStore *table, uint32_t id) {
  boost::read_write_mutex::scoped_write_lock lock(mRwMutex);      
  if (id >= mNextTableId) 
    mNextTableId = id+1;
  if (table->GetLogCutoffTime() > mLogCutoffTime)
    mLogCutoffTime = table->GetLogCutoffTime();
  mStores.push_back(table);
  mDiskUsage += table->DiskUsage();
}



namespace {
  struct ltCellStore {
    bool operator()(CellStore *cs1, CellStore *cs2) const {
      return !(cs1->DiskUsage() < cs2->DiskUsage());
    }
  };
}


void AccessGroup::RunCompaction(uint64_t timestamp, bool major) {
  std::string cellStoreFile;
  char md5DigestStr[33];
  char filename[16];
  KeyT  *key = 0;
  ByteString32T *value = 0;
  KeyComponentsT keyComps;
  boost::shared_ptr<CellListScanner> scannerPtr;
  RangeInfoPtr rangeInfoPtr;
  size_t tableIndex = 1;
  int error;
  CellStoreV0 *cellStore = 0;
  vector<string> replacedFiles;

  {
    boost::read_write_mutex::scoped_write_lock lock(mRwMutex);

    if (mCellCache->MemoryUsed() == 0) {
      if ((major && mStores.size() <= (size_t)1) || (!major && mStores.size() <= (size_t)Global::localityGroupMaxFiles))
	return;
    }

    if (major) {
      tableIndex = 0;
      LOG_INFO("Starting Major Compaction");
    }
    else if (mStores.size() > (size_t)Global::localityGroupMaxFiles) {
      ltCellStore sortObj;
      sort(mStores.begin(), mStores.end(), sortObj);
      tableIndex = mStores.size() - Global::localityGroupMergeFiles;
      LOG_INFO("Starting Merging Compaction");
    }
    else {
      tableIndex = mStores.size();
      LOG_INFO("Starting Minor Compaction");
    }
  }

  /**
   * Read lock the memtable while we're scanning
   */
  {
    boost::read_write_mutex::scoped_read_lock lock(mRwMutex);

    if (mStartRow == "")
      memset(md5DigestStr, '0', 24);
    else
      md5_string(mStartRow.c_str(), md5DigestStr);
    md5DigestStr[24] = 0;
    sprintf(filename, "cs%d", mNextTableId++);
    cellStoreFile = (string)"/hypertable/tables/" + mTableName + "/" + mName + "/" + md5DigestStr + "/" + filename;

    cellStore = new CellStoreV0(Global::hdfsClient);

    if (cellStore->Create(cellStoreFile.c_str()) != 0) {
      LOG_VA_ERROR("Problem compacting locality group to file '%s'", cellStoreFile.c_str());
      return;
    }

    if (major || tableIndex < mStores.size()) {
      MergeScanner *mscanner = new MergeScanner(major);
      mscanner->AddScanner(mCellCache->CreateScanner(false));
      for (size_t i=tableIndex; i<mStores.size(); i++)
	mscanner->AddScanner(mStores[i]->CreateScanner(false));
      scannerPtr.reset(mscanner);
    }
    else
      scannerPtr.reset(mCellCache->CreateScanner(false));
    scannerPtr->Reset();
    while (scannerPtr->Get(&key, &value)) {
      if (!Load(key, keyComps)) {
	LOG_ERROR("Problem deserializing key/value pair");
	return;
      }
      // this assumes that the timestamp of the oldest entry in all CellStores is less than timestamp
      // this should be asserted somewhere.
      if (keyComps.timestamp <= timestamp)
	cellStore->Add(key, value);
      scannerPtr->Forward();
    }

    string fname;
    for (size_t i=tableIndex; i<mStores.size(); i++) {
      fname = mStores[i]->GetFilename();
      replacedFiles.push_back(fname);  // hack: fix me!
    }
    
    //if (cellStore->Finalize(timestamp, replacedFiles) != 0) {
    if (cellStore->Finalize(timestamp) != 0) {
      LOG_VA_ERROR("Problem finalizing CellStore '%s'", cellStoreFile.c_str());
      return;
    }
  }

  /**
   * Write lock the memtable while we're changing tables and purging
   */
  {
    boost::read_write_mutex::scoped_write_lock lock(mRwMutex);

    /**
     * Close CellStores and delete underlying files
     *
     * hack: fix me!
     */
    for (size_t i=tableIndex; i<mStores.size(); i++) {
      mStores[i]->Close();
      if ((mStores[i]->GetFlags() & CellStore::FLAG_SHARED) == 0) {
	std::string &fname = mStores[i]->GetFilename();
	Global::hdfsClient->Remove(fname.c_str());
      }
      delete mStores[i];
    }

    /**
     * Drop the compacted tables from the table vector
     */
    if (tableIndex < mStores.size())
      mStores.resize(tableIndex);

    /**
     * Add the new table to the table vector
     */
    mStores.push_back(cellStore);

    /**
     * Re-compute disk usage
     */
    mDiskUsage = 0;
    for (size_t i=0; i<mStores.size(); i++)
      mDiskUsage += mStores[i]->DiskUsage();

    /**
     *  Update METADATA with new cellStore information
     */
    if ((error = Global::metadata->GetRangeInfo(mTableName, mStartRow, mEndRow, rangeInfoPtr)) != Error::OK) {
      LOG_VA_ERROR("Unable to find tablet (table='%s' startRow='%s' endRow='%s') in metadata - %s",
		   mTableName.c_str(), mStartRow.c_str(), mEndRow.c_str(), Error::GetText(error));
      exit(1);
    }
    for (vector<string>::iterator iter = replacedFiles.begin(); iter != replacedFiles.end(); iter++)
      rangeInfoPtr->RemoveCellStore(*iter);
    rangeInfoPtr->AddCellStore(cellStoreFile);
    Global::metadata->Sync();

    mCellCache->Purge(timestamp);
  }

  // Compaction thread function should re-shuffle the heap of locality groups and purge the commit log

  LOG_INFO("Finished Compaction");

}

