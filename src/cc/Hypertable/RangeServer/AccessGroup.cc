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

#include <algorithm>
#include <iterator>
#include <vector>

#include <boost/shared_ptr.hpp>

#include "Common/Error.h"
#include "Common/md5.h"

#include "AccessGroup.h"
#include "CellCache.h"
#include "CellCacheScanner.h"
#include "CellStoreScannerV0.h"
#include "CellStoreV0.h"
#include "Global.h"
#include "MergeScanner.h"


AccessGroup::AccessGroup(Schema::AccessGroup *lg, RangeInfoPtr &rangeInfoPtr) : CellList(), mMutex(), mName(lg->name), mStores(), mCellCachePtr(), mNextTableId(0), mLogCutoffTime(0), mBusy(false), mDiskUsage(0), mSplitKeys() {
  rangeInfoPtr->GetTableName(mTableName);
  rangeInfoPtr->GetStartRow(mStartRow);
  rangeInfoPtr->GetEndRow(mEndRow);
  mCellCachePtr.reset( new CellCache() );
  for (list<Schema::ColumnFamily *>::iterator iter = lg->columns.begin(); iter != lg->columns.end(); iter++) {
    mColumnFamilies.insert((uint8_t)(*iter)->id);
  }
}


AccessGroup::~AccessGroup() {
  return;
}

/**
 * This should be called with the CellCache locked
 * Also, at the end of compaction processing, when mCellCachePtr gets reset to a new value,
 * the CellCache should be locked as well.
 */
int AccessGroup::Add(const KeyT *key, const ByteString32T *value) {
  return mCellCachePtr->Add(key, value);
}


CellListScanner *AccessGroup::CreateScanner(bool suppressDeleted) {
  boost::mutex::scoped_lock lock(mMutex);
  MergeScanner *scanner = new MergeScanner(suppressDeleted);
  scanner->AddScanner( new CellCacheScanner(mCellCachePtr) );
  for (size_t i=0; i<mStores.size(); i++)
    scanner->AddScanner( new CellStoreScannerV0(mStores[i]) );
  return scanner;
}

/**
 * Should we lock here?  what about the addition of column families?
 */
bool AccessGroup::FamiliesIntersect(std::set<uint8_t> &families) {
  boost::mutex::scoped_lock lock(mMutex);
  std::set<uint8_t> commonFamilies;
  set_intersection(families.begin(), families.end(), mColumnFamilies.begin(), mColumnFamilies.end(), inserter(commonFamilies, commonFamilies.begin()));
  return commonFamilies.size() > 0;
}


void AccessGroup::GetSplitKeys(SplitKeyQueueT &keyHeap) {
  boost::mutex::scoped_lock lock(mMutex);
  SplitKeyInfoT ski;
  for (size_t i=0; i<mStores.size(); i++) {
    ski.timestamp = mStores[i]->GetLogCutoffTime();
    ski.key = mStores[i]->GetSplitKey();
    keyHeap.push(ski);
  }
}


uint64_t AccessGroup::DiskUsage() {
  boost::mutex::scoped_lock lock(mMutex);
  return mDiskUsage + mCellCachePtr->MemoryUsed();
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
  boost::mutex::scoped_lock lock(mMutex);
  // should we also lock the memtable?
  if (!mBusy && mCellCachePtr->MemoryUsed() >= (uint32_t)Global::localityGroupMaxMemory)
    return true;
  return false;
}

void AccessGroup::MarkBusy() {
  boost::mutex::scoped_lock lock(mMutex);
  mBusy = true;
}

void AccessGroup::UnmarkBusy() {
  boost::mutex::scoped_lock lock(mMutex);
  mBusy = false;
}

void AccessGroup::AddCellStore(CellStorePtr &cellStorePtr, uint32_t id) {
  boost::mutex::scoped_lock lock(mMutex);
  if (id >= mNextTableId) 
    mNextTableId = id+1;
  if (cellStorePtr->GetLogCutoffTime() > mLogCutoffTime)
    mLogCutoffTime = cellStorePtr->GetLogCutoffTime();
  mStores.push_back(cellStorePtr);
  mDiskUsage += cellStorePtr->DiskUsage();
}



namespace {
  struct ltCellStore {
    bool operator()(const CellStorePtr &csPtr1, const CellStorePtr &csPtr2) const {
      return !(csPtr1->DiskUsage() < csPtr2->DiskUsage());
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
  CellStorePtr cellStorePtr;
  vector<string> replacedFiles;


  if (mCellCachePtr->MemoryUsed() == 0) {
    if ((major && mStores.size() <= (size_t)1) || (!major && mStores.size() <= (size_t)Global::localityGroupMaxFiles))
      return;
  }

  if (major) {
    tableIndex = 0;
    LOG_INFO("Starting Major Compaction");
  }
  else if (mStores.size() > (size_t)Global::localityGroupMaxFiles) {
    boost::mutex::scoped_lock lock(mMutex);
    ltCellStore sortObj;
    sort(mStores.begin(), mStores.end(), sortObj);
    tableIndex = mStores.size() - Global::localityGroupMergeFiles;
    LOG_INFO("Starting Merging Compaction");
  }
  else {
    tableIndex = mStores.size();
    LOG_INFO("Starting Minor Compaction");
  }


  if (mStartRow == "")
    memset(md5DigestStr, '0', 24);
  else
    md5_string(mStartRow.c_str(), md5DigestStr);
  md5DigestStr[24] = 0;
  sprintf(filename, "cs%d", mNextTableId++);
  cellStoreFile = (string)"/hypertable/tables/" + mTableName + "/" + mName + "/" + md5DigestStr + "/" + filename;

  cellStorePtr.reset( new CellStoreV0(Global::hdfsClient) );

  if (cellStorePtr->Create(cellStoreFile.c_str()) != 0) {
    LOG_VA_ERROR("Problem compacting locality group to file '%s'", cellStoreFile.c_str());
    return;
  }

  if (major || tableIndex < mStores.size()) {
    MergeScanner *mscanner = new MergeScanner(major);
    mscanner->AddScanner( new CellCacheScanner(mCellCachePtr) );
    for (size_t i=tableIndex; i<mStores.size(); i++)
      mscanner->AddScanner( new CellStoreScannerV0(mStores[i]) );
    scannerPtr.reset(mscanner);
  }
  else
    scannerPtr.reset( new CellCacheScanner(mCellCachePtr) );

  scannerPtr->Reset();

  while (scannerPtr->Get(&key, &value)) {
    if (!Load(key, keyComps)) {
      LOG_ERROR("Problem deserializing key/value pair");
      return;
    }
    // this assumes that the timestamp of the oldest entry in all CellStores is less than timestamp
    // this should be asserted somewhere.
    if (keyComps.timestamp <= timestamp)
      cellStorePtr->Add(key, value);
    scannerPtr->Forward();
  }

  string fname;
  for (size_t i=tableIndex; i<mStores.size(); i++) {
    fname = mStores[i]->GetFilename();
    replacedFiles.push_back(fname);  // hack: fix me!
  }
    
  if (cellStorePtr->Finalize(timestamp) != 0) {
    LOG_VA_ERROR("Problem finalizing CellStore '%s'", cellStoreFile.c_str());
    return;
  }

  /**
   * HACK: Delete underlying files -- fix me!!!
   */
  for (size_t i=tableIndex; i<mStores.size(); i++) {
    if ((mStores[i]->GetFlags() & CellStore::FLAG_SHARED) == 0) {
      std::string &fname = mStores[i]->GetFilename();
      Global::hdfsClient->Remove(fname.c_str());
    }
  }

  /**
   * Drop the compacted tables from the table vector
   */
  if (tableIndex < mStores.size())
    mStores.resize(tableIndex);

  /**
   * Add the new table to the table vector
   */
  mStores.push_back(cellStorePtr);

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

  /**
   * Create new CellCache with compacted entries sliced off
   */
  mCellCachePtr.reset( mCellCachePtr->SliceCopy(timestamp) );

  // Compaction thread function should re-shuffle the heap of locality groups and purge the commit log

  LOG_INFO("Finished Compaction");
}
