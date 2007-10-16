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

#include <algorithm>
#include <iterator>
#include <vector>

#include <boost/shared_ptr.hpp>

#include "Common/Error.h"
#include "Common/md5.h"

#include "AccessGroup.h"
#include "CellCache.h"
#include "CellCacheScanner.h"
#include "CellStoreV0.h"
#include "Global.h"
#include "MergeScanner.h"


AccessGroup::AccessGroup(SchemaPtr &schemaPtr, Schema::AccessGroup *lg, RangeInfoPtr &rangeInfoPtr) : CellList(), mMutex(), mLock(mMutex,false), mSchemaPtr(schemaPtr), mName(lg->name), mStores(), mCellCachePtr(), mNextTableId(0), mLogCutoffTime(0), mDiskUsage(0) {
  rangeInfoPtr->GetTableName(mTableName);
  rangeInfoPtr->GetStartRow(mStartRow);
  rangeInfoPtr->GetEndRow(mEndRow);
  mCellCachePtr = new CellCache();
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
int AccessGroup::Add(const ByteString32T *key, const ByteString32T *value) {
  return mCellCachePtr->Add(key, value);
}


CellListScanner *AccessGroup::CreateScanner(ScanContextPtr &scanContextPtr) {
  boost::mutex::scoped_lock lock(mMutex);
  MergeScanner *scanner = new MergeScanner(scanContextPtr);
  scanner->AddScanner( mCellCachePtr->CreateScanner(scanContextPtr) );
  for (size_t i=0; i<mStores.size(); i++)
    scanner->AddScanner( mStores[i]->CreateScanner(scanContextPtr) );
  return scanner;
}

bool AccessGroup::IncludeInScan(ScanContextPtr &scanContextPtr) {
  boost::mutex::scoped_lock lock(mMutex);
  for (std::set<uint8_t>::iterator iter = mColumnFamilies.begin(); iter != mColumnFamilies.end(); iter++) {
    if (scanContextPtr->familyMask[*iter])
      return true;
  }
  return false;
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

bool AccessGroup::NeedsCompaction() {
  boost::mutex::scoped_lock lock(mMutex);
  if (mCellCachePtr->MemoryUsed() >= (uint32_t)Global::localityGroupMaxMemory)
    return true;
  return false;
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
  ByteString32T *key = 0;
  ByteString32T *value = 0;
  Key keyComps;
  boost::shared_ptr<CellListScanner> scannerPtr;
  RangeInfoPtr rangeInfoPtr;
  size_t tableIndex = 1;
  int error;
  CellStorePtr cellStorePtr;
  vector<string> replacedFiles;

  {
    boost::mutex::scoped_lock lock(mMutex);
    if (major) {
      // TODO: if the oldest CellCache entry is newer than timestamp, then return
      if (mCellCachePtr->MemoryUsed() == 0 && mStores.size() <= (size_t)1)
	return;
      tableIndex = 0;
      LOG_INFO("Starting Major Compaction");
    }
    else {
      if (mCellCachePtr->MemoryUsed() < (uint32_t)Global::localityGroupMaxMemory)
	return;

      if (mStores.size() > (size_t)Global::localityGroupMaxFiles) {
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
  }

  if (mEndRow == "")
    memset(md5DigestStr, '0', 24);
  else
    md5_string(mEndRow.c_str(), md5DigestStr);
  md5DigestStr[24] = 0;
  sprintf(filename, "cs%d", mNextTableId++);
  cellStoreFile = (string)"/hypertable/tables/" + mTableName + "/" + mName + "/" + md5DigestStr + "/" + filename;

  cellStorePtr = new CellStoreV0(Global::dfs);

  if (cellStorePtr->Create(cellStoreFile.c_str()) != 0) {
    LOG_VA_ERROR("Problem compacting locality group to file '%s'", cellStoreFile.c_str());
    return;
  }

  {
    ScanContextPtr scanContextPtr;

    scanContextPtr.reset( new ScanContext(timestamp, 0, mSchemaPtr) );

    if (major || tableIndex < mStores.size()) {
      MergeScanner *mscanner = new MergeScanner(scanContextPtr, !major);
      mscanner->AddScanner( mCellCachePtr->CreateScanner(scanContextPtr) );
      for (size_t i=tableIndex; i<mStores.size(); i++)
	mscanner->AddScanner( mStores[i]->CreateScanner(scanContextPtr) );
      scannerPtr.reset(mscanner);
    }
    else
      scannerPtr.reset( mCellCachePtr->CreateScanner(scanContextPtr) );
  }

  while (scannerPtr->Get(&key, &value)) {
    if (!keyComps.load(key)) {
      LOG_ERROR("Problem deserializing key/value pair");
      return;
    }
    // this assumes that the timestamp of the oldest entry in all CellStores is less than timestamp
    // this should be asserted somewhere.
    if (keyComps.timestamp <= timestamp)
      cellStorePtr->Add(key, value);
    scannerPtr->Forward();
  }

  {
    boost::mutex::scoped_lock lock(mMutex);
    string fname;
    for (size_t i=tableIndex; i<mStores.size(); i++) {
      fname = mStores[i]->GetFilename();
      replacedFiles.push_back(fname);  // hack: fix me!
    }
  }
    
  if (cellStorePtr->Finalize(timestamp) != 0) {
    LOG_VA_ERROR("Problem finalizing CellStore '%s'", cellStoreFile.c_str());
    return;
  }

  /**
   * HACK: Delete underlying files -- fix me!!!
  for (size_t i=tableIndex; i<mStores.size(); i++) {
    if ((mStores[i]->GetFlags() & CellStore::FLAG_SHARED) == 0) {
      std::string &fname = mStores[i]->GetFilename();
      Global::hdfsClient->Remove(fname.c_str());
    }
  }
  */

  /**
   *  Update METADATA with new cellStore information
   */
  if ((error = Global::metadata->GetRangeInfo(mTableName, mEndRow, rangeInfoPtr)) != Error::OK) {
    LOG_VA_ERROR("Unable to find tablet (table='%s' endRow='%s') in metadata - %s",
		 mTableName.c_str(), mEndRow.c_str(), Error::GetText(error));
    exit(1);
  }
  for (vector<string>::iterator iter = replacedFiles.begin(); iter != replacedFiles.end(); iter++)
    rangeInfoPtr->RemoveCellStore(*iter);
  rangeInfoPtr->AddCellStore(cellStoreFile);
  Global::metadata->Sync();

  /**
   * Install new CellCache and CellStore
   */
  {
    boost::mutex::scoped_lock lock(mMutex);

    /** Slice and install new CellCache **/
    mCellCachePtr = mCellCachePtr->SliceCopy(timestamp);

    /** Drop the compacted tables from the table vector **/
    if (tableIndex < mStores.size())
      mStores.resize(tableIndex);

    /** Add the new table to the table vector **/
    mStores.push_back(cellStorePtr);

    /** Re-compute disk usage **/
    mDiskUsage = 0;
    for (size_t i=0; i<mStores.size(); i++)
      mDiskUsage += mStores[i]->DiskUsage();
  }

  // Compaction thread function should re-shuffle the heap of locality groups and purge the commit log

  LOG_INFO("Finished Compaction");
}
