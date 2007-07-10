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
#ifndef HYPERTABLE_ACCESSGROUP_H
#define HYPERTABLE_ACCESSGROUP_H

#include <queue>
#include <string>
#include <vector>

#include "Hypertable/Schema.h"

#include "CellCache.h"
#include "CellStore.h"
#include "RangeInfo.h"

using namespace hypertable;

namespace hypertable {

  class AccessGroup : CellList {

  public:

    typedef struct {
      KeyT     *key;
      uint64_t timestamp;
    } SplitKeyInfoT;

    struct ltSplitKeyInfo {
      bool operator()(const SplitKeyInfoT &ski1, const SplitKeyInfoT &ski2) const {
	return ski1.timestamp < ski2.timestamp;
      }
    };

    typedef std::priority_queue<SplitKeyInfoT, vector<SplitKeyInfoT>, ltSplitKeyInfo> SplitKeyQueueT;
    
    AccessGroup(Schema::AccessGroup *lg, RangeInfoPtr &tabletInfoPtr);
    virtual ~AccessGroup();
    virtual int Add(const KeyT *key, const ByteString32T *value);
    virtual CellListScanner *CreateScanner(bool suppressDeleted);
    virtual void Lock() { mRwMutex.lock(); }
    virtual void Unlock() { mRwMutex.unlock(); }
    virtual void LockShareable() { mRwMutex.lock_shareable(); }
    virtual void UnlockShareable() { mRwMutex.unlock_shareable(); }
    virtual void GetSplitKeys(SplitKeyQueueT &keyHeap);

    bool FamiliesIntersect(std::set<uint8_t> &families);
    uint64_t DiskUsage();
    void AddCellStore(CellStore *table, uint32_t id);
    bool NeedsCompaction();
    void RunCompaction(uint64_t timestamp, bool major);
    uint64_t GetLogCutoffTime() { return mLogCutoffTime; }
    void MarkBusy();
    void UnmarkBusy();

  private:
    boost::read_write_mutex  mRwMutex;
    std::set<uint8_t>    mColumnFamilies;
    std::string          mName;
    std::string          mTableName;
    std::string          mStartRow;
    std::string          mEndRow;
    std::vector<CellStore *> mStores;
    CellCache            *mCellCache;
    uint32_t             mNextTableId;
    uint64_t             mLogCutoffTime;
    bool                 mBusy;
    uint64_t             mDiskUsage;
    std::vector<KeyPtr>  mSplitKeys;
  };

}

#endif // HYPERTABLE_ACCESSGROUP_H
