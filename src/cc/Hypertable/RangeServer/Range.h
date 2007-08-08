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
#ifndef HYPERTABLE_RANGE_H
#define HYPERTABLE_RANGE_H

#include <map>
#include <string>
#include <vector>

#include <boost/shared_ptr.hpp>
#include <boost/thread/condition.hpp>

#include "Hypertable/Schema.h"

#include "AccessGroup.h"
#include "CellStore.h"
#include "CommitLog.h"
#include "Key.h"
#include "RangeInfo.h"

namespace hypertable {

  class Range : public CellList {

    typedef std::map<string, AccessGroup *> AccessGroupMapT;
    typedef std::vector<AccessGroup *>  ColumnFamilyVectorT;

  public:
    Range(SchemaPtr &schemaPtr, RangeInfoPtr &rangeInfoPtr);
    virtual ~Range() { return; }
    virtual int Add(const KeyT *key, const ByteString32T *value);
    virtual CellListScanner *CreateScanner(bool suppressDeleted);
    virtual void Lock();
    virtual void Unlock();
    virtual KeyT *GetSplitKey();

    uint64_t GetLogCutoffTime();
    uint64_t DiskUsage();
    CellListScanner *CreateScanner(std::set<uint8_t> &columns, bool suppressDeleted);

    string &StartRow() { return mStartRow; }
    string &EndRow() { return mEndRow; }

    void ScheduleMaintenance(uint64_t timestamp);
    void ClearMaintenenceMode() {
      boost::mutex::scoped_lock lock(mMutex);
      mMaintenanceMode = false;
    }
    void DoMaintenance(uint64_t timestamp);

    void IncrementUpdateCounter();
    void DecrementUpdateCounter();

    bool GetSplitInfo(KeyPtr &splitKeyPtr, CommitLogPtr &splitLogPtr, uint64_t *splitStartTime) {
      boost::mutex::scoped_lock lock(mMutex);
      *splitStartTime = mSplitStartTime;
      if (mSplitStartTime == 0)
	return false;
      splitKeyPtr = mSplitKeyPtr;
      splitLogPtr = mSplitLogPtr;
      return true;
    }

    std::vector<AccessGroup *> &AccessGroupVector() { return mAccessGroupVector; }
    AccessGroup *GetAccessGroup(string &lgName) { return mAccessGroupMap[lgName]; }

  private:
    bool NeedMaintenance();
    void ReplayCommitLog(string &logDir, uint64_t minLogCutoff);
    bool ExtractAccessGroupFromPath(std::string &path, std::string &name, uint32_t *tableIdp);

    boost::mutex     mMutex;
    std::string      mTableName;
    SchemaPtr        mSchema;
    std::string      mStartRow;
    std::string      mEndRow;
    KeyPtr           mStartKeyPtr;
    KeyPtr           mEndKeyPtr;
    AccessGroupMapT        mAccessGroupMap;
    std::vector<AccessGroup *>  mAccessGroupVector;
    ColumnFamilyVectorT      mColumnFamilyVector;
    bool       mMaintenanceMode;

    uint64_t         mSplitStartTime;
    KeyPtr           mSplitKeyPtr;
    CommitLogPtr     mSplitLogPtr;

    boost::mutex     mSplitMutex;
    boost::condition mSplitFinishedCond;
    boost::condition mSplitReadyCond;
    bool             mSplitPending;
    uint32_t         mUpdateCounter;
  };

  typedef boost::shared_ptr<Range> RangePtr;

}


#endif // HYPERTABLE_RANGE_H
