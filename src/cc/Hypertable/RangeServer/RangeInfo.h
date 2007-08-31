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
#ifndef HYPERTABLE_TABLETINFO_H
#define HYPERTABLE_TABLETINFO_H

#include <set>
#include <string>
#include <vector>
#include <iostream>

#include <boost/thread/mutex.hpp>

namespace hypertable {

  class RangeInfo {
  public:
    RangeInfo() : mMutex(), mTableName(""), mStartRow(""), mEndRow(""), mLogDir(""), cellStores(), mSplitLogDir(""), mSplitPoint("") {
      return;
    }

    void GetTableName(std::string &tableName) {
      boost::mutex::scoped_lock lock(mMutex);
      tableName = mTableName;
    }
    void SetTableName(std::string &tableName) {
      boost::mutex::scoped_lock lock(mMutex);
      mTableName = tableName;
    }

    void GetStartRow(std::string &startRow) {
      boost::mutex::scoped_lock lock(mMutex);
      startRow = mStartRow;
    }
    void SetStartRow(std::string &startRow) {
      boost::mutex::scoped_lock lock(mMutex);
      mStartRow = startRow;
    }

    void GetEndRow(std::string &endRow) {
      boost::mutex::scoped_lock lock(mMutex);
      endRow = mEndRow;
    }
    void SetEndRow(std::string &endRow) {
      boost::mutex::scoped_lock lock(mMutex);
      mEndRow = endRow;
    }

    void GetLogDir(std::string &logDir) {
      boost::mutex::scoped_lock lock(mMutex);
      logDir = mLogDir;
    }
    void SetLogDir(std::string &logDir) {
      boost::mutex::scoped_lock lock(mMutex);
      mLogDir = logDir;
    }

    void AddCellStore(std::string &cellStore) {
      boost::mutex::scoped_lock lock(mMutex);
      cellStores.insert(cellStore);
    }
    void RemoveCellStore(std::string &cellStore) {
      boost::mutex::scoped_lock lock(mMutex);
      cellStores.erase(cellStore);
    }
    void GetTables(std::vector<std::string> &tableVec) {
      boost::mutex::scoped_lock lock(mMutex);
      for (std::set<std::string>::iterator iter = cellStores.begin(); iter != cellStores.end(); iter++)
	tableVec.push_back(*iter);
    }

    void GetSplitLogDir(std::string &splitLogDir) {
      boost::mutex::scoped_lock lock(mMutex);
      splitLogDir = mSplitLogDir;
    }
    void SetSplitLogDir(std::string &splitLogDir) {
      boost::mutex::scoped_lock lock(mMutex);
      mSplitLogDir = splitLogDir;
    }

    void GetSplitPoint(std::string &splitPoint) {
      boost::mutex::scoped_lock lock(mMutex);
      splitPoint = mSplitPoint;
    }
    void SetSplitPoint(std::string &splitPoint) {
      boost::mutex::scoped_lock lock(mMutex);
      mSplitPoint = splitPoint;
    }

  private:
    boost::mutex mMutex;
    std::string mTableName;
    std::string mStartRow;
    std::string mEndRow;
    std::string mLogDir;
    std::set<std::string> cellStores;
    std::string mSplitLogDir;
    std::string mSplitPoint;
  };

  typedef boost::shared_ptr<RangeInfo> RangeInfoPtr;

}



#endif // HYPERTABLE_TABLETINFO_H

