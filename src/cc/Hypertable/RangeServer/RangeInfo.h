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

#include "Common/read_write_mutex_generic.hpp"

namespace hypertable {

  class RangeInfo {
  public:
    RangeInfo() : mRwMutex(), mTableName(""), mStartRow(""), mEndRow(""), mLogDir(""), cellStores(), mSplitLogDir(""), mSplitPoint("") {
      return;
    }

    void GetTableName(std::string &tableName) {
      boost::read_write_mutex::scoped_read_lock lock(mRwMutex);
      tableName = mTableName;
    }
    void SetTableName(std::string &tableName) {
      boost::read_write_mutex::scoped_write_lock lock(mRwMutex);
      mTableName = tableName;
    }

    void GetStartRow(std::string &startRow) {
      boost::read_write_mutex::scoped_read_lock lock(mRwMutex);
      startRow = mStartRow;
    }
    void SetStartRow(std::string &startRow) {
      boost::read_write_mutex::scoped_write_lock lock(mRwMutex);
      mStartRow = startRow;
    }

    void GetEndRow(std::string &endRow) {
      boost::read_write_mutex::scoped_read_lock lock(mRwMutex);
      endRow = mEndRow;
    }
    void SetEndRow(std::string &endRow) {
      boost::read_write_mutex::scoped_write_lock lock(mRwMutex);
      mEndRow = endRow;
    }

    void GetLogDir(std::string &logDir) {
      boost::read_write_mutex::scoped_read_lock lock(mRwMutex);
      logDir = mLogDir;
    }
    void SetLogDir(std::string &logDir) {
      boost::read_write_mutex::scoped_write_lock lock(mRwMutex);
      mLogDir = logDir;
    }

    void AddCellStore(std::string &cellStore) {
      boost::read_write_mutex::scoped_write_lock lock(mRwMutex);
      cellStores.insert(cellStore);
    }
    void RemoveCellStore(std::string &cellStore) {
      boost::read_write_mutex::scoped_write_lock lock(mRwMutex);
      cellStores.erase(cellStore);
    }
    void GetTables(std::vector<std::string> &tableVec) {
      boost::read_write_mutex::scoped_read_lock lock(mRwMutex);
      for (std::set<std::string>::iterator iter = cellStores.begin(); iter != cellStores.end(); iter++)
	tableVec.push_back(*iter);
    }

    void GetSplitLogDir(std::string &splitLogDir) {
      boost::read_write_mutex::scoped_read_lock lock(mRwMutex);
      splitLogDir = mSplitLogDir;
    }
    void SetSplitLogDir(std::string &splitLogDir) {
      boost::read_write_mutex::scoped_write_lock lock(mRwMutex);
      mSplitLogDir = splitLogDir;
    }

    void GetSplitPoint(std::string &splitPoint) {
      boost::read_write_mutex::scoped_read_lock lock(mRwMutex);
      splitPoint = mSplitPoint;
    }
    void SetSplitPoint(std::string &splitPoint) {
      boost::read_write_mutex::scoped_write_lock lock(mRwMutex);
      mSplitPoint = splitPoint;
    }


  private:
    boost::read_write_mutex  mRwMutex;
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

