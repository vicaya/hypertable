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

#ifndef HYPERTABLE_SPLITLOGMAP_H
#define HYPERTABLE_SPLITLOGMAP_H

#include <map>

#include <boost/shared_ptr.hpp>

#include "Common/StringExt.h"
#include "Common/read_write_mutex_generic.hpp"

#include "Range.h"

namespace hypertable {
  
  class CommitLog;
  class RangeInfo;

  class SplitLogMap {

  public:

    typedef struct {
      CommitLog  *splitLog;
      RangeInfo *rangeInfo;
      uint64_t    cutoffTime;
    } SplitLogInfoT;

    typedef boost::shared_ptr<SplitLogInfoT> SplitLogInfoPtr;

    SplitLogMap() : mRwMutex() { return; }
    void Insert(SplitLogInfoT *splitLogInfo);
    bool Lookup(string &row, SplitLogInfoPtr &splitLogInfoPtr);

  private:
    typedef std::map<std::string, SplitLogInfoPtr> SplitLogInfoMapT;

    boost::read_write_mutex  mRwMutex;
    SplitLogInfoMapT         mSplitLogInfoMap;
  };

}

#endif // HYPERTABLE_SPLITLOGMAP_H
