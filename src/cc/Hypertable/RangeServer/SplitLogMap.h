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
