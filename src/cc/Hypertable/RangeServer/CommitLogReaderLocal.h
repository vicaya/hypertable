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
#ifndef HYPERTABLE_COMMITLOGREADERLOCAL_H
#define HYPERTABLE_COMMITLOGREADERLOCAL_H

#include <string>
#include <vector>

extern "C" {
#include <stdio.h>
}

#include <boost/thread/mutex.hpp>

#include "CommitLog.h"
#include "CommitLogReader.h"
#include "Key.h"

using namespace hypertable;

namespace hypertable {

  typedef struct {
    uint32_t     num;
    std::string  fname;
    uint64_t     timestamp;
  } LogFileInfoT;

  inline bool operator<(const LogFileInfoT &lfi1, const LogFileInfoT &lfi2) {
    return lfi1.num < lfi2.num;
  }

  class CommitLogReaderLocal : public CommitLogReader {
  public:
    CommitLogReaderLocal(std::string &logDirRoot, std::string &logDir);
    virtual ~CommitLogReaderLocal() { return; }
    virtual void InitializeRead(uint64_t timestamp);
    virtual bool NextBlock(CommitLogHeaderT **blockp);

  private:
    std::string            mLogDir;
    vector<LogFileInfoT>   mLogFileInfo;
    uint64_t               mCutoffTime;
    size_t                 mCurLogOffset;
    FILE                  *mFp;
    DynamicBuffer          mBlockBuffer;
  };
}

#endif // HYPERTABLE_COMMITLOGREADERLOCAL_H

