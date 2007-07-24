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
    std::string               mLogDir;
    std::vector<LogFileInfoT> mLogFileInfo;
    uint64_t                  mCutoffTime;
    size_t                    mCurLogOffset;
    FILE                     *mFp;
    DynamicBuffer             mBlockBuffer;
  };
}

#endif // HYPERTABLE_COMMITLOGREADERLOCAL_H

