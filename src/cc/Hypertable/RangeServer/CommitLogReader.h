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
#ifndef HYPERTABLE_COMMITLOGREADER_H
#define HYPERTABLE_COMMITLOGREADER_H

#include <string>
#include <vector>

#include <boost/thread/mutex.hpp>

#include "Hypertable/Lib/Filesystem.h"
#include "Hypertable/Lib/Key.h"

#include "CommitLog.h"
#include "CommitLogReader.h"

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

  class CommitLogReader {
  public:
    CommitLogReader(Filesystem *fs, std::string &logDir);
    virtual ~CommitLogReader() { return; }
    virtual void InitializeRead(uint64_t timestamp);
    virtual bool NextBlock(CommitLogHeaderT **blockp);

  private:
    Filesystem               *mFs;
    std::string               mLogDir;
    std::vector<LogFileInfoT> mLogFileInfo;
    uint64_t                  mCutoffTime;
    size_t                    mCurLogOffset;
    int32_t                   mFd;
    DynamicBuffer             mBlockBuffer;
  };
}

#endif // HYPERTABLE_COMMITLOGREADER_H

