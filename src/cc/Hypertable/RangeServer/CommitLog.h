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

#ifndef HYPERTABLE_COMMITLOG_H
#define HYPERTABLE_COMMITLOG_H

#include <queue>
#include <string>

extern "C" {
#include <sys/time.h>
}

#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>

namespace hypertable {

  typedef struct {
    char      marker[8];
    uint64_t  timestamp;
    uint32_t  length;
    uint16_t  checksum;
  } __attribute__((packed)) CommitLogHeaderT;

  typedef struct {
    std::string  fname;
    uint64_t     timestamp;
  } CommitLogFileInfoT;

  /**
   * The commit log consists of a series of blocks.  Each block contains a series of one
   * or more key/value pairs that were commtted together.  Each block begins with a header
   * with the following format:
   *
   * "-BLOCK--"   - 8 bytes
   * timestamp    - 8 bytes
   * length       - 4 bytes
   * checksum     - 2 bytes
   * tableName    - variable
   * '\0'         - 1 byte
   * payload      - variable
   *
   * The log ends with an empty block containing the timestamp of the last real block.
   */
  class CommitLog {
  public:
    CommitLog(std::string &logDirRoot, std::string &logDir, int64_t logFileSize);
    virtual ~CommitLog() { return; }
    int Write(const char *tableName, uint8_t *data, uint32_t len, uint64_t timestamp);
    int Close(uint64_t timestamp);
    int Purge(uint64_t timestamp);
    std::string &GetLogDir() { return mLogDir; }

    uint64_t GetTimestamp() { 
      struct timeval tval;
      boost::mutex::scoped_lock lock(mMutex);
      gettimeofday(&tval, 0);
      return ((uint64_t)tval.tv_sec * 1000000LL) + (uint64_t)tval.tv_usec;
    }

  protected:
    virtual int create(std::string &name, int32_t *fdp) = 0;
    virtual int write(int32_t fd, const void *buf, uint32_t amount) = 0;
    virtual int close(int32_t fd) = 0;
    virtual int sync(int32_t fd) = 0;
    virtual int unlink(std::string &name) = 0;

    std::string                mLogDir;
    std::string                mLogFile;
    int64_t                    mMaxFileSize;
    int64_t                    mCurLogLength;
    uint32_t                   mCurLogNum;
    int                        mFd;
    boost::mutex               mMutex;
    std::queue<CommitLogFileInfoT>   mFileInfoQueue;
  };

  typedef boost::shared_ptr<CommitLog> CommitLogPtr;
  
}

#endif // HYPERTABLE_COMMITLOG_H

