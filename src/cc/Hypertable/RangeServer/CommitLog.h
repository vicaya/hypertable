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

#ifndef HYPERTABLE_COMMITLOG_H
#define HYPERTABLE_COMMITLOG_H

#include <queue>
#include <string>

#include "Common/read_write_mutex_generic.hpp"

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
    int Purge(uint64_t timestamp);
    std::string &GetLogDir() { return mLogDir; }
    boost::read_write_mutex &ReadWriteMutex() { return mRwMutex; }
    uint64_t GetLastTimestamp() { return mLastTimestamp; }

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
    boost::read_write_mutex    mRwMutex;
    std::queue<CommitLogFileInfoT>   mFileInfoQueue;
    uint64_t                   mLastTimestamp;
  };
}

#endif // HYPERTABLE_COMMITLOG_H

