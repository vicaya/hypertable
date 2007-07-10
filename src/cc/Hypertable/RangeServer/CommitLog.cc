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

#include <boost/shared_array.hpp>

#include "Common/Error.h"

#include "Global.h"
#include "CommitLog.h"

using namespace hypertable;


/**
 *
 */
CommitLog::CommitLog(std::string &logDirRoot, std::string &logDir, int64_t logFileSize) : mLogDir(), mLogFile(), mMaxFileSize(logFileSize), mCurLogLength(0), mCurLogNum(0), mRwMutex(), mFileInfoQueue(), mLastTimestamp(0) {
  char buf[32];

  if (logDirRoot.find('/', logDirRoot.length()-1) == string::npos)
    mLogDir = logDirRoot + logDir;
  else
    mLogDir = logDirRoot.substr(0, logDirRoot.length()-1) + logDir;

  if (mLogDir.find('/', mLogDir.length()-1) == string::npos)
    mLogDir += "/";

  sprintf(buf, "%d", mCurLogNum);
  
  mLogFile = mLogDir + buf;
}



/**
 * 
 */
int CommitLog::Write(const char *tableName, uint8_t *data, uint32_t len, uint64_t timestamp) {
  uint32_t totalLen = sizeof(CommitLogHeaderT) + strlen(tableName) + 1 + len;
  boost::shared_array<uint8_t> blockPtr(new uint8_t [ totalLen ]);
  uint8_t *block = blockPtr.get();
  CommitLogHeaderT *header = (CommitLogHeaderT *)block;
  uint8_t *ptr, *endptr;
  uint16_t checksum = 0;
  CommitLogFileInfoT fileInfo;
  int error;

  // marker
  memcpy(header->marker, "-BLOCK--", 8);

  mLastTimestamp = timestamp;
  header->timestamp = timestamp;

  // length
  header->length = totalLen;
  
  // checksum (zero for now)
  header->checksum = 0;

  // '\0' terminated table name
  ptr = block + sizeof(CommitLogHeaderT);
  strcpy((char *)ptr, tableName);
  ptr += strlen((char *)ptr) + 1;

  memcpy(ptr, data, len);

  // compute checksum
  endptr = block + totalLen;
  for (ptr=block; ptr<endptr; ptr++)
    checksum += *ptr;
  header->checksum = checksum;

  if ((error = this->write(mFd, block, totalLen)) != Error::OK)
    return error;

  if ((error = this->sync(mFd)) != Error::OK)
    return error;

  mCurLogLength += totalLen;

  /**
   * Roll the log
   */
  if (mCurLogLength > mMaxFileSize) {
    char buf[32];

    /**
     *  Write trailing empty block
     */
    checksum = 0;
    header->checksum = 0;
    header->length = sizeof(CommitLogHeaderT);
    endptr = (uint8_t *)&header[1];
    for (ptr=block; ptr<endptr; ptr++)
      checksum += *ptr;
    header->checksum = checksum;

    if ((error = this->write(mFd, header, sizeof(CommitLogHeaderT))) != Error::OK)
      return error;

    if ((error = this->sync(mFd)) != Error::OK)
      return error;

    if ((error = this->close(mFd)) != Error::OK)
      return error;

    fileInfo.timestamp = timestamp;
    fileInfo.fname = mLogFile;
    mFileInfoQueue.push(fileInfo);

    mCurLogNum++;
    sprintf(buf, "%d", mCurLogNum);
    mLogFile = mLogDir + buf;

    if ((error = this->create(mLogFile, &mFd)) != Error::OK)
      return error;

    mCurLogLength = 0;
  }

  return Error::OK;
}



/**
 * 
 */
int CommitLog::Purge(uint64_t timestamp) {
  CommitLogFileInfoT fileInfo;
  int error = Error::OK;

  while (!mFileInfoQueue.empty()) {
    fileInfo = mFileInfoQueue.front();
    if (fileInfo.timestamp < timestamp) {
      // should do something on error, but for now, just move on
      if ((error = this->unlink(fileInfo.fname)) == Error::OK)
	mFileInfoQueue.pop();
    }
    else
      break;
  }
  return Error::OK;
}
