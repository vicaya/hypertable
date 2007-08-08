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

#include <boost/shared_array.hpp>

#include "Common/Error.h"

#include "Global.h"
#include "CommitLog.h"

using namespace hypertable;


/**
 *
 */
CommitLog::CommitLog(std::string &logDirRoot, std::string &logDir, int64_t logFileSize) : mLogDir(), mLogFile(), mMaxFileSize(logFileSize), mCurLogLength(0), mCurLogNum(0), mMutex(), mFileInfoQueue() {
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
int CommitLog::Close(uint64_t timestamp) {
  CommitLogHeaderT header;
  int error = Error::OK;
  memcpy(header.marker, "-BLOCK--", 8);
  header.timestamp = timestamp;
  header.checksum = 0;
  header.length = sizeof(CommitLogHeaderT);

  if ((error = this->write(mFd, &header, sizeof(CommitLogHeaderT))) != Error::OK)
    return error;

  if ((error = this->sync(mFd)) != Error::OK)
    return error;

  if ((error = this->close(mFd)) != Error::OK)
    return error;

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
