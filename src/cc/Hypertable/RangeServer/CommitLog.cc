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

#include <boost/shared_array.hpp>

#include "Common/Error.h"
#include "Common/StringExt.h"

#include "Global.h"
#include "CommitLog.h"

using namespace hypertable;


/**
 *
 */
CommitLog::CommitLog(Filesystem *fs, std::string &logDir, int64_t logFileSize) : mFs(fs), mLogDir(logDir), mLogFile(), mMaxFileSize(logFileSize), mCurLogLength(0), mCurLogNum(0), mMutex(), mFileInfoQueue() {
  int error;

  if (mLogDir.find('/', mLogDir.length()-1) == string::npos)
    mLogDir += "/";

  mLogFile = mLogDir + mCurLogNum;

  if ((error = mFs->Create(mLogFile, true, 8192, 3, 67108864, &mFd)) != Error::OK) {
    LOG_VA_ERROR("Problem creating commit log file '%s' - %s", mLogFile.c_str(), Error::GetText(error));
    exit(1);
  }

}



/**
 * 
 */
int CommitLog::Write(const char *tableName, uint8_t *data, uint32_t len, uint64_t timestamp) {
  uint32_t totalLen = sizeof(CommitLogHeaderT) + strlen(tableName) + 1 + len;
  uint8_t *block = new uint8_t [ totalLen ];
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

  if ((error = mFs->Append(mFd, block, totalLen)) != Error::OK)
    return error;

  if ((error = mFs->Flush(mFd)) != Error::OK)
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
    header = new CommitLogHeaderT[1];
    header->checksum = 0;
    header->length = sizeof(CommitLogHeaderT);
    endptr = (uint8_t *)&header[1];
    for (ptr=block; ptr<endptr; ptr++)
      checksum += *ptr;
    header->checksum = checksum;

    if ((error = mFs->Append(mFd, header, sizeof(CommitLogHeaderT))) != Error::OK) {
      LOG_VA_ERROR("Problem appending %d bytes to commit log file '%s' - %s", sizeof(CommitLogHeaderT), mLogFile.c_str(), Error::GetText(error));
      return error;
    }

    if ((error = mFs->Flush(mFd)) != Error::OK) {
      LOG_VA_ERROR("Problem flushing commit log file '%s' - %s", mLogFile.c_str(), Error::GetText(error));
      return error;
    }

    if ((error = mFs->Close(mFd)) != Error::OK) {
      LOG_VA_ERROR("Problem closing commit log file '%s' - %s", mLogFile.c_str(), Error::GetText(error));
      return error;
    }

    fileInfo.timestamp = timestamp;
    fileInfo.fname = mLogFile;
    mFileInfoQueue.push(fileInfo);

    mCurLogNum++;
    sprintf(buf, "%d", mCurLogNum);
    mLogFile = mLogDir + buf;

    mCurLogLength = 0;

    if ((error = mFs->Create(mLogFile, true, 8192, 3, 67108864, &mFd)) != Error::OK) {
      LOG_VA_ERROR("Problem creating commit log file '%s' - %s", mLogFile.c_str(), Error::GetText(error));
      return error;
    }

  }

  return Error::OK;
}



/**
 * 
 */
int CommitLog::Close(uint64_t timestamp) {
  CommitLogHeaderT *header = new CommitLogHeaderT[1];
  int error = Error::OK;
  memcpy(header->marker, "-BLOCK--", 8);
  header->timestamp = timestamp;
  header->checksum = 0;
  header->length = sizeof(CommitLogHeaderT);

  if ((error = mFs->Append(mFd, header, sizeof(CommitLogHeaderT))) != Error::OK) {
    LOG_VA_ERROR("Problem appending %d bytes to commit log file '%s' - %s", sizeof(CommitLogHeaderT), mLogFile.c_str(), Error::GetText(error));
      return error;
  }

  if ((error = mFs->Flush(mFd)) != Error::OK) {
    LOG_VA_ERROR("Problem flushing commit log file '%s' - %s", mLogFile.c_str(), Error::GetText(error));
    return error;
  }

  if ((error = mFs->Close(mFd)) != Error::OK) {
    LOG_VA_ERROR("Problem closing commit log file '%s' - %s", mLogFile.c_str(), Error::GetText(error));
    return error;
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
      if ((error = mFs->Remove(fileInfo.fname)) == Error::OK)
	mFileInfoQueue.pop();
    }
    else
      break;
  }
  return Error::OK;
}
