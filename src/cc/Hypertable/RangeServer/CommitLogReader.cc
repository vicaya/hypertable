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

#include <string>
#include <vector>

extern "C" {
#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
}

#include "Common/Error.h"
#include "Common/Logger.h"

#include "CommitLog.h"
#include "CommitLogReader.h"

using namespace hypertable;
using namespace std;

namespace {
  const uint32_t READAHEAD_BUFFER_SIZE = 131072;
}

/**
 *
 */
CommitLogReader::CommitLogReader(Filesystem *fs, std::string &logDir) : mFs(fs), mLogDir(logDir), mFd(-1), mBlockBuffer(sizeof(CommitLogHeaderT)) {
  LogFileInfoT fileInfo;
  int error;
  int32_t fd;
  CommitLogHeaderT  header;
  vector<string> listing;
  int64_t flen;
  uint32_t nread;

  LOG_VA_INFO("LogDir = %s", logDir.c_str());

  if (mLogDir.find('/', mLogDir.length()-1) == string::npos)
    mLogDir += "/";

  if ((error = mFs->Readdir(mLogDir, listing)) != Error::OK) {
    LOG_VA_ERROR("Problem reading directory '%s' - %s", mLogDir.c_str(), Error::GetText(error));
    exit(1);
  }

  mLogFileInfo.clear();

  for (size_t i=0; i<listing.size(); i++) {
    char *endptr;
    long num = strtol(listing[i].c_str(), &endptr, 10);
    if (*endptr != 0) {
      LOG_VA_WARN("Invalid file '%s' found in commit log directory '%s'", listing[i].c_str(), mLogDir.c_str());
    }
    else {
      fileInfo.num = (uint32_t)num;
      fileInfo.fname = mLogDir + listing[i];
      mLogFileInfo.push_back(fileInfo);
    }
  }

  sort(mLogFileInfo.begin(), mLogFileInfo.end());

  for (size_t i=0; i<mLogFileInfo.size(); i++) {

    mLogFileInfo[i].timestamp = 0;

    if ((error = mFs->Length(mLogFileInfo[i].fname, &flen)) != Error::OK) {
      LOG_VA_ERROR("Problem trying to determine length of commit log file '%s' - %s", mLogFileInfo[i].fname.c_str(), strerror(errno));
      exit(1);
    }

    if (flen < sizeof(CommitLogHeaderT))
      continue;

    if ((error = mFs->Open(mLogFileInfo[i].fname, &fd)) != Error::OK) {
      LOG_VA_ERROR("Problem opening commit log file '%s' - %s", mLogFileInfo[i].fname.c_str(), strerror(errno));
      exit(1);
    }

    if ((error = mFs->Pread(fd, flen-sizeof(CommitLogHeaderT), sizeof(CommitLogHeaderT), (uint8_t *)&header, &nread)) != Error::OK ||
	nread != sizeof(CommitLogHeaderT)) {
      LOG_VA_ERROR("Problem reading trailing header in commit log file '%s' - %s", mLogFileInfo[i].fname.c_str(), strerror(errno));
      exit(1);
    }

    if ((error = mFs->Close(fd)) != Error::OK) {
      LOG_VA_ERROR("Problem closing commit log file '%s' - %s", mLogFileInfo[i].fname.c_str(), strerror(errno));
      exit(1);
    }

    if (!strncmp(header.marker, "-BLOCK--", 8))
      mLogFileInfo[i].timestamp = header.timestamp;

    //cout << mLogFileInfo[i].num << ":  " << mLogFileInfo[i].fname << " " << mLogFileInfo[i].timestamp << endl;
  }

}



void CommitLogReader::InitializeRead(uint64_t timestamp) {
  mCutoffTime = timestamp;
  mCurLogOffset = 0;
  mFd = -1;
}



bool CommitLogReader::NextBlock(CommitLogHeaderT **blockp) {
  bool done = false;
  size_t toread;
  uint32_t nread;
  int error;

  while (!done) {

    if (mFd == -1) {

      while (mCurLogOffset < mLogFileInfo.size()) {
	if (mLogFileInfo[mCurLogOffset].timestamp == 0 ||
	    mLogFileInfo[mCurLogOffset].timestamp > mCutoffTime)
	  break;
	mCurLogOffset++;
      }

      if (mCurLogOffset == mLogFileInfo.size())
	return false;

      if ((error = mFs->OpenBuffered(mLogFileInfo[mCurLogOffset].fname, READAHEAD_BUFFER_SIZE, &mFd)) != Error::OK) {
	LOG_VA_ERROR("Problem trying to open commit log file '%s' - %s", mLogFileInfo[mCurLogOffset].fname.c_str(), Error::GetText(error));
	return false;
      }
    }

    mBlockBuffer.ptr = mBlockBuffer.buf;

    if ((error = mFs->Read(mFd, sizeof(CommitLogHeaderT), mBlockBuffer.ptr, &nread)) != Error::OK) {
      LOG_VA_ERROR("Problem reading header from commit log file '%s' - %s", mLogFileInfo[mCurLogOffset].fname.c_str(), Error::GetText(error));
      return false;
    }
    
    if (nread != sizeof(CommitLogHeaderT)) {
      if ((error = mFs->Close(mFd)) != Error::OK) {
	LOG_VA_ERROR("Problem closing commit log file '%s' - %s", mLogFileInfo[mCurLogOffset].fname.c_str(), Error::GetText(error));
	return false;
      }
      mFd = -1;
      mCurLogOffset++;
    }
    else {
      CommitLogHeaderT *header = (CommitLogHeaderT *)mBlockBuffer.buf;
      if (header->length == sizeof(CommitLogHeaderT))
	continue;
      mBlockBuffer.ptr += sizeof(CommitLogHeaderT);
      // TODO: sanity check this header
      toread = header->length-sizeof(CommitLogHeaderT);
      mBlockBuffer.ensure(toread+sizeof(CommitLogHeaderT));

      if ((error = mFs->Read(mFd, toread, mBlockBuffer.ptr, &nread)) != Error::OK) {
	LOG_VA_ERROR("Problem reading commit block from commit log file '%s' - %s", mLogFileInfo[mCurLogOffset].fname.c_str(), Error::GetText(error));
	return false;
      }
      
      if (nread != toread) {
	LOG_VA_ERROR("Short read of commit log block '%s'", mLogFileInfo[mCurLogOffset].fname.c_str());
	if ((error = mFs->Close(mFd)) != Error::OK) {
	  LOG_VA_ERROR("Problem closing commit log file '%s' - %s", mLogFileInfo[mCurLogOffset].fname.c_str(), Error::GetText(error));
	  return false;
	}
	mFd = -1;
	return false;
      }
      *blockp = (CommitLogHeaderT *)mBlockBuffer.buf;
      return true;
    }
  }

  return false;
  
}
