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

#include "Common/Logger.h"

#include "CommitLog.h"
#include "CommitLogReader.h"

using namespace hypertable;
using namespace std;

/**
 *
 */
CommitLogReader::CommitLogReader(Filesystem *fs, std::string &logDir) : mFs(fs), mLogDir(logDir), mBlockBuffer(sizeof(CommitLogHeaderT)) {
  LogFileInfoT fileInfo;
  struct stat statbuf;
  int fd;
  CommitLogHeaderT  header;
  vector<string> listing;

  LOG_VA_INFO("LogDir = %s", logDir.c_str());

  if (mLogDir.find('/', mLogDir.length()-1) == string::npos)
    mLogDir += "/";

  if ((error = mFs->Readdir(mLogDir.c_str(), listing)) != Error::OK) {
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

    if (stat(mLogFileInfo[i].fname.c_str(), &statbuf) != 0) {
      LOG_VA_ERROR("Problem trying to stat commit log file '%s' - %s", mLogFileInfo[i].fname.c_str(), strerror(errno));
      exit(1);
    }

    if (statbuf.st_size < (off_t)sizeof(CommitLogHeaderT))
      continue;

    if ((fd = open(mLogFileInfo[i].fname.c_str(), O_RDONLY)) == -1) {
      LOG_VA_ERROR("Problem trying to open commit log file '%s' - %s", mLogFileInfo[i].fname.c_str(), strerror(errno));
      exit(1);
    }

    if (pread(fd, &header, sizeof(CommitLogHeaderT), statbuf.st_size-sizeof(CommitLogHeaderT)) != sizeof(CommitLogHeaderT)) {
      LOG_VA_ERROR("Problem reading trailing header in commit log file '%s' - %s", mLogFileInfo[i].fname.c_str(), strerror(errno));
      exit(1);
    }

    close(fd);

    if (!strncmp(header.marker, "-BLOCK--", 8))
      mLogFileInfo[i].timestamp = header.timestamp;

    //cout << mLogFileInfo[i].num << ":  " << mLogFileInfo[i].fname << " " << mLogFileInfo[i].timestamp << endl;
  }

}



void CommitLogReader::InitializeRead(uint64_t timestamp) {
  mCutoffTime = timestamp;
  mCurLogOffset = 0;
  mFp = 0;
}



bool CommitLogReader::NextBlock(CommitLogHeaderT **blockp) {
  bool done = false;
  size_t toread;

  while (!done) {

    if (mFp == 0) {

      while (mCurLogOffset < mLogFileInfo.size()) {
	if (mLogFileInfo[mCurLogOffset].timestamp == 0 ||
	    mLogFileInfo[mCurLogOffset].timestamp > mCutoffTime)
	  break;
	mCurLogOffset++;
      }

      if (mCurLogOffset == mLogFileInfo.size())
	return false;

      if ((mFp = fopen(mLogFileInfo[mCurLogOffset].fname.c_str(), "r")) == 0) {
	LOG_VA_ERROR("Problem trying to open commit log file '%s' - %s", mLogFileInfo[mCurLogOffset].fname.c_str(), strerror(errno));
	return false;
      }
    }

    mBlockBuffer.ptr = mBlockBuffer.buf;
    
    if (fread(mBlockBuffer.ptr, 1, sizeof(CommitLogHeaderT), mFp) != sizeof(CommitLogHeaderT)) {
      if (feof(mFp)) {
	fclose(mFp);
	mFp = 0;
	mCurLogOffset++;
      }
      else {
	int error = ferror(mFp);
	LOG_VA_ERROR("Problem trying to read commit log file '%s' - %s", mLogFileInfo[mCurLogOffset].fname.c_str(), strerror(error));
	fclose(mFp);
	mFp = 0;
	return false;
      }
    }
    else {
      CommitLogHeaderT *header = (CommitLogHeaderT *)mBlockBuffer.buf;
      if (header->length == sizeof(CommitLogHeaderT)) {
	if (feof(mFp)) {
	  fclose(mFp);
	  mFp = 0;
	  mCurLogOffset++;
	}
	continue;
      }
      mBlockBuffer.ptr += sizeof(CommitLogHeaderT);
      // TODO: sanity check this header
      toread = header->length-sizeof(CommitLogHeaderT);
      mBlockBuffer.ensure(toread);
      if (fread(mBlockBuffer.ptr, 1, toread, mFp) != toread) {
	LOG_VA_ERROR("Short read of commit log block '%s'", mLogFileInfo[mCurLogOffset].fname.c_str());
	fclose(mFp);
	mFp = 0;
	return false;
      }
      *blockp = (CommitLogHeaderT *)mBlockBuffer.buf;
      return true;
    }
  }

  return false;
  
}
