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

#include <boost/shared_array.hpp>

extern "C" {
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
}

#include "Common/Error.h"
#include "Common/Logger.h"

#include "Global.h"
#include "CommitLogLocal.h"

using namespace hypertable;

/**
 *
 */
CommitLogLocal::CommitLogLocal(std::string &logDirRoot, std::string &logDir, int64_t logFileSize) : CommitLog(logDirRoot, logDir, logFileSize) {
  int error;
  if ((error = this->create(mLogFile, &mFd)) != Error::OK) {
    exit(1);
  }
}


int CommitLogLocal::create(std::string &name, int32_t *fdp) {
  if ((*fdp = ::open(name.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644)) == -1) {
    LOG_VA_ERROR("Problem creating Log file '%s'", name.c_str());
    return Error::LOCAL_IO_ERROR;
  }
  return Error::OK;
}


int CommitLogLocal::write(int32_t fd, const void *buf, uint32_t amount) {
  ssize_t nwritten;
  if ((nwritten = ::write(fd, buf, (size_t)amount)) != (ssize_t)amount) {
    LOG_VA_ERROR("Problem writing to commit log '%s', tried to write %d, write() returned %d - %s",
		 mLogFile.c_str(), amount, nwritten, strerror(errno));
    return Error::LOCAL_IO_ERROR;
  }
  return Error::OK;
}


int CommitLogLocal::close(int32_t fd) {
  if (::close(fd) == -1) {
    LOG_VA_ERROR("Problem closing commit log '%s' - %s", mLogFile.c_str(), strerror(errno));
    return Error::LOCAL_IO_ERROR;
  }
  return Error::OK;  
}


int CommitLogLocal::sync(int32_t fd) {
  if (::fsync(fd) != 0) {
    LOG_VA_ERROR("Problem syncing commit log '%s' - %s", mLogFile.c_str(), strerror(errno));
    return Error::LOCAL_IO_ERROR;
  }
  return Error::OK;
}


int CommitLogLocal::unlink(std::string &name) {
  if (::unlink(name.c_str()) != 0) {
    LOG_VA_ERROR("Problem deleting Log file '%s'", name.c_str());
    return Error::LOCAL_IO_ERROR;
  }
  return Error::OK;
}

