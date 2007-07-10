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

