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

#include <cerrno>
#include <cstring>

extern "C" {
#include <fcntl.h>
#include <poll.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
}

#include "Common/FileUtils.h"
#include "Common/System.h"

#include "LocalBroker.h"

using namespace hypertable;


LocalBroker::LocalBroker(PropertiesPtr &propsPtr) : mVerbose(false) {
  const char *root;

  mVerbose = propsPtr->getPropertyBool("verbose", false);

  /**
   * Determine root directory
   */
  if ((root = propsPtr->getProperty("DfsBroker.local.root", 0)) == 0) {
    LOG_ERROR("Required property 'DfsBroker.local.root' not specified, exiting...");
    exit(1);
  }

  mRootdir = (root[0] == '/') ? root : System::installDir + "/" + root;

  // strip off the trailing '/'
  if (root[strlen(root)-1] == '/')
    mRootdir = mRootdir.substr(0, mRootdir.length()-1);

  // ensure that root directory exists
  if (!FileUtils::Mkdirs(mRootdir.c_str()))
    exit(1);
}



LocalBroker::~LocalBroker() {
}


/**
 * Open
 */
void LocalBroker::Open(ResponseCallbackOpen *cb, const char *fileName, uint32_t bufferSize) {
  int fd;
  std::string absFileName;
  
  if (mVerbose) {
    LOG_VA_INFO("open file='%s' bufferSize=%d", fileName, bufferSize);
  }

  if (fileName[0] == '/')
    absFileName = mRootdir + fileName;
  else
    absFileName = mRootdir + "/" + fileName;

  //fd = atomic_inc_return(&msUniqueId);

  /**
   * Open the file
   */
  if ((fd = open(absFileName.c_str(), O_RDONLY)) == -1) {
    LOG_VA_ERROR("open failed: file='%s' - %s", absFileName.c_str(), strerror(errno));
    ReportError(cb);
    return;
  }

  {
    int error;
    struct sockaddr_in addr;
    OpenFileDataLocalPtr dataPtr(new OpenFileDataLocal(fd, O_RDONLY));

    cb->get_address(addr);

    mOpenFileMap.Create(fd, addr, dataPtr);

    cb->response(fd);
  }
}


/**
 * Create
 */
void LocalBroker::Create(ResponseCallbackOpen *cb, const char *fileName, bool overwrite,
	    uint32_t bufferSize, uint16_t replication, uint64_t blockSize) {
  int fd;
  int flags;
  std::string absFileName;
  
  if (mVerbose) {
    LOG_VA_INFO("create file='%s' overwrite=%d bufferSize=%d replication=%d blockSize=%d",
		fileName, (int)overwrite, bufferSize, replication, blockSize);
  }

  if (fileName[0] == '/')
    absFileName = mRootdir + fileName;
  else
    absFileName = mRootdir + "/" + fileName;

  //fd = atomic_inc_return(&msUniqueId);

  if (overwrite)
    flags = O_WRONLY | O_CREAT | O_TRUNC;
  else
    flags = O_WRONLY | O_CREAT | O_APPEND;

  /**
   * Open the file
   */
  if ((fd = open(absFileName.c_str(), flags, 0644)) == -1) {
    LOG_VA_ERROR("open failed: file='%s' - %s", absFileName.c_str(), strerror(errno));
    ReportError(cb);
    return;
  }

  {
    int error;
    struct sockaddr_in addr;
    OpenFileDataLocalPtr dataPtr(new OpenFileDataLocal(fd, O_WRONLY));

    cb->get_address(addr);

    mOpenFileMap.Create(fd, addr, dataPtr);

    cb->response(fd);
  }
}


/**
 * Close
 */
void LocalBroker::Close(ResponseCallback *cb, uint32_t fd) {
  if (mVerbose) {
    LOG_VA_INFO("close fd=%d", fd);
  }
  mOpenFileMap.Remove(fd);
  cb->response_ok();
}


/**
 * Read
 */
void LocalBroker::Read(ResponseCallbackRead *cb, uint32_t fd, uint32_t amount) {
  OpenFileDataLocalPtr dataPtr;
  ssize_t nread;
  uint64_t offset;
  uint8_t *buf;

  if (mVerbose) {
    LOG_VA_INFO("read fd=%d amount=%d", fd, amount);
  }

  buf = new uint8_t [ amount ]; // TODO: we should sanity check this amount

  if (!mOpenFileMap.Get(fd, dataPtr)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  if ((offset = (uint64_t)lseek(dataPtr->fd, 0, SEEK_CUR)) == -1) {
    LOG_VA_ERROR("lseek failed: fd=%d offset=0 SEEK_CUR - %s", dataPtr->fd, strerror(errno));
    ReportError(cb);
    return;
  }

  if ((nread = FileUtils::Read(dataPtr->fd, buf, amount)) == -1) {
    LOG_VA_ERROR("read failed: fd=%d amount=%d - %s", dataPtr->fd, amount, strerror(errno));
    ReportError(cb);
    return;
  }

  cb->response(offset, nread, buf);

}


/**
 * Append
 */
void LocalBroker::Append(ResponseCallbackAppend *cb, uint32_t fd, uint32_t amount, uint8_t *data) {
  OpenFileDataLocalPtr dataPtr;
  ssize_t nwritten;
  uint64_t offset;

  if (mVerbose) {
    LOG_VA_INFO("append fd=%d amount=%d", fd, amount);
  }

  if (!mOpenFileMap.Get(fd, dataPtr)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  if ((offset = (uint64_t)lseek(dataPtr->fd, 0, SEEK_CUR)) == -1) {
    LOG_VA_ERROR("lseek failed: fd=%d offset=0 SEEK_CUR - %s", dataPtr->fd, strerror(errno));
    ReportError(cb);
    return;
  }

  if ((nwritten = FileUtils::Write(dataPtr->fd, data, amount)) == -1) {
    LOG_VA_ERROR("write failed: fd=%d amount=%d - %s", dataPtr->fd, amount, strerror(errno));
    ReportError(cb);
    return;
  }

  cb->response(offset, nwritten);
  
  return;
}


/**
 * Seek
 */
void LocalBroker::Seek(ResponseCallback *cb, uint32_t fd, uint64_t offset) {
  OpenFileDataLocalPtr dataPtr;

  if (!mOpenFileMap.Get(fd, dataPtr)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  if ((offset = (uint64_t)lseek(dataPtr->fd, offset, SEEK_SET)) == -1) {
    LOG_VA_ERROR("lseek failed: fd=%d offset=%lld - %s", dataPtr->fd, offset, strerror(errno));
    ReportError(cb);
    return;
  }

  cb->response_ok();
}


/**
 * Remove
 */
void LocalBroker::Remove(ResponseCallback *cb, const char *fileName) {
  std::string absFileName;
  
  if (mVerbose) {
    LOG_VA_INFO("remove file='%s'", fileName);
  }

  if (fileName[0] == '/')
    absFileName = mRootdir + fileName;
  else
    absFileName = mRootdir + "/" + fileName;

  if (unlink(absFileName.c_str()) == -1) {
    LOG_VA_ERROR("unlink failed: file='%s' - %s", absFileName.c_str(), strerror(errno));
    ReportError(cb);
    return;
  }

  cb->response_ok();
}


/**
 * Length
 */
void LocalBroker::Length(ResponseCallbackLength *cb, const char *fileName) {
  std::string absFileName;
  uint64_t length;
  
  if (mVerbose) {
    LOG_VA_INFO("length file='%s'", fileName);
  }

  if (fileName[0] == '/')
    absFileName = mRootdir + fileName;
  else
    absFileName = mRootdir + "/" + fileName;

  if ((length = FileUtils::Length(absFileName.c_str())) == (off_t)-1) {
    LOG_VA_ERROR("length (stat) failed: file='%s' - %s", absFileName.c_str(), strerror(errno));
    ReportError(cb);
    return;
  }

  cb->response(length);
}


/**
 * Pread
 */
void LocalBroker::Pread(ResponseCallbackRead *cb, uint32_t fd, uint64_t offset, uint32_t amount) {
  OpenFileDataLocalPtr dataPtr;
  ssize_t nread;
  uint8_t *buf;

  if (mVerbose) {
    LOG_VA_INFO("pread fd=%d offset=%lld amount=%d", fd, offset, amount);
  }

  buf = new uint8_t [ amount ]; // TODO: we should sanity check this amount

  if (!mOpenFileMap.Get(fd, dataPtr)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  if ((nread = FileUtils::Pread(dataPtr->fd, buf, amount, (off_t)offset)) == -1) {
    LOG_VA_ERROR("pread failed: fd=%d amount=%d offset=%lld - %s", dataPtr->fd, amount, offset, strerror(errno));
    ReportError(cb);
    return;
  }

  cb->response(offset, nread, buf);
}


/**
 * Mkdirs
 */
void LocalBroker::Mkdirs(ResponseCallback *cb, const char *dirName) {
  std::string absDirName;
  uint64_t length;
  
  if (mVerbose) {
    LOG_VA_INFO("length file='%s'", dirName);
  }

  if (dirName[0] == '/')
    absDirName = mRootdir + dirName;
  else
    absDirName = mRootdir + "/" + dirName;

  if (!FileUtils::Mkdirs(absDirName.c_str())) {
    LOG_VA_ERROR("mkdirs failed: dirName='%s' - %s", absDirName.c_str(), strerror(errno));
    ReportError(cb);
    return;
  }

  cb->response_ok();
}


/**
 * Flush
 */
void LocalBroker::Flush(ResponseCallback *cb, uint32_t fd) {
  OpenFileDataLocalPtr dataPtr;

  if (mVerbose) {
    LOG_VA_INFO("flush fd=%d", fd);
  }

  if (!mOpenFileMap.Get(fd, dataPtr)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  if (fsync(dataPtr->fd) != 0) {
    LOG_VA_ERROR("flush failed: fd=%d - %s", dataPtr->fd, strerror(errno));
    ReportError(cb);
    return;
  }

  cb->response_ok();
}


/**
 * Status
 */
void LocalBroker::Status(ResponseCallback *cb) {
  cb->response_ok();
}


/**
 * Shutdown
 */
void LocalBroker::Shutdown(ResponseCallback *cb) {
  mOpenFileMap.RemoveAll();
  cb->response_ok();
  poll(0, 0, 2000);
}


/**
 * ReportError
 */
void LocalBroker::ReportError(ResponseCallback *cb) {
  char errbuf[128];
  strerror_r(errno, errbuf, 128);
  if (errno == ENOTDIR || errno == ENAMETOOLONG || errno == ENOENT)
    cb->error(Error::DFSBROKER_BAD_FILENAME, errbuf);
  else if (errno == EACCES || errno == EPERM)
    cb->error(Error::DFSBROKER_PERMISSION_DENIED, errbuf);
  else if (errno == EBADF)
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
  else if (errno == EINVAL)
    cb->error(Error::DFSBROKER_INVALID_ARGUMENT, errbuf);
  else
    cb->error(Error::DFSBROKER_IO_ERROR, errbuf);
}
