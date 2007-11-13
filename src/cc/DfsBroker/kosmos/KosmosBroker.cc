/**
 * Copyright (C) 2007 Sriram Rao (Kosmix Corp)
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
#include <time.h>
}

#include "Common/FileUtils.h"
#include "Common/System.h"

#include "KosmosBroker.h"

using namespace Hypertable;
using KFS::KfsClient;

KosmosBroker::KosmosBroker(PropertiesPtr &propsPtr) : mVerbose(false) {

  const char *metaName;
  int metaPort;

  mVerbose = propsPtr->getPropertyBool("verbose", false);

  KfsClient *clnt = KfsClient::Instance();
  metaName = propsPtr->getProperty("kfs.metaServer.name", "");
  metaPort = propsPtr->getPropertyInt("kfs.metaServer.port", -1);

  std::cerr << "Server: " << metaName << " " << metaPort << std::endl;
  clnt->Init(metaName, metaPort);
  mRootdir = "";
}



KosmosBroker::~KosmosBroker() {
}


/**
 * Open
 */
void KosmosBroker::Open(ResponseCallbackOpen *cb, const char *fileName, uint32_t bufferSize) {
  int fd;
  std::string absFileName;
  KfsClient *clnt = KfsClient::Instance();

  if (mVerbose) {
    LOG_VA_INFO("open file='%s' bufferSize=%d", fileName, bufferSize);
  }

  if (fileName[0] == '/')
    absFileName = fileName;
  else
    absFileName = mRootdir + "/" + fileName;

  std::cerr << "Open file: " << absFileName << std::endl;  

  //fd = atomic_inc_return(&msUniqueId);

  /**
   * Open the file
   */
  if ((fd = clnt->Open(absFileName.c_str(), O_RDONLY)) < 0) {
    string errMsg = KFS::ErrorCodeToStr(fd);
    LOG_VA_ERROR("open failed: file='%s' - %s", absFileName.c_str(), errMsg.c_str());
    ReportError(cb, fd);
    return;
  }

  {
    int error;
    struct sockaddr_in addr;
    OpenFileDataKosmosPtr dataPtr(new OpenFileDataKosmos(fd, O_RDONLY));

    cb->get_address(addr);

    std::cerr << "Got fd for file: " << absFileName << std::endl;

    mOpenFileMap.Create(fd, addr, dataPtr);

    std::cerr << "Inserted fd into open file map for file: "<< absFileName << std::endl;

    cb->response(fd);
  }
}


/**
 * Create
 */
void KosmosBroker::Create(ResponseCallbackOpen *cb, const char *fileName, bool overwrite,
	    uint32_t bufferSize, uint16_t replication, uint64_t blockSize) {
  int fd;
  int flags;
  std::string absFileName;
  KfsClient *clnt = KfsClient::Instance();
  
  if (mVerbose) {
    LOG_VA_INFO("create file='%s' overwrite=%d bufferSize=%d replication=%d blockSize=%d",
		fileName, (int)overwrite, bufferSize, replication, blockSize);
  }

  if (fileName[0] == '/')
    absFileName = fileName;
  else
    absFileName = mRootdir + "/" + fileName;


  //fd = atomic_inc_return(&msUniqueId);

  if (overwrite)
    flags = O_WRONLY | O_CREAT | O_TRUNC;
  else
    flags = O_WRONLY | O_CREAT | O_APPEND;

  std::cerr << "Create file: " << absFileName << " " << flags << std::endl;  

  /**
   * Open the file
   */
  if ((fd = clnt->Open(absFileName.c_str(), flags)) < 0) {
    string errMsg = KFS::ErrorCodeToStr(fd);
    std::cerr << "Create failed: " << errMsg << std::endl;

    LOG_VA_ERROR("open failed: file='%s' - %s", absFileName.c_str(), errMsg.c_str());
    ReportError(cb, fd);
    return;
  }

  {
    int error;
    struct sockaddr_in addr;
    OpenFileDataKosmosPtr dataPtr(new OpenFileDataKosmos(fd, O_WRONLY));

    cb->get_address(addr);

    mOpenFileMap.Create(fd, addr, dataPtr);

    cb->response(fd);
  }
}


/**
 * Close
 */
void KosmosBroker::Close(ResponseCallback *cb, uint32_t fd) {
  KfsClient *clnt = KfsClient::Instance();
  if (mVerbose) {
    LOG_VA_INFO("close fd=%d", fd);
  }

  mOpenFileMap.Remove(fd);

  cb->response_ok();
}


/**
 * Read
 */
void KosmosBroker::Read(ResponseCallbackRead *cb, uint32_t fd, uint32_t amount) {
  OpenFileDataKosmosPtr dataPtr;
  ssize_t nread;
  uint64_t offset;
  uint8_t *buf;
  KfsClient *clnt = KfsClient::Instance();

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

  offset = clnt->Tell(dataPtr->fd);

  if ((nread = clnt->Read(dataPtr->fd, (char *) buf, (size_t) amount)) < 0) {
    string errMsg = KFS::ErrorCodeToStr(nread);
    LOG_VA_ERROR("read failed: fd=%d amount=%d - %s", dataPtr->fd, amount, errMsg.c_str());
    ReportError(cb, nread);
    return;
  }

  cb->response(offset, nread, buf);

}


/**
 * Append
 */
void KosmosBroker::Append(ResponseCallbackAppend *cb, uint32_t fd, uint32_t amount, uint8_t *data) {
  OpenFileDataKosmosPtr dataPtr;
  ssize_t nwritten;
  uint64_t offset;
  KfsClient *clnt = KfsClient::Instance();

  if (mVerbose) {
    LOG_VA_INFO("append fd=%d amount=%d", fd, amount);
  }

  if (!mOpenFileMap.Get(fd, dataPtr)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  offset = clnt->Tell(dataPtr->fd);

  if ((nwritten = clnt->Write(dataPtr->fd, (const char *) data, (size_t) amount)) < 0) {
    string errMsg = KFS::ErrorCodeToStr(nwritten);
    LOG_VA_ERROR("write failed: fd=%d amount=%d - %s", dataPtr->fd, amount, errMsg.c_str());
    ReportError(cb, nwritten);
    return;
  }

  cb->response(offset, nwritten);
  
  return;
}


/**
 * Seek
 */
void KosmosBroker::Seek(ResponseCallback *cb, uint32_t fd, uint64_t offset) {
  OpenFileDataKosmosPtr dataPtr;
  KfsClient *clnt = KfsClient::Instance();

  if (!mOpenFileMap.Get(fd, dataPtr)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  if ((offset = (uint64_t) clnt->Seek(dataPtr->fd, offset, SEEK_SET)) == (uint64_t) -1) {
    string errMsg = KFS::ErrorCodeToStr(offset);
    LOG_VA_ERROR("lseek failed: fd=%d offset=%lld - %s", dataPtr->fd, offset, errMsg.c_str());
    ReportError(cb, (int) offset);
    return;
  }

  cb->response_ok();
}


/**
 * Remove
 */
void KosmosBroker::Remove(ResponseCallback *cb, const char *fileName) {
  std::string absFileName;
  KfsClient *clnt = KfsClient::Instance();
  int res;
  
  if (mVerbose) {
    LOG_VA_INFO("remove file='%s'", fileName);
  }

  if (fileName[0] == '/')
    absFileName = fileName;
  else
    absFileName = mRootdir + "/" + fileName;

  if ((res = clnt->Remove(absFileName.c_str())) < 0) {
    string errMsg = KFS::ErrorCodeToStr(res);
    LOG_VA_ERROR("unlink failed: file='%s' - %s", absFileName.c_str(), errMsg.c_str());
    ReportError(cb, res);
    return;
  }

  cb->response_ok();
}


/**
 * Length
 */
void KosmosBroker::Length(ResponseCallbackLength *cb, const char *fileName) {
  std::string absFileName;
  uint64_t length;
  int res;
  KfsClient *clnt = KfsClient::Instance();
  struct stat statInfo;
  
  if (mVerbose) {
    LOG_VA_INFO("length file='%s'", fileName);
  }

  if (fileName[0] == '/')
    absFileName = fileName;
  else
    absFileName = mRootdir + "/" + fileName;

  if ((res = clnt->Stat(absFileName.c_str(), statInfo)) < 0) {
    string errMsg = KFS::ErrorCodeToStr(res);
    LOG_VA_ERROR("length (stat) failed: file='%s' - %s", absFileName.c_str(), errMsg.c_str());
    ReportError(cb, res);
    return;
  }
  length = statInfo.st_size;
  cb->response(length);
}


/**
 * Pread
 */
void KosmosBroker::Pread(ResponseCallbackRead *cb, uint32_t fd, uint64_t offset, uint32_t amount) {
  OpenFileDataKosmosPtr dataPtr;
  ssize_t nread;
  uint8_t *buf;
  KfsClient *clnt = KfsClient::Instance();

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

  if ((offset = (uint64_t) clnt->Seek(dataPtr->fd, offset, SEEK_SET)) == (uint64_t) -1) {
    string errMsg = KFS::ErrorCodeToStr(offset);
    LOG_VA_ERROR("lseek failed: fd=%d offset=%lld - %s", dataPtr->fd, offset, errMsg.c_str());
    ReportError(cb, (int) offset);
    return;
  }

  if ((nread = clnt->Read(dataPtr->fd, (char *) buf, (size_t) amount)) < 0) {
    string errMsg = KFS::ErrorCodeToStr(nread);
    LOG_VA_ERROR("read failed: fd=%d amount=%d - %s", dataPtr->fd, amount, errMsg.c_str());
    ReportError(cb, nread);
    return;
  }
  
  cb->response(offset, nread, buf);
}


/**
 * Mkdirs
 */
void KosmosBroker::Mkdirs(ResponseCallback *cb, const char *dirName) {
  std::string absDirName;
  std::string::size_type curr = 0;
  KfsClient *clnt = KfsClient::Instance();
  int res = 0;
  struct stat statBuf;
  
  if (mVerbose) {
    LOG_VA_INFO("mkdirs dir='%s'", dirName);
  }

  if (dirName[0] == '/')
    absDirName = dirName;
  else
    absDirName = mRootdir + "/" + dirName;

  std::string path;
  while (curr != std::string::npos) {
    curr = absDirName.find('/', curr + 1);
    if (curr != std::string::npos) 
      path.assign(absDirName, 0, curr);
    else
      path = absDirName;

    std::cerr << "Stat'ing: " << path << std::endl;

    if (clnt->Stat(path.c_str(), statBuf) == 0) {
      if (!S_ISDIR(statBuf.st_mode)) {
	res = -ENOTDIR;
	break;
      }
      continue;
    }
    std::cerr << "Mkdir'ing: " << path << std::endl;

    res = clnt->Mkdir(path.c_str());
    if (res < 0)
      break;
  }

  if (res < 0) {
    string errMsg = KFS::ErrorCodeToStr(res);
    LOG_VA_ERROR("mkdirs failed: dirName='%s' - %s", absDirName.c_str(), errMsg.c_str());
    ReportError(cb, res);
    return;
  }

  cb->response_ok();
}


/**
 * Rmdir
 */
void KosmosBroker::Rmdir(ResponseCallback *cb, const char *dirName) {
  std::string absDirName;
  KfsClient *clnt = KfsClient::Instance();
  int res;
  
  if (mVerbose) {
    LOG_VA_INFO("rmdir dir='%s'", dirName);
  }

  if (dirName[0] == '/')
    absDirName = dirName;
  else
    absDirName = mRootdir + "/" + dirName;

  if ((res = clnt->Rmdir(absDirName.c_str())) != 0) {
    string errMsg = KFS::ErrorCodeToStr(res);
    LOG_VA_ERROR("rmdir failed: dirName='%s' - %s", absDirName.c_str(), errMsg.c_str());
    ReportError(cb, res);
    return;
  }

  cb->response_ok();
}


/**
 * Flush
 */
void KosmosBroker::Flush(ResponseCallback *cb, uint32_t fd) {
  OpenFileDataKosmosPtr dataPtr;
  KfsClient *clnt = KfsClient::Instance();
  int res;

  if (mVerbose) {
    LOG_VA_INFO("flush fd=%d", fd);
  }

  if (!mOpenFileMap.Get(fd, dataPtr)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  if ((res = clnt->Sync(dataPtr->fd)) < 0) {
    string errMsg = KFS::ErrorCodeToStr(res);
    LOG_VA_ERROR("flush failed: fd=%d - %s", dataPtr->fd, errMsg.c_str());
    ReportError(cb, res);
    return;
  }

  cb->response_ok();
}


/**
 * Status
 */
void KosmosBroker::Status(ResponseCallback *cb) {
  cb->response_ok();
}


/**
 * Shutdown
 */
void KosmosBroker::Shutdown(ResponseCallback *cb) {
  mOpenFileMap.RemoveAll();
  cb->response_ok();
  poll(0, 0, 2000);
}


/**
 * ReportError
 */
void KosmosBroker::ReportError(ResponseCallback *cb, int error) {

  string errbuf = KFS::ErrorCodeToStr(error);

  if (error == ENOTDIR || error == ENAMETOOLONG || error == ENOENT)
    cb->error(Error::DFSBROKER_BAD_FILENAME, errbuf.c_str());
  else if (error == EACCES || error == EPERM)
    cb->error(Error::DFSBROKER_PERMISSION_DENIED, errbuf.c_str());
  else if (error == EBADF)
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf.c_str());
  else if (error == EINVAL)
    cb->error(Error::DFSBROKER_INVALID_ARGUMENT, errbuf.c_str());
  else
    cb->error(Error::DFSBROKER_IO_ERROR, errbuf.c_str());
}
