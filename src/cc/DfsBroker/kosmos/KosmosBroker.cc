/**
 * Copyright (C) 2008 Sriram Rao (Kosmix Corp)
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

  mVerbose = propsPtr->get_bool("Hypertable.Verbose", false);

  KfsClient *clnt = KfsClient::Instance();
  metaName = propsPtr->get("Kfs.MetaServer.Name", "");
  metaPort = propsPtr->get_int("Kfs.MetaServer.Port", -1);

  std::cerr << "Server: " << metaName << " " << metaPort << std::endl;
  clnt->Init(metaName, metaPort);
  mRootdir = "";
}



KosmosBroker::~KosmosBroker() {
}


/**
 * open
 */
void KosmosBroker::open(ResponseCallbackOpen *cb, const char *fileName, uint32_t bufferSize) {
  int fd;
  std::string absFileName;
  KfsClient *clnt = KfsClient::Instance();

  if (mVerbose) {
    HT_INFOF("open file='%s' bufferSize=%d", fileName, bufferSize);
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
    HT_ERRORF("open failed: file='%s' - %s", absFileName.c_str(), errMsg.c_str());
    ReportError(cb, fd);
    return;
  }

  {
    struct sockaddr_in addr;
    OpenFileDataKosmosPtr dataPtr(new OpenFileDataKosmos(fd, O_RDONLY));

    cb->get_address(addr);

    std::cerr << "Got fd for file: " << absFileName << std::endl;

    m_open_file_map.create(fd, addr, dataPtr);
    
    std::cerr << "Inserted fd into open file map for file: "<< absFileName << std::endl;

    cb->response(fd);
  }
}


/**
 * create
 */
void KosmosBroker::create(ResponseCallbackOpen *cb, const char *fileName, bool overwrite,
	    uint32_t bufferSize, uint16_t replication, uint64_t blockSize) {
  int fd;
  int flags;
  std::string absFileName;
  KfsClient *clnt = KfsClient::Instance();
  
  if (mVerbose) {
    HT_INFOF("create file='%s' overwrite=%d bufferSize=%d replication=%d blockSize=%d",
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

    HT_ERRORF("open failed: file='%s' - %s", absFileName.c_str(), errMsg.c_str());
    ReportError(cb, fd);
    return;
  }

  {
    struct sockaddr_in addr;
    OpenFileDataKosmosPtr dataPtr(new OpenFileDataKosmos(fd, O_WRONLY));

    cb->get_address(addr);

    m_open_file_map.create(fd, addr, dataPtr);

    cb->response(fd);
  }
}


/**
 * close
 */
void KosmosBroker::close(ResponseCallback *cb, uint32_t fd) {
  if (mVerbose) {
    HT_INFOF("close fd=%d", fd);
  }

  m_open_file_map.remove(fd);

  cb->response_ok();
}


/**
 * read
 */
void KosmosBroker::read(ResponseCallbackRead *cb, uint32_t fd, uint32_t amount) {
  OpenFileDataKosmosPtr dataPtr;
  ssize_t nread;
  uint64_t offset;
  uint8_t *buf;
  KfsClient *clnt = KfsClient::Instance();

  if (mVerbose) {
    HT_INFOF("read fd=%d amount=%d", fd, amount);
  }

  buf = new uint8_t [ amount ]; // TODO: we should sanity check this amount

  if (!m_open_file_map.get(fd, dataPtr)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  offset = clnt->Tell(dataPtr->fd);

  if ((nread = clnt->Read(dataPtr->fd, (char *) buf, (size_t) amount)) < 0) {
    string errMsg = KFS::ErrorCodeToStr(nread);
    HT_ERRORF("read failed: fd=%d amount=%d - %s", dataPtr->fd, amount, errMsg.c_str());
    ReportError(cb, nread);
    return;
  }

  cb->response(offset, nread, buf);

}


/**
 * append
 */
void KosmosBroker::append(ResponseCallbackAppend *cb, uint32_t fd, uint32_t amount, uint8_t *data) {
  OpenFileDataKosmosPtr dataPtr;
  ssize_t nwritten;
  uint64_t offset;
  KfsClient *clnt = KfsClient::Instance();

  if (mVerbose) {
    HT_INFOF("append fd=%d amount=%d", fd, amount);
  }

  if (!m_open_file_map.get(fd, dataPtr)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  offset = clnt->Tell(dataPtr->fd);

  if ((nwritten = clnt->Write(dataPtr->fd, (const char *) data, (size_t) amount)) < 0) {
    string errMsg = KFS::ErrorCodeToStr(nwritten);
    HT_ERRORF("write failed: fd=%d amount=%d - %s", dataPtr->fd, amount, errMsg.c_str());
    ReportError(cb, nwritten);
    return;
  }

  cb->response(offset, nwritten);
  
  return;
}


/**
 * seek
 */
void KosmosBroker::seek(ResponseCallback *cb, uint32_t fd, uint64_t offset) {
  OpenFileDataKosmosPtr dataPtr;
  KfsClient *clnt = KfsClient::Instance();

  if (!m_open_file_map.get(fd, dataPtr)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  if ((offset = (uint64_t) clnt->Seek(dataPtr->fd, offset, SEEK_SET)) == (uint64_t) -1) {
    string errMsg = KFS::ErrorCodeToStr(offset);
    HT_ERRORF("lseek failed: fd=%d offset=%lld - %s", dataPtr->fd, offset, errMsg.c_str());
    ReportError(cb, (int) offset);
    return;
  }

  cb->response_ok();
}


/**
 * remove
 */
void KosmosBroker::remove(ResponseCallback *cb, const char *fileName) {
  std::string absFileName;
  KfsClient *clnt = KfsClient::Instance();
  int res;
  
  if (mVerbose) {
    HT_INFOF("remove file='%s'", fileName);
  }

  if (fileName[0] == '/')
    absFileName = fileName;
  else
    absFileName = mRootdir + "/" + fileName;

  if ((res = clnt->Remove(absFileName.c_str())) < 0) {
    string errMsg = KFS::ErrorCodeToStr(res);
    HT_ERRORF("unlink failed: file='%s' - %s", absFileName.c_str(), errMsg.c_str());
    ReportError(cb, res);
    return;
  }

  cb->response_ok();
}


/**
 * length
 */
void KosmosBroker::length(ResponseCallbackLength *cb, const char *fileName) {
  std::string absFileName;
  uint64_t length;
  int res;
  KfsClient *clnt = KfsClient::Instance();
  struct stat statInfo;
  
  if (mVerbose) {
    HT_INFOF("length file='%s'", fileName);
  }

  if (fileName[0] == '/')
    absFileName = fileName;
  else
    absFileName = mRootdir + "/" + fileName;

  if ((res = clnt->Stat(absFileName.c_str(), statInfo)) < 0) {
    string errMsg = KFS::ErrorCodeToStr(res);
    HT_ERRORF("length (stat) failed: file='%s' - %s", absFileName.c_str(), errMsg.c_str());
    ReportError(cb, res);
    return;
  }
  length = statInfo.st_size;
  cb->response(length);
}


/**
 * pread
 */
void KosmosBroker::pread(ResponseCallbackRead *cb, uint32_t fd, uint64_t offset, uint32_t amount) {
  OpenFileDataKosmosPtr dataPtr;
  ssize_t nread;
  uint8_t *buf;
  KfsClient *clnt = KfsClient::Instance();

  if (mVerbose) {
    HT_INFOF("pread fd=%d offset=%lld amount=%d", fd, offset, amount);
  }

  buf = new uint8_t [ amount ]; // TODO: we should sanity check this amount

  if (!m_open_file_map.get(fd, dataPtr)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  if ((offset = (uint64_t) clnt->Seek(dataPtr->fd, offset, SEEK_SET)) == (uint64_t) -1) {
    string errMsg = KFS::ErrorCodeToStr(offset);
    HT_ERRORF("lseek failed: fd=%d offset=%lld - %s", dataPtr->fd, offset, errMsg.c_str());
    ReportError(cb, (int) offset);
    return;
  }

  if ((nread = clnt->Read(dataPtr->fd, (char *) buf, (size_t) amount)) < 0) {
    string errMsg = KFS::ErrorCodeToStr(nread);
    HT_ERRORF("read failed: fd=%d amount=%d - %s", dataPtr->fd, amount, errMsg.c_str());
    ReportError(cb, nread);
    return;
  }
  
  cb->response(offset, nread, buf);
}


/**
 * mkdirs
 */
void KosmosBroker::mkdirs(ResponseCallback *cb, const char *dirName) {
  std::string absDirName;
  std::string::size_type curr = 0;
  KfsClient *clnt = KfsClient::Instance();
  int res = 0;
  struct stat statBuf;
  
  if (mVerbose) {
    HT_INFOF("mkdirs dir='%s'", dirName);
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
    HT_ERRORF("mkdirs failed: dirName='%s' - %s", absDirName.c_str(), errMsg.c_str());
    ReportError(cb, res);
    return;
  }

  cb->response_ok();
}


/**
 * rmdir
 */
void KosmosBroker::rmdir(ResponseCallback *cb, const char *dirName) {
  std::string absDirName;
  KfsClient *clnt = KfsClient::Instance();
  int res;
  
  if (mVerbose) {
    HT_INFOF("rmdir dir='%s'", dirName);
  }

  if (dirName[0] == '/')
    absDirName = dirName;
  else
    absDirName = mRootdir + "/" + dirName;

  if ((res = clnt->Rmdirs(absDirName.c_str())) != 0) {
    string errMsg = KFS::ErrorCodeToStr(res);
    HT_ERRORF("rmdir failed: dirName='%s' - %s", absDirName.c_str(), errMsg.c_str());
    ReportError(cb, res);
    return;
  }

  cb->response_ok();
}


/**
 * flush
 */
void KosmosBroker::flush(ResponseCallback *cb, uint32_t fd) {
  OpenFileDataKosmosPtr dataPtr;
  KfsClient *clnt = KfsClient::Instance();
  int res;

  if (mVerbose) {
    HT_INFOF("flush fd=%d", fd);
  }

  if (!m_open_file_map.get(fd, dataPtr)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  if ((res = clnt->Sync(dataPtr->fd)) < 0) {
    string errMsg = KFS::ErrorCodeToStr(res);
    HT_ERRORF("flush failed: fd=%d - %s", dataPtr->fd, errMsg.c_str());
    ReportError(cb, res);
    return;
  }

  cb->response_ok();
}


/**
 * status
 */
void KosmosBroker::status(ResponseCallback *cb) {
  cb->response_ok();
}


/**
 * shutdown
 */
void KosmosBroker::shutdown(ResponseCallback *cb) {
  m_open_file_map.remove_all();
  cb->response_ok();
  poll(0, 0, 2000);
}


void KosmosBroker::readdir(ResponseCallbackReaddir *cb, const char *dirName) {
  std::vector<std::string> listing;
  std::vector<std::string> stripped_listing;
  std::string abs_path;
  KfsClient *clnt = KfsClient::Instance();
  int error;

  if (mVerbose) {
    HT_INFOF("readdir dir_name='%s'", dirName);
  }

  if (dirName[0] == '/')
    abs_path = dirName;
  else
    abs_path = mRootdir + "/" + dirName;

  if ((error = clnt->Readdir(abs_path.c_str(), listing)) < 0) {
    string errMsg = KFS::ErrorCodeToStr(error);
    HT_ERRORF("readdir failed: file='%s' - %s", abs_path.c_str(), errMsg.c_str());
    ReportError(cb, error);
    return;
  }

  for (size_t i=0;i<listing.size(); i++) {
	 if (listing[i] == "." || listing[i] == "..")
	   continue;
	 stripped_listing.push_back(listing[i]);
       }

  HT_INFOF("Sending back %d listings", stripped_listing.size());

  cb->response(stripped_listing);
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
