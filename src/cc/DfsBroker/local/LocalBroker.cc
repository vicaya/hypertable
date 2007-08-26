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
}

#include "Common/System.h"

#include "LocalBroker.h"

using namespace hypertable;

//atomic_t LocalBroker::msUniqueId = ATOMIC_INIT(0);

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

}



LocalBroker::~LocalBroker() {
}



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
    char errbuf[128];
    strerror_r(errno, errbuf, 128);

    LOG_VA_INFO("open failed: file='%s' - %s", absFileName.c_str(), errbuf);
    if (errno == ENOTDIR || errno == ENAMETOOLONG || errno == ENOENT)
      cb->error(Error::DFSBROKER_BAD_FILENAME, (const char *)errbuf);
    else if (errno == EACCES)
      cb->error(Error::DFSBROKER_PERMISSION_DENIED, (const char *)errbuf);
    else
      cb->error(Error::DFSBROKER_IO_ERROR, (const char *)errbuf);
    return;
  }


  {
    int error;
    struct sockaddr_in addr;
    OpenFileDataLocalPtr dataPtr(new OpenFileDataLocal(fd));

    cb->get_address(addr);

    mOpenFileMap.Create(fd, addr, dataPtr);

    if ((error = cb->response(fd)) != Error::OK) {
      LOG_VA_ERROR("Problem sending response back to OPEN request - %s", Error::GetText(error));
    }
  }
}

void LocalBroker::Create(ResponseCallbackOpen *cb, const char *fileName, bool overwrite,
	    uint32_t bufferSize, uint16_t replication, uint64_t blockSize) {
  return;
}

void LocalBroker::Close(ResponseCallback *cb, uint32_t fd) {
  return;
}

void LocalBroker::Read(ResponseCallbackRead *cb, uint32_t fd, uint32_t amount) {
  return;
}

void LocalBroker::Append(ResponseCallbackAppend *cb, uint32_t fd, uint32_t amount, uint8_t *data) {
  return;
}

void LocalBroker::Seek(ResponseCallback *cb, uint32_t fd, uint64_t offset) {
  return;
}

void LocalBroker::Remove(ResponseCallback *cb, const char *fileName) {
  return;
}

void LocalBroker::Length(ResponseCallbackLength *cb, const char *fileName) {
  return;
}

void LocalBroker::Pread(ResponseCallbackRead *cb, uint32_t fd, uint64_t offset, uint32_t amount) {
  return;
}

void LocalBroker::Mkdirs(ResponseCallback *cb, const char *dirName) {
  return;
}

void LocalBroker::Flush(ResponseCallback *cb, uint32_t fd) {
  return;
}

void LocalBroker::Status(ResponseCallback *cb) {
  return;
}

void LocalBroker::Shutdown(ResponseCallback *cb, uint16_t flags) {
  return;
}
