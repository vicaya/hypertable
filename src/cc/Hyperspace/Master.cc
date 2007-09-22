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

#include <cstring>

extern "C" {
#include <fcntl.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/xattr.h>
}

#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/System.h"

#include "DfsBroker/Lib/Client.h"
#include "Hypertable/Lib/Schema.h"

#include "Master.h"
#include "Session.h"
#include "SessionData.h"

using namespace hypertable;
using namespace Hyperspace;
using namespace std;

const uint32_t Master::DEFAULT_MASTER_PORT;
const uint32_t Master::DEFAULT_LEASE_INTERVAL;
const uint32_t Master::DEFAULT_KEEPALIVE_INTERVAL;

Master::Master(ConnectionManager *connManager, PropertiesPtr &propsPtr) : mVerbose(false), mNextHandleNumber(1), mNextSessionId(1) {
  const char *dirname;
  std::string str;
  ssize_t xattrLen;

  mVerbose = propsPtr->getPropertyBool("verbose", false);

  mLeaseInterval = (uint32_t)propsPtr->getPropertyInt("Hyperspace.Lease.Interval", DEFAULT_LEASE_INTERVAL);

  mKeepAliveInterval = (uint32_t)propsPtr->getPropertyInt("Hyperspace.KeepAlive.Interval", DEFAULT_KEEPALIVE_INTERVAL);

  if ((dirname = propsPtr->getProperty("Hyperspace.Master.dir", 0)) == 0) {
    LOG_ERROR("Property 'Hyperspace.Master.dir' not found.");
    exit(1);
  }

  if (dirname[strlen(dirname)-1] == '/')
    str = std::string(dirname, strlen(dirname)-1);
  else
    str = dirname;

  if (dirname[0] == '/')
    mBaseDir = str;
  else
    mBaseDir = System::installDir + "/" + str;

  if ((mBaseFd = open(mBaseDir.c_str(), O_RDONLY)) < 0) {
    LOG_VA_ERROR("Unable to open base directory %s - %s", mBaseDir.c_str(), strerror(errno));
    exit(1);
  }

  /**
   * Lock the base directory to prevent concurrent masters
   */
  if (flock(mBaseFd, LOCK_EX | LOCK_NB) != 0) {
    if (errno == EWOULDBLOCK) {
      LOG_VA_ERROR("Base directory '%s' is locked by another process.", mBaseDir.c_str());
    }
    else {
      LOG_VA_ERROR("Unable to lock base directory '%s' - %s", mBaseDir.c_str(), strerror(errno));
    }
    exit(1);
  }

  /**
   * Increment generation number and save
   */
  if ((xattrLen = FileUtils::Getxattr(mBaseDir.c_str(), "generation", &mGeneration, sizeof(uint32_t))) < 0) {
    if (errno == ENOATTR) {
      cerr << "'generation' attribute not found on base dir, creating ..." << endl;
      mGeneration = 1;
      if (FileUtils::Setxattr(mBaseDir.c_str(), "generation", &mGeneration, sizeof(uint32_t), XATTR_CREATE) == -1) {
	LOG_VA_ERROR("Problem creating extended attribute 'generation' on base dir '%s' - %s", mBaseDir.c_str(), strerror(errno));
	exit(1);
      }
    }
    else {
      LOG_VA_ERROR("Unable to read extended attribute 'generation' on base dir '%s' - %s", mBaseDir.c_str(), strerror(errno));
      exit(1);
    }
  }
  else {
    mGeneration++;
    if (FileUtils::Setxattr(mBaseDir.c_str(), "generation", &mGeneration, sizeof(uint32_t), XATTR_REPLACE) == -1) {
      LOG_VA_ERROR("Problem creating extended attribute 'generation' on base dir '%s' - %s", mBaseDir.c_str(), strerror(errno));
      exit(1);
    }
  }

  if (mVerbose) {
    cout << "Hyperspace.Lease.Interval=" << mLeaseInterval << endl;
    cout << "Hyperspace.KeepAlive.Interval=" << mKeepAliveInterval << endl;
    cout << "Hyperspace.Master.dir=" << mBaseDir << endl;
    cout << "Generation=" << mGeneration << endl;
  }

}


Master::~Master() {
  close(mBaseFd);
  return;
}


void Master::CreateSession(struct sockaddr_in &addr, SessionDataPtr &sessionPtr) {
  boost::mutex::scoped_lock lock(mSessionMapMutex);
  uint64_t sessionId = mNextSessionId++;
  sessionPtr = new SessionData(addr, mLeaseInterval, sessionId);
  mSessionMap[sessionId] = sessionPtr;
  return;
}


bool Master::GetSession(uint64_t sessionId, SessionDataPtr &sessionPtr) {
  boost::mutex::scoped_lock lock(mSessionMapMutex);
  SessionMapT::iterator iter = mSessionMap.find(sessionId);
  if (iter == mSessionMap.end())
    return false;
  sessionPtr = (*iter).second;
  return true;
}


/**
 *
 */
void Master::CreateHandle(uint64_t *handlep, HandleDataPtr &handlePtr) {
  boost::mutex::scoped_lock lock(mHandleMapMutex);

  *handlep = ++mNextHandleNumber;

  handlePtr = new HandleData();

  mHandleMap[*handlep] = handlePtr;
}



/**
 * Mkdir
 */
void Master::Mkdir(ResponseCallback *cb, uint64_t sessionId, const char *name) {
  std::string normalName;
  std::string absName;
  
  if (mVerbose) {
    LOG_VA_INFO("mkdir %s", name);
  }

  NormalizeName(name, normalName);

  absName = mBaseDir + normalName;

  if (mkdir(absName.c_str(), 0755) < 0)
    ReportError(cb);
  else
    cb->response_ok();

  return;
}



/**
 * Open
 */
void Master::Open(ResponseCallbackOpen *cb, uint64_t sessionId, const char *name, uint32_t flags, uint32_t eventMask) {
  std::string normalName;
  std::string absName;
  SessionDataPtr sessionPtr;
  NodeDataPtr nodePtr;
  HandleDataPtr handlePtr;
  int error;
  bool created = false;
  bool existed;
  uint64_t handle;
  struct stat statbuf;
  int oflags = 0;

  if (mVerbose) {
    LOG_VA_INFO("open(%s, flags=0x%x, eventMask=0x%x)", name, flags, eventMask);
  }

  NormalizeName(name, normalName);

  absName = mBaseDir + normalName;

  if (!GetSession(sessionId, sessionPtr)) {
    cb->error(Error::HYPERSPACE_EXPIRED_SESSION, "");
    return;
  }

  {
    boost::mutex::scoped_lock lock(mNodeMapMutex);
    int fd;

    NodeMapT::iterator iter = mNodeMap.find(normalName);
    if (iter != mNodeMap.end()) {
      if ((flags & OPEN_FLAG_CREATE) && (flags & OPEN_FLAG_EXCL)) {
	cb->error(Error::HYPERSPACE_FILE_EXISTS, "mode=CREATE|EXCL");
	return;
      }
      nodePtr = (*iter).second;
    }

    if (stat(absName.c_str(), &statbuf) != 0) {
      if (errno == ENOENT)
	existed = false;
      else {
	ReportError(cb);
	return;
      }
    }
    else {
      existed = true;
      if (statbuf.st_mode & S_IFDIR) {
	if (flags & OPEN_FLAG_WRITE) {
	  cb->error(Error::HYPERSPACE_IS_DIRECTORY, "");
	  return;
	}
	oflags = O_RDONLY;
      }
      else
	oflags = O_RDWR;
    }

    if (!nodePtr || nodePtr->fd < 0) {
      if (flags & OPEN_FLAG_CREATE)
	oflags |= O_CREAT;
      if (flags & OPEN_FLAG_EXCL)
	oflags |= O_EXCL;
      fd = open(absName.c_str(), oflags, 0644);
      if (fd < 0) {
	ReportError(cb);
	return;
      }
      if (!nodePtr) {
	nodePtr = new NodeData();
	nodePtr->name = normalName;
	mNodeMap[normalName] = nodePtr;
      }
      nodePtr->fd = fd;
      if (!existed)
	created = true;
    }

    CreateHandle(&handle, handlePtr);

    handlePtr->nodePtr = nodePtr;
    handlePtr->openFlags = flags;
    handlePtr->eventMask = eventMask;

  }

  cb->response(handle, created);
  
}



/**
 * ReportError
 */
void Master::ReportError(ResponseCallback *cb) {
  char errbuf[128];
  errbuf[0] = 0;
  strerror_r(errno, errbuf, 128);
  if (errno == ENOTDIR || errno == ENAMETOOLONG || errno == ENOENT)
    cb->error(Error::HYPERSPACE_BAD_PATHNAME, errbuf);
  else if (errno == EACCES || errno == EPERM)
    cb->error(Error::HYPERSPACE_PERMISSION_DENIED, errbuf);
  else if (errno == EEXIST)
    cb->error(Error::HYPERSPACE_FILE_EXISTS, errbuf);
  else
    cb->error(Error::HYPERSPACE_IO_ERROR, errbuf);
}



/**
 *
 */
void Master::NormalizeName(std::string name, std::string &normal) {
  normal = "";
  if (name[0] != '/')
    normal += "/";

  if (name.find('/', name.length()-1) == string::npos)
    normal += name;
  else
    normal += name.substr(0, name.length()-1);
}
