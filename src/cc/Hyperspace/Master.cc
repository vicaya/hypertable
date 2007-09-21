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
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/xattr.h>
}

#include "Common/Error.h"
#include "Common/System.h"

#include "DfsBroker/Lib/Client.h"
#include "Hypertable/Lib/Schema.h"

#include "Master.h"
#include "SessionData.h"

using namespace hypertable;
using namespace Hyperspace;
using namespace std;

const uint32_t Master::DEFAULT_MASTER_PORT;
const uint32_t Master::DEFAULT_LEASE_INTERVAL;
const uint32_t Master::DEFAULT_KEEPALIVE_INTERVAL;

atomic_t Master::msNextSessionId = ATOMIC_INIT(1);

Master::Master(ConnectionManager *connManager, PropertiesPtr &propsPtr) : mMutex(), mVerbose(false) {
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
  if ((xattrLen = getxattr(mBaseDir.c_str(), "generation", &mGeneration, sizeof(uint32_t), 0, 0)) < 0) {
    if (errno == ENOATTR) {
      cerr << "'generation' attribute not found on base dir, creating ..." << endl;
      mGeneration = 1;
      if (setxattr(mBaseDir.c_str(), "generation", &mGeneration, sizeof(uint32_t), 0, XATTR_CREATE) == -1) {
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
    if (setxattr(mBaseDir.c_str(), "generation", &mGeneration, sizeof(uint32_t), 0, XATTR_REPLACE) == -1) {
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
  boost::mutex::scoped_lock lock(mMutex);
  uint32_t sessionId = atomic_inc_return(&msNextSessionId);
  sessionPtr = new SessionData(addr, mLeaseInterval, sessionId);
  mSessionMap[sessionId] = sessionPtr;
  return;
}


bool Master::GetSession(uint32_t sessionId, SessionDataPtr &sessionPtr) {
  boost::mutex::scoped_lock lock(mMutex);
  SessionMapT::iterator iter = mSessionMap.find(sessionId);
  if (iter == mSessionMap.end())
    return false;
  sessionPtr = (*iter).second;
  return true;
}

void Master::Mkdir(ResponseCallback *cb, const char *name) {
  std::string absName;
  
  if (mVerbose) {
    LOG_VA_INFO("mkdir %s", name);
  }

  if (name[0] == '/')
    absName = mBaseDir + name;
  else
    absName = mBaseDir + "/" + name;

  if (mkdir(absName.c_str(), 0755) < 0)
    ReportError(cb);
  else
    cb->response_ok();

  return;
    
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
  else
    cb->error(Error::HYPERSPACE_IO_ERROR, errbuf);
}
