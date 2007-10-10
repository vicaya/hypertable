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

#include <algorithm>
#include <cstring>

extern "C" {
#include <dirent.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/file.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/xattr.h>
#include <unistd.h>
}

#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/StringExt.h"
#include "Common/System.h"

#include "DfsBroker/Lib/Client.h"
#include "Hypertable/Lib/Schema.h"

#include "Event.h"
#include "Notification.h"
#include "Master.h"
#include "Session.h"
#include "SessionData.h"

using namespace hypertable;
using namespace Hyperspace;
using namespace std;

const uint32_t Master::DEFAULT_MASTER_PORT;
const uint32_t Master::DEFAULT_LEASE_INTERVAL;
const uint32_t Master::DEFAULT_KEEPALIVE_INTERVAL;

/**
 * This constructor reads the properties file and pulls out some relevant properties.  It
 * also sets up the mBaseDir variable to be the absolute path of the root of the
 * Hyperspace directory (with no trailing slash).  It locks this root directory to
 * prevent concurrent masters and then reads/increments/writes the 32-bit integer extended
 * attribute 'generation'.  It also creates the server Keepalive handler.
 */
Master::Master(ConnectionManager *connManager, PropertiesPtr &propsPtr, ServerKeepaliveHandlerPtr &keepaliveHandlerPtr) : mVerbose(false), mNextHandleNumber(1), mNextSessionId(1) {
  const char *dirname;
  std::string str;
  ssize_t xattrLen;
  uint16_t port;

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

  NodeDataPtr rootNodePtr = new NodeData();
  rootNodePtr->name = "/";
  mNodeMap["/"] = rootNodePtr;

  port = propsPtr->getPropertyInt("Hyperspace.Master.port", DEFAULT_MASTER_PORT);
  InetAddr::Initialize(&mLocalAddr, INADDR_ANY, port);

  mKeepaliveHandlerPtr.reset( new ServerKeepaliveHandler(connManager->GetComm(), this) );
  keepaliveHandlerPtr = mKeepaliveHandlerPtr;

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


uint64_t Master::CreateSession(struct sockaddr_in &addr) {
  boost::mutex::scoped_lock lock(mSessionMapMutex);
  SessionDataPtr sessionPtr;
  uint64_t sessionId = mNextSessionId++;
  sessionPtr = new SessionData(addr, mLeaseInterval, sessionId);
  mSessionMap[sessionId] = sessionPtr;
  mSessionHeap.push_back(sessionPtr);
  return sessionId;
}


bool Master::GetSession(uint64_t sessionId, SessionDataPtr &sessionPtr) {
  boost::mutex::scoped_lock lock(mSessionMapMutex);
  SessionMapT::iterator iter = mSessionMap.find(sessionId);
  if (iter == mSessionMap.end())
    return false;
  sessionPtr = (*iter).second;
  return true;
}


int Master::RenewSessionLease(uint64_t sessionId) {
  boost::mutex::scoped_lock lock(mSessionMapMutex);  
  boost::xtime now;

  SessionMapT::iterator iter = mSessionMap.find(sessionId);
  if (iter == mSessionMap.end())
    return Error::HYPERSPACE_EXPIRED_SESSION;

  if (!(*iter).second->RenewLease())
    return Error::HYPERSPACE_EXPIRED_SESSION;

  return Error::OK;
}

bool Master::NextExpiredSession(SessionDataPtr &sessionPtr) {
  boost::mutex::scoped_lock lock(mSessionMapMutex);
  struct ltSessionData compFunc;
  boost::xtime now;

  boost::xtime_get(&now, boost::TIME_UTC);

  if (mSessionHeap.size() > 0) {
    std::make_heap(mSessionHeap.begin(), mSessionHeap.end(), compFunc);
    if (mSessionHeap[0]->IsExpired(now)) {
      sessionPtr = mSessionHeap[0];
      std::pop_heap(mSessionHeap.begin(), mSessionHeap.end(), compFunc);
      mSessionHeap.resize(mSessionHeap.size()-1);
      return true;
    }    
  }
  return false;
}


/**
 * 
 */
void Master::RemoveExpiredSessions() {
  SessionDataPtr sessionPtr;
  int error;
  std::string errMsg;
  
  while (NextExpiredSession(sessionPtr)) {

    if (mVerbose) {
      LOG_VA_INFO("Expiring session %lld", sessionPtr->id);
    }

    sessionPtr->Expire();

    {
      boost::mutex::scoped_lock slock(sessionPtr->mutex);
      HandleDataPtr handlePtr;
      for (set<uint64_t>::iterator iter = sessionPtr->handles.begin(); iter != sessionPtr->handles.end(); iter++) {
	if (mVerbose) {
	  LOG_VA_INFO("Destroying handle %lld", *iter);
	}
	if (!DestroyHandle(*iter, &error, errMsg, false)) {
	  LOG_VA_ERROR("Problem destroying handle - %s (%s)", Error::GetText(error), errMsg.c_str());
	}
      }
      sessionPtr->handles.clear();
    }
  }

}


/**
 *
 */
void Master::CreateHandle(uint64_t *handlep, HandleDataPtr &handlePtr) {
  boost::mutex::scoped_lock lock(mHandleMapMutex);
  *handlep = ++mNextHandleNumber;
  handlePtr = new HandleData();
  handlePtr->id = *handlep;
  handlePtr->locked = false;
  mHandleMap[*handlep] = handlePtr;
}

/**
 *
 */
bool Master::GetHandleData(uint64_t handle, HandleDataPtr &handlePtr) {
  boost::mutex::scoped_lock lock(mHandleMapMutex);
  HandleMapT::iterator iter = mHandleMap.find(handle);
  if (iter == mHandleMap.end())
    return false;
  handlePtr = (*iter).second;
  return true;
}

/**
 *
 */
bool Master::RemoveHandleData(uint64_t handle, HandleDataPtr &handlePtr) {
  boost::mutex::scoped_lock lock(mHandleMapMutex);
  HandleMapT::iterator iter = mHandleMap.find(handle);
  if (iter == mHandleMap.end())
    return false;
  handlePtr = (*iter).second;
  mHandleMap.erase(iter);
  return true;
}



/**
 * This method creates a directory with absolute path 'name'.  It locks the parent
 * node to prevent concurrent modifications.
 */
void Master::Mkdir(ResponseCallback *cb, uint64_t sessionId, const char *name) {
  std::string absName;
  NodeDataPtr parentNodePtr;
  std::string childName;

  if (mVerbose) {
    LOG_VA_INFO("mkdir(sessionId=%lld, name=%s)", sessionId, name);
  }

  if (!FindParentNode(name, parentNodePtr, childName)) {
    cb->error(Error::HYPERSPACE_FILE_EXISTS, "directory '/' exists");
    return;
  }

  assert(name[0] == '/' && name[strlen(name)-1] != '/');

  {
    boost::mutex::scoped_lock nodeLock(parentNodePtr->mutex);

    absName = mBaseDir + name;
    
    if (mkdir(absName.c_str(), 0755) < 0) {
      ReportError(cb);
      return;
    }
    else {
      HyperspaceEventPtr eventPtr( new EventNamed(EVENT_MASK_CHILD_NODE_ADDED, childName) );
      DeliverEventNotifications(parentNodePtr.get(), eventPtr);
    }
  }

  cb->response_ok();
}



/**
 * Delete
 */
void Master::Delete(ResponseCallback *cb, uint64_t sessionId, const char *name) {
  std::string absName;
  struct stat statbuf;
  std::string childName;
  NodeDataPtr parentNodePtr;

  if (mVerbose) {
    LOG_VA_INFO("delete(sessionId=%lld, name=%s)", sessionId, name);
  }

  if (!strcmp(name, "/")) {
    cb->error(Error::HYPERSPACE_PERMISSION_DENIED, "Cannot remove '/' directory");
    return;
  }

  if (!FindParentNode(name, parentNodePtr, childName)) {
    cb->error(Error::HYPERSPACE_BAD_PATHNAME, name);
    return;
  }

  assert(name[0] == '/' && name[strlen(name)-1] != '/');

  {
    boost::mutex::scoped_lock nodeLock(parentNodePtr->mutex);

    absName = mBaseDir + name;

    if (stat(absName.c_str(), &statbuf) < 0) {
      ReportError(cb);
      return;
    }
    else {
      if (statbuf.st_mode & S_IFDIR) {
	if (rmdir(absName.c_str()) < 0) {
	  ReportError(cb);
	  return;
	}
      }
      else {
	if (unlink(absName.c_str()) < 0) {
	  ReportError(cb);
	  return;
	}
      }
    }

    // deliver event notifications
    {
      HyperspaceEventPtr eventPtr( new EventNamed(EVENT_MASK_CHILD_NODE_REMOVED, childName) );
      DeliverEventNotifications(parentNodePtr.get(), eventPtr);
    }

  }

  cb->response_ok();
}



/**
 * Open
 */
void Master::Open(ResponseCallbackOpen *cb, uint64_t sessionId, const char *name, uint32_t flags, uint32_t eventMask) {
  std::string absName;
  std::string childName;
  SessionDataPtr sessionPtr;
  NodeDataPtr nodePtr;
  NodeDataPtr parentNodePtr;
  HandleDataPtr handlePtr;
  int error;
  bool created = false;
  bool isDirectory = false;
  bool existed;
  uint64_t handle;
  struct stat statbuf;
  int oflags = 0;
  ssize_t len;
  bool lockNotify = false;
  uint32_t lockMode = 0;
  uint64_t lockGeneration = 0;

  if (mVerbose) {
    LOG_VA_INFO("open(sessionId=%lld, fname=%s, flags=0x%x, eventMask=0x%x)", sessionId, name, flags, eventMask);
  }

  assert(name[0] == '/');

  absName = mBaseDir + name;

  if (!GetSession(sessionId, sessionPtr)) {
    cb->error(Error::HYPERSPACE_EXPIRED_SESSION, "");
    return;
  }

  if (!FindParentNode(name, parentNodePtr, childName)) {
    cb->error(Error::HYPERSPACE_BAD_PATHNAME, name);
    return;
  }

  // If path name to open is "/", then create a dummy NodeData object for
  // the parent because the previous call will return the node itself as
  // the parent, causing the second of the following two mutex locks to hang
  if (!strcmp(name, "/"))
    parentNodePtr = new NodeData();

  GetNode(name, nodePtr);

  {
    boost::mutex::scoped_lock parentLock(parentNodePtr->mutex);
    boost::mutex::scoped_lock nodeLock(nodePtr->mutex);

    if ((flags & OPEN_FLAG_LOCK_SHARED) == OPEN_FLAG_LOCK_SHARED) {
      if (nodePtr->exclusiveLockHandle != 0) {
	cb->error(Error::HYPERSPACE_LOCK_CONFLICT, "");
	return;
      }
      lockMode = LOCK_MODE_SHARED;
      if (nodePtr->sharedLockHandles.empty())
	lockNotify = true;
    }
    else if ((flags & OPEN_FLAG_LOCK_EXCLUSIVE) == OPEN_FLAG_LOCK_EXCLUSIVE) {
      if (nodePtr->exclusiveLockHandle != 0 || !nodePtr->sharedLockHandles.empty()) {
	cb->error(Error::HYPERSPACE_LOCK_CONFLICT, "");
	return;
      }
      lockMode = LOCK_MODE_EXCLUSIVE;
      lockNotify = true;
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
	/** if (flags & OPEN_FLAG_WRITE) {
	    cb->error(Error::HYPERSPACE_IS_DIRECTORY, "");
	    return;
	    } **/
	isDirectory = true;
	oflags = O_RDONLY;
#if defined(__linux__)
	oflags |= O_DIRECTORY;
#endif
      }
    }

    if (!isDirectory)
      oflags = O_RDWR;

    if (existed && (flags & OPEN_FLAG_CREATE) && (flags & OPEN_FLAG_EXCL)) {
      cb->error(Error::HYPERSPACE_FILE_EXISTS, "mode=CREATE|EXCL");
      return;
    }

    if (nodePtr->fd < 0) {

      if (existed && (flags & OPEN_FLAG_TEMP)) {
	cb->error(Error::HYPERSPACE_FILE_EXISTS, "Unable to open TEMP file because it exists and is permanent");
	return;
      }

      if (!existed) {
	if (flags & OPEN_FLAG_CREATE)
	  oflags |= O_CREAT;
	if (flags & OPEN_FLAG_EXCL)
	  oflags |= O_EXCL;
      }
      nodePtr->fd = open(absName.c_str(), oflags, 0644);
      if (nodePtr->fd < 0) {
	LOG_VA_ERROR("open(%s, 0x%x, 0644) failed (errno=%d) - %s", absName.c_str(), oflags, errno, strerror(errno));
	ReportError(cb);
	return;
      }

      // Read/create 'lock.generation' attribute

      len = FileUtils::Fgetxattr(nodePtr->fd, "lock.generation", &nodePtr->lockGeneration, sizeof(uint64_t));
      if (len < 0) {
	if (errno == ENOATTR) {
	  nodePtr->lockGeneration = 1;
	  if (FileUtils::Fsetxattr(nodePtr->fd, "lock.generation", &nodePtr->lockGeneration, sizeof(uint64_t), 0) == -1) {
	    LOG_VA_ERROR("Problem creating extended attribute 'lock.generation' on file '%s' - %s",
			 name, strerror(errno));
	    DUMP_CORE;
	  }
	}
	else {
	  LOG_VA_ERROR("Problem reading extended attribute 'lock.generation' on file '%s' - %s",
		       name, strerror(errno));
	  DUMP_CORE;
	}
	len = sizeof(int64_t);
      }

      assert(len == sizeof(int64_t));

      if (flags & OPEN_FLAG_TEMP) {
	nodePtr->ephemeral = true;
	unlink(absName.c_str());
      }

    }
    else {
      if (existed && (flags & OPEN_FLAG_TEMP) && !nodePtr->ephemeral) {
	cb->error(Error::HYPERSPACE_FILE_EXISTS, "Unable to open TEMP file because it exists and is permanent");
	return;
      }
    }

    if (!existed)
      created = true;

    CreateHandle(&handle, handlePtr);

    handlePtr->node = nodePtr.get();
    handlePtr->openFlags = flags;
    handlePtr->eventMask = eventMask;
    handlePtr->sessionPtr = sessionPtr;

    sessionPtr->AddHandle(handle);

    if (created) {
      HyperspaceEventPtr eventPtr( new EventNamed(EVENT_MASK_CHILD_NODE_ADDED, childName) );
      DeliverEventNotifications(parentNodePtr.get(), eventPtr);
    }

    /**
     * If open flags LOCK_SHARED or LOCK_EXCLUSIVE, then obtain lock
     */
    if (lockMode != 0) {
      handlePtr->node->lockGeneration++;
      if (FileUtils::Fsetxattr(handlePtr->node->fd, "lock.generation", &handlePtr->node->lockGeneration, sizeof(uint64_t), 0) == -1) {
	LOG_VA_ERROR("Problem creating extended attribute 'lock.generation' on file '%s' - %s",
		     handlePtr->node->name.c_str(), strerror(errno));
	exit(1);
      }
      lockGeneration = handlePtr->node->lockGeneration;
      handlePtr->node->currentLockMode = lockMode;

      LockHandle(handlePtr, lockMode);

      // deliver notification to handles to this same node
      if (lockNotify) {
	HyperspaceEventPtr eventPtr( new EventLockAcquired(lockMode) );
	DeliverEventNotifications(handlePtr->node, eventPtr);
      }
    }

    handlePtr->node->AddHandle(handle, handlePtr);

  }

  cb->response(handle, created, lockGeneration);
}


/**
 *
 */
void Master::Close(ResponseCallback *cb, uint64_t sessionId, uint64_t handle) {
  SessionDataPtr sessionPtr;
  int error;
  std::string errMsg;

  if (mVerbose) {
    LOG_VA_INFO("close(session=%lld, handle=%lld)", sessionId, handle);
  }

  if (!GetSession(sessionId, sessionPtr)) {
    cb->error(Error::HYPERSPACE_EXPIRED_SESSION, "");
    return;
  }

  if (!DestroyHandle(handle, &error, errMsg)) {
    cb->error(error, errMsg);
    return;
  }

  if ((error = cb->response_ok()) != Error::OK) {
    LOG_VA_ERROR("Problem sending back response - %s", Error::GetText(error));
  }
}



/**
 *
 */
void Master::AttrSet(ResponseCallback *cb, uint64_t sessionId, uint64_t handle, const char *name, const void *value, size_t valueLen) {
  SessionDataPtr sessionPtr;
  HandleDataPtr handlePtr;
  int error;

  if (mVerbose) {
    LOG_VA_INFO("attrset(session=%lld, handle=%lld, name=%s, valueLen=%d)", sessionId, handle, name, valueLen);
  }

  if (!GetSession(sessionId, sessionPtr)) {
    cb->error(Error::HYPERSPACE_EXPIRED_SESSION, "");
    return;
  }

  if (!GetHandleData(handle, handlePtr)) {
    cb->error(Error::HYPERSPACE_INVALID_HANDLE, std::string("handle=") + handle);
    return;
  }

  {
    boost::mutex::scoped_lock nodeLock(handlePtr->node->mutex);

    if (FileUtils::Fsetxattr(handlePtr->node->fd, name, value, valueLen, 0) == -1) {
      LOG_VA_ERROR("Problem creating extended attribute '%s' on file '%s' - %s",
		   name, handlePtr->node->name.c_str(), strerror(errno));
      DUMP_CORE;
    }

    HyperspaceEventPtr eventPtr( new EventNamed(EVENT_MASK_ATTR_SET, name) );
    DeliverEventNotifications(handlePtr->node, eventPtr);
  }

  if ((error = cb->response_ok()) != Error::OK) {
    LOG_VA_ERROR("Problem sending back response - %s", Error::GetText(error));
  }

}


/**
 * 
 */
void Master::AttrGet(ResponseCallbackAttrGet *cb, uint64_t sessionId, uint64_t handle, const char *name) {
  SessionDataPtr sessionPtr;
  HandleDataPtr handlePtr;
  int error;
  int alen;
  uint8_t *buf;

  if (mVerbose) {
    LOG_VA_INFO("attrget(session=%lld, handle=%lld, name=%s)", sessionId, handle, name);
  }

  if (!GetSession(sessionId, sessionPtr)) {
    cb->error(Error::HYPERSPACE_EXPIRED_SESSION, "");
    return;
  }

  if (!GetHandleData(handle, handlePtr)) {
    cb->error(Error::HYPERSPACE_INVALID_HANDLE, std::string("handle=") + handle);
    return;
  }

  {
    boost::mutex::scoped_lock nodeLock(handlePtr->node->mutex);

    if ((alen = FileUtils::Fgetxattr(handlePtr->node->fd, name, 0, 0)) < 0) {
      LOG_VA_ERROR("Problem determining size of extended attribute '%s' on file '%s' - %s",
		   name, handlePtr->node->name.c_str(), strerror(errno));
      ReportError(cb);
      return;
    }

    buf = new uint8_t [ alen + 8 ];

    if ((alen = FileUtils::Fgetxattr(handlePtr->node->fd, name, buf, alen)) < 0) {
      LOG_VA_ERROR("Problem determining size of extended attribute '%s' on file '%s' - %s",
		   name, handlePtr->node->name.c_str(), strerror(errno));
      DUMP_CORE;
    }
  }

  if ((error = cb->response(buf, (uint32_t)alen)) != Error::OK) {
    LOG_VA_ERROR("Problem sending back response - %s", Error::GetText(error));
  }
  
}



void Master::AttrDel(ResponseCallback *cb, uint64_t sessionId, uint64_t handle, const char *name) {
  SessionDataPtr sessionPtr;
  HandleDataPtr handlePtr;
  int error;

  if (mVerbose) {
    LOG_VA_INFO("attrdel(session=%lld, handle=%lld, name=%s)", sessionId, handle, name);
  }

  if (!GetSession(sessionId, sessionPtr)) {
    cb->error(Error::HYPERSPACE_EXPIRED_SESSION, "");
    return;
  }

  if (!GetHandleData(handle, handlePtr)) {
    cb->error(Error::HYPERSPACE_INVALID_HANDLE, std::string("handle=") + handle);
    return;
  }

  {
    boost::mutex::scoped_lock nodeLock(handlePtr->node->mutex);

    if (FileUtils::Fremovexattr(handlePtr->node->fd, name) == -1) {
      LOG_VA_ERROR("Problem removing extended attribute '%s' on file '%s' - %s",
		   name, handlePtr->node->name.c_str(), strerror(errno));
      ReportError(cb);
      return;
    }

    HyperspaceEventPtr eventPtr( new EventNamed(EVENT_MASK_ATTR_DEL, name) );
    DeliverEventNotifications(handlePtr->node, eventPtr);
  }

  if ((error = cb->response_ok()) != Error::OK) {
    LOG_VA_ERROR("Problem sending back response - %s", Error::GetText(error));
  }

}


void Master::Exists(ResponseCallbackExists *cb, uint64_t sessionId, const char *name) {
  std::string absName;
  int error;

  if (mVerbose) {
    LOG_VA_INFO("exists(sessionId=%lld, name=%s)", sessionId, name);
  }

  assert(name[0] == '/' && name[strlen(name)-1] != '/');

  absName = mBaseDir + name;

  if ((error = cb->response( FileUtils::Exists(absName.c_str()) )) != Error::OK) {
    LOG_VA_ERROR("Problem sending back response - %s", Error::GetText(error));
  }
}


void Master::Readdir(ResponseCallbackReaddir *cb, uint64_t sessionId, uint64_t handle) {
  std::string absName;
  SessionDataPtr sessionPtr;
  HandleDataPtr handlePtr;
  struct DirEntryT dentry;
  std::vector<struct DirEntryT> listing;

  if (mVerbose) {
    LOG_VA_INFO("readdir(session=%lld, handle=%lld)", sessionId, handle);
  }

  if (!GetSession(sessionId, sessionPtr)) {
    cb->error(Error::HYPERSPACE_EXPIRED_SESSION, "");
    return;
  }

  if (!GetHandleData(handle, handlePtr)) {
    cb->error(Error::HYPERSPACE_INVALID_HANDLE, std::string("handle=") + handle);
    return;
  }

  {
    boost::mutex::scoped_lock lock(handlePtr->node->mutex);

    absName = mBaseDir + handlePtr->node->name;

    DIR *dirp = opendir(absName.c_str());

    if (dirp == 0) {
      ReportError(cb);
      LOG_VA_ERROR("opendir('%s') failed - %s", absName.c_str(), strerror(errno));
      return;
    }

    struct dirent dent;
    struct dirent *dp;

    if (readdir_r(dirp, &dent, &dp) != 0) {
      ReportError(cb);
      LOG_VA_ERROR("readdir('%s') failed - %s", absName.c_str(), strerror(errno));
      (void)closedir(dirp);
      return;
    }

    while (dp != 0) {

      if (dp->d_name[0] != 0 && strcmp(dp->d_name, ".") && strcmp(dp->d_name, "..")) {
	dentry.isDirectory = (dp->d_type == DT_DIR);
	dentry.name = dp->d_name;
	listing.push_back(dentry);
      }

      if (readdir_r(dirp, &dent, &dp) != 0) {
	ReportError(cb);
	LOG_VA_ERROR("readdir('%s') failed - %s", absName.c_str(), strerror(errno));
	(void)closedir(dirp);
	return;
      }
    }
    (void)closedir(dirp);

  }

  cb->response(listing);
}



void Master::Lock(ResponseCallbackLock *cb, uint64_t sessionId, uint64_t handle, uint32_t mode, bool tryAcquire) {
  SessionDataPtr sessionPtr;
  HandleDataPtr handlePtr;
  int error;
  bool notify = true;

  if (mVerbose) {
    LOG_VA_INFO("lock(session=%lld, handle=%lld, mode=0x%x, tryAcquire=%d)", sessionId, handle, mode, tryAcquire);
  }

  if (!GetSession(sessionId, sessionPtr)) {
    cb->error(Error::HYPERSPACE_EXPIRED_SESSION, "");
    return;
  }

  if (!GetHandleData(handle, handlePtr)) {
    cb->error(Error::HYPERSPACE_INVALID_HANDLE, std::string("handle=") + handle);
    return;
  }

  if (!(handlePtr->openFlags & OPEN_FLAG_LOCK)) {
    cb->error(Error::HYPERSPACE_MODE_RESTRICTION, "handle not open for locking");
    return;
  }

  if (!(handlePtr->openFlags & OPEN_FLAG_WRITE)) {
    cb->error(Error::HYPERSPACE_MODE_RESTRICTION, "handle not open for writing");
    return;
  }

  {
    boost::mutex::scoped_lock lock(handlePtr->node->mutex);

    if (handlePtr->node->currentLockMode == LOCK_MODE_EXCLUSIVE) {
      if (tryAcquire)
	cb->response(LOCK_STATUS_BUSY);
      else {
	handlePtr->node->pendingLockRequests.push_back( LockRequest(handle, mode) );
	cb->response(LOCK_STATUS_PENDING);
      }
      return;
    }
    else if (handlePtr->node->currentLockMode == LOCK_MODE_SHARED) {
      if (mode == LOCK_MODE_EXCLUSIVE) {
	if (tryAcquire)
	  cb->response(LOCK_STATUS_BUSY);
	else {
	  handlePtr->node->pendingLockRequests.push_back( LockRequest(handle, mode) );
	  cb->response(LOCK_STATUS_PENDING);
	}
	return;
      }

      assert(mode == LOCK_MODE_SHARED);

      if (!handlePtr->node->pendingLockRequests.empty()) {
	handlePtr->node->pendingLockRequests.push_back( LockRequest(handle, mode) );
	cb->response(LOCK_STATUS_PENDING);
	return;
      }
    }

    // at this point we're OK to acquire the lock

    if (mode == LOCK_MODE_SHARED && !handlePtr->node->sharedLockHandles.empty())
      notify = false;

    handlePtr->node->lockGeneration++;
    if (FileUtils::Fsetxattr(handlePtr->node->fd, "lock.generation", &handlePtr->node->lockGeneration, sizeof(uint64_t), 0) == -1) {
      LOG_VA_ERROR("Problem creating extended attribute 'lock.generation' on file '%s' - %s",
		   handlePtr->node->name.c_str(), strerror(errno));
      exit(1);
    }
    handlePtr->node->currentLockMode = mode;

    LockHandle(handlePtr, mode);

    // deliver notification to handles to this same node
    if (notify) {
      HyperspaceEventPtr eventPtr( new EventLockAcquired(mode) );
      DeliverEventNotifications(handlePtr->node, eventPtr);
    }

    cb->response(LOCK_STATUS_GRANTED, handlePtr->node->lockGeneration);
  }
}

/**
 * Assumes node is locked.
 */
void Master::LockHandle(HandleDataPtr &handlePtr, uint32_t mode) {
  if (mode == LOCK_MODE_SHARED)
    handlePtr->node->sharedLockHandles.insert(handlePtr->id);
  else {
    assert(mode == LOCK_MODE_EXCLUSIVE);
    handlePtr->node->exclusiveLockHandle = handlePtr->id;
  }
  handlePtr->locked = true;
}

/**
 * Assumes node is locked.
 */
void Master::LockHandleWithNotification(HandleDataPtr &handlePtr, uint32_t mode, bool waitForNotify) {

  LockHandle(handlePtr, mode);

  // deliver notification to handles to this same node
  {
    HyperspaceEventPtr eventPtr( new EventLockGranted(mode, handlePtr->node->lockGeneration) );
    DeliverEventNotification(handlePtr, eventPtr, waitForNotify);
  }

}



/**
 * 
 */
void Master::Release(ResponseCallback *cb, uint64_t sessionId, uint64_t handle) {
  SessionDataPtr sessionPtr;
  HandleDataPtr handlePtr;

  if (mVerbose) {
    LOG_VA_INFO("release(session=%lld, handle=%lld)", sessionId, handle);
  }

  if (!GetSession(sessionId, sessionPtr)) {
    cb->error(Error::HYPERSPACE_EXPIRED_SESSION, "");
    return;
  }

  if (!GetHandleData(handle, handlePtr)) {
    cb->error(Error::HYPERSPACE_INVALID_HANDLE, std::string("handle=") + handle);
    return;
  }

  ReleaseLock(handlePtr);

  cb->response_ok();
}


void Master::ReleaseLock(HandleDataPtr &handlePtr, bool waitForNotify) {
  boost::mutex::scoped_lock lock(handlePtr->node->mutex);
  vector<HandleDataPtr> nextLockHandles;
  int nextMode = 0;

  if (handlePtr->locked) {
    if (handlePtr->node->exclusiveLockHandle != 0) {
      assert(handlePtr->id == handlePtr->node->exclusiveLockHandle);
      handlePtr->node->exclusiveLockHandle = 0;
    }
    else {
      unsigned int count = handlePtr->node->sharedLockHandles.erase(handlePtr->id);
      assert(count);
    }
    handlePtr->locked = false;
  }
  else
    return;

  // deliver LOCK_RELEASED notifications if no more locks held on node
  if (handlePtr->node->sharedLockHandles.empty()) {
    LOG_INFO("About to deliver lock released notifications");
    HyperspaceEventPtr eventPtr( new EventLockReleased() );
    DeliverEventNotifications(handlePtr->node, eventPtr, waitForNotify);
    LOG_INFO("Finished delivering lock released notifications");
  }

  handlePtr->node->currentLockMode = 0;

  if (!handlePtr->node->pendingLockRequests.empty()) {
    HandleDataPtr nextHandlePtr;
    const LockRequest &frontLockReq = handlePtr->node->pendingLockRequests.front();
    if (frontLockReq.mode == LOCK_MODE_EXCLUSIVE) {
      nextMode = LOCK_MODE_EXCLUSIVE;
      if (GetHandleData(frontLockReq.handle, nextHandlePtr))
	nextLockHandles.push_back(nextHandlePtr);
      handlePtr->node->pendingLockRequests.pop_front();
    }
    else {
      nextMode = LOCK_MODE_SHARED;
      do {
	const LockRequest &lockreq = handlePtr->node->pendingLockRequests.front();
	if (lockreq.mode != LOCK_MODE_SHARED)
	  break;
	if (GetHandleData(lockreq.handle, nextHandlePtr))
	  nextLockHandles.push_back(nextHandlePtr);
	handlePtr->node->pendingLockRequests.pop_front();
      } while (!handlePtr->node->pendingLockRequests.empty());
    }

    if (!nextLockHandles.empty()) {

      handlePtr->node->lockGeneration++;
      if (FileUtils::Fsetxattr(handlePtr->node->fd, "lock.generation", &handlePtr->node->lockGeneration, sizeof(uint64_t), 0) == -1) {
	LOG_VA_ERROR("Problem creating extended attribute 'lock.generation' on file '%s' - %s",
		     handlePtr->node->name.c_str(), strerror(errno));
	exit(1);
      }

      handlePtr->node->currentLockMode = nextMode;

      for (size_t i=0; i<nextLockHandles.size(); i++) {
	assert(handlePtr->id != nextLockHandles[i]->id);
	LockHandleWithNotification(nextLockHandles[i], nextMode, waitForNotify);
      }

      // deliver notification to handles to this same node
      {
	HyperspaceEventPtr eventPtr( new EventLockAcquired(nextMode) );
	DeliverEventNotifications(handlePtr->node, eventPtr, waitForNotify);
      }

    }

  }
  
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
  else if (errno == ENOATTR)
    cb->error(Error::HYPERSPACE_ATTR_NOT_FOUND, errbuf);
  else if (errno = EISDIR)
    cb->error(Error::HYPERSPACE_IS_DIRECTORY, errbuf);
  else {
    LOG_VA_ERROR("Unknown error, errno = %d", errno);
    cb->error(Error::HYPERSPACE_IO_ERROR, errbuf);
  }
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


/**
 * Assumes node is locked.
 */
void Master::DeliverEventNotifications(NodeData *node, HyperspaceEventPtr &eventPtr, bool waitForNotify) {
  int notifications = 0;

  // log event

  for (HandleMapT::iterator iter = node->handleMap.begin(); iter != node->handleMap.end(); iter++) {
    //LOG_VA_INFO("Delivering notification (%d == %d)", (*iter).second->eventMask, eventPtr->GetMask());
    if ((*iter).second->eventMask & eventPtr->GetMask()) {
      (*iter).second->sessionPtr->AddNotification( new Notification((*iter).first, eventPtr) );
      mKeepaliveHandlerPtr->DeliverEventNotifications((*iter).second->sessionPtr->id);
      notifications++;
    }
  }

  if (waitForNotify && notifications)
    eventPtr->WaitForNotifications();
}


/**
 * Assumes node is locked.
 */
void Master::DeliverEventNotification(HandleDataPtr &handlePtr, HyperspaceEventPtr &eventPtr, bool waitForNotify) {
  int notifications = 0;

  // log event

  handlePtr->sessionPtr->AddNotification( new Notification(handlePtr->id, eventPtr) );
  mKeepaliveHandlerPtr->DeliverEventNotifications(handlePtr->sessionPtr->id);

  if (waitForNotify)
    eventPtr->WaitForNotifications();

}



/**
 *
 */
bool Master::FindParentNode(std::string normalName, NodeDataPtr &parentNodePtr, std::string &childName) {
  NodeMapT::iterator nodeIter;
  unsigned int lastSlash = normalName.rfind("/", normalName.length());
  std::string parentName;

  if (lastSlash > 0) {
    boost::mutex::scoped_lock lock(mNodeMapMutex);
    parentName = normalName.substr(0, lastSlash);
    childName = normalName.substr(lastSlash+1);
    nodeIter = mNodeMap.find(parentName);
    if (nodeIter != mNodeMap.end())
      parentNodePtr = (*nodeIter).second;
    else {
      parentNodePtr = new NodeData();
      parentNodePtr->name = parentName;
      mNodeMap[parentName] = parentNodePtr;
    }
    return true;
  }
  else if (lastSlash == 0) {
    boost::mutex::scoped_lock lock(mNodeMapMutex);
    parentName = "/";
    nodeIter = mNodeMap.find(parentName);
    assert (nodeIter != mNodeMap.end());
    childName = normalName.substr(1);
    parentNodePtr = (*nodeIter).second;
    return true;
  }

  return false;
}


/**
 *
 */
bool Master::DestroyHandle(uint64_t handle, int *errorp, std::string &errMsg, bool waitForNotify) {
  HandleDataPtr handlePtr;
  unsigned int refCount = 0;

  if (!RemoveHandleData(handle, handlePtr)) {
    *errorp = Error::HYPERSPACE_INVALID_HANDLE;
    errMsg = std::string("handle=") + handle;
    return false;
  }

  {
    boost::mutex::scoped_lock lock(handlePtr->node->mutex);

    handlePtr->node->RemoveHandle(handlePtr->id);

    refCount = handlePtr->node->ReferenceCount();
  }

  ReleaseLock(handlePtr, waitForNotify);

  if (refCount == 0) {

    handlePtr->node->Close();

    if (handlePtr->node->ephemeral) {
      std::string childName;
      NodeDataPtr parentNodePtr;

      if (FindParentNode(handlePtr->node->name, parentNodePtr, childName)) {
	HyperspaceEventPtr eventPtr( new EventNamed(EVENT_MASK_CHILD_NODE_REMOVED, childName) );
	DeliverEventNotifications(parentNodePtr.get(), eventPtr, waitForNotify);
      }

      // remove node
      NodeMapT::iterator nodeIter = mNodeMap.find(handlePtr->node->name);
      if (nodeIter != mNodeMap.end())
	mNodeMap.erase(nodeIter);
    }
  }

  return true;
}
