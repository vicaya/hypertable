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

using namespace Hypertable;
using namespace Hyperspace;
using namespace std;

const uint32_t Master::DEFAULT_MASTER_PORT;
const uint32_t Master::DEFAULT_LEASE_INTERVAL;
const uint32_t Master::DEFAULT_KEEPALIVE_INTERVAL;

/**
 * This constructor reads the properties file and pulls out some relevant properties.  It
 * also sets up the m_base_dir variable to be the absolute path of the root of the
 * Hyperspace directory (with no trailing slash).  It locks this root directory to
 * prevent concurrent masters and then reads/increments/writes the 32-bit integer extended
 * attribute 'generation'.  It also creates the server Keepalive handler.
 */
Master::Master(ConnectionManagerPtr &connManagerPtr, PropertiesPtr &propsPtr, ServerKeepaliveHandlerPtr &keepaliveHandlerPtr) : m_verbose(false), m_next_handle_number(1), m_next_session_id(1) {
  const char *dirname;
  std::string str;
  ssize_t xattrLen;
  uint16_t port;

  m_verbose = propsPtr->get_bool("Hypertable.Verbose", false);

  m_lease_interval = (uint32_t)propsPtr->get_int("Hyperspace.Lease.Interval", DEFAULT_LEASE_INTERVAL);

  m_keep_alive_interval = (uint32_t)propsPtr->get_int("Hyperspace.KeepAlive.Interval", DEFAULT_KEEPALIVE_INTERVAL);

  if ((dirname = propsPtr->get("Hyperspace.Master.Dir", 0)) == 0) {
    HT_ERROR("Property 'Hyperspace.Master.Dir' not found.");
    exit(1);
  }

  if (dirname[strlen(dirname)-1] == '/')
    str = std::string(dirname, strlen(dirname)-1);
  else
    str = dirname;

  if (dirname[0] == '/')
    m_base_dir = str;
  else
    m_base_dir = System::installDir + "/" + str;

  if ((m_base_fd = ::open(m_base_dir.c_str(), O_RDONLY)) < 0) {
    HT_WARNF("Unable to open base directory '%s' - will create.", m_base_dir.c_str(), strerror(errno));
    if (!FileUtils::mkdirs(m_base_dir.c_str())) {
      HT_ERRORF("Unable to create base directory %s - %s", m_base_dir.c_str(), strerror(errno));
      exit(1);
    }
    if ((m_base_fd = ::open(m_base_dir.c_str(), O_RDONLY)) < 0) {
      HT_ERRORF("Unable to open base directory %s - %s", m_base_dir.c_str(), strerror(errno));
      exit(1);
    }
  }

  /**
   * Lock the base directory to prevent concurrent masters
   */
  if (flock(m_base_fd, LOCK_EX | LOCK_NB) != 0) {
    if (errno == EWOULDBLOCK) {
      HT_ERRORF("Base directory '%s' is locked by another process.", m_base_dir.c_str());
    }
    else {
      HT_ERRORF("Unable to lock base directory '%s' - %s", m_base_dir.c_str(), strerror(errno));
    }
    exit(1);
  }

  /**
   * Increment generation number and save
   */
  if ((xattrLen = FileUtils::getxattr(m_base_dir.c_str(), "generation", &m_generation, sizeof(uint32_t))) < 0) {
    if (errno == ENOATTR) {
      cerr << "'generation' attribute not found on base dir, creating ..." << endl;
      m_generation = 1;
      if (FileUtils::setxattr(m_base_dir.c_str(), "generation", &m_generation, sizeof(uint32_t), XATTR_CREATE) == -1) {
	HT_ERRORF("Problem creating extended attribute 'generation' on base dir '%s' - %s", m_base_dir.c_str(), strerror(errno));
	exit(1);
      }
    }
    else {
      HT_ERRORF("Unable to read extended attribute 'generation' on base dir '%s' - %s", m_base_dir.c_str(), strerror(errno));
      exit(1);
    }
  }
  else {
    m_generation++;
    if (FileUtils::setxattr(m_base_dir.c_str(), "generation", &m_generation, sizeof(uint32_t), XATTR_REPLACE) == -1) {
      HT_ERRORF("Problem creating extended attribute 'generation' on base dir '%s' - %s", m_base_dir.c_str(), strerror(errno));
      exit(1);
    }
  }

  NodeDataPtr rootNodePtr = new NodeData();
  rootNodePtr->name = "/";
  m_node_map["/"] = rootNodePtr;

  port = propsPtr->get_int("Hyperspace.Master.Port", DEFAULT_MASTER_PORT);
  InetAddr::initialize(&m_local_addr, INADDR_ANY, port);

  m_keepalive_handler_ptr.reset( new ServerKeepaliveHandler(connManagerPtr->get_comm(), this) );
  keepaliveHandlerPtr = m_keepalive_handler_ptr;

  if (m_verbose) {
    cout << "Hyperspace.Lease.Interval=" << m_lease_interval << endl;
    cout << "Hyperspace.KeepAlive.Interval=" << m_keep_alive_interval << endl;
    cout << "Hyperspace.Master.Dir=" << m_base_dir << endl;
    cout << "Generation=" << m_generation << endl;
  }

}


Master::~Master() {
  ::close(m_base_fd);
  return;
}


uint64_t Master::create_session(struct sockaddr_in &addr) {
  boost::mutex::scoped_lock lock(m_session_map_mutex);
  SessionDataPtr sessionPtr;
  uint64_t sessionId = m_next_session_id++;
  sessionPtr = new SessionData(addr, m_lease_interval, sessionId);
  m_session_map[sessionId] = sessionPtr;
  m_session_heap.push_back(sessionPtr);
  return sessionId;
}


bool Master::get_session(uint64_t sessionId, SessionDataPtr &sessionPtr) {
  boost::mutex::scoped_lock lock(m_session_map_mutex);
  SessionMapT::iterator iter = m_session_map.find(sessionId);
  if (iter == m_session_map.end())
    return false;
  sessionPtr = (*iter).second;
  return true;
}

void Master::destroy_session(uint64_t sessionId) {
  boost::mutex::scoped_lock lock(m_session_map_mutex);
  SessionDataPtr sessionPtr;
  SessionMapT::iterator iter = m_session_map.find(sessionId);
  if (iter == m_session_map.end())
    return;
  sessionPtr = (*iter).second;
  m_session_map.erase(sessionId);
  sessionPtr->expire();
  // force it to top of expiration heap
  boost::xtime_get(&sessionPtr->expireTime, boost::TIME_UTC);
}


int Master::renew_session_lease(uint64_t sessionId) {
  boost::mutex::scoped_lock lock(m_session_map_mutex);  

  SessionMapT::iterator iter = m_session_map.find(sessionId);
  if (iter == m_session_map.end())
    return Error::HYPERSPACE_EXPIRED_SESSION;

  if (!(*iter).second->renew_lease())
    return Error::HYPERSPACE_EXPIRED_SESSION;

  return Error::OK;
}

bool Master::next_expired_session(SessionDataPtr &sessionPtr) {
  boost::mutex::scoped_lock lock(m_session_map_mutex);
  struct ltSessionData compFunc;
  boost::xtime now;

  boost::xtime_get(&now, boost::TIME_UTC);

  if (m_session_heap.size() > 0) {
    std::make_heap(m_session_heap.begin(), m_session_heap.end(), compFunc);
    if (m_session_heap[0]->is_expired(now)) {
      sessionPtr = m_session_heap[0];
      std::pop_heap(m_session_heap.begin(), m_session_heap.end(), compFunc);
      m_session_heap.resize(m_session_heap.size()-1);
      m_session_map.erase(sessionPtr->id);
      return true;
    }    
  }
  return false;
}


/**
 * 
 */
void Master::remove_expired_sessions() {
  SessionDataPtr sessionPtr;
  int error;
  std::string errMsg;
  
  while (next_expired_session(sessionPtr)) {

    if (m_verbose) {
      HT_INFOF("Expiring session %lld", sessionPtr->id);
    }

    sessionPtr->expire();

    {
      boost::mutex::scoped_lock slock(sessionPtr->mutex);
      HandleDataPtr handlePtr;
      for (set<uint64_t>::iterator iter = sessionPtr->handles.begin(); iter != sessionPtr->handles.end(); iter++) {
	if (m_verbose) {
	  HT_INFOF("Destroying handle %lld", *iter);
	}
	if (!destroy_handle(*iter, &error, errMsg, false)) {
	  HT_ERRORF("Problem destroying handle - %s (%s)", Error::get_text(error), errMsg.c_str());
	}
      }
      sessionPtr->handles.clear();
    }
  }

}


/**
 *
 */
void Master::create_handle(uint64_t *handlep, HandleDataPtr &handlePtr) {
  boost::mutex::scoped_lock lock(m_handle_map_mutex);
  *handlep = ++m_next_handle_number;
  handlePtr = new HandleData();
  handlePtr->id = *handlep;
  handlePtr->locked = false;
  m_handle_map[*handlep] = handlePtr;
}

/**
 *
 */
bool Master::get_handle_data(uint64_t handle, HandleDataPtr &handlePtr) {
  boost::mutex::scoped_lock lock(m_handle_map_mutex);
  HandleMapT::iterator iter = m_handle_map.find(handle);
  if (iter == m_handle_map.end())
    return false;
  handlePtr = (*iter).second;
  return true;
}

/**
 *
 */
bool Master::remove_handle_data(uint64_t handle, HandleDataPtr &handlePtr) {
  boost::mutex::scoped_lock lock(m_handle_map_mutex);
  HandleMapT::iterator iter = m_handle_map.find(handle);
  if (iter == m_handle_map.end())
    return false;
  handlePtr = (*iter).second;
  m_handle_map.erase(iter);
  return true;
}



/**
 * This method creates a directory with absolute path 'name'.  It locks the parent
 * node to prevent concurrent modifications.
 */
void Master::mkdir(ResponseCallback *cb, uint64_t sessionId, const char *name) {
  std::string absName;
  NodeDataPtr parentNodePtr;
  std::string childName;

  if (m_verbose) {
    HT_INFOF("mkdir(sessionId=%lld, name=%s)", sessionId, name);
  }

  if (!find_parent_node(name, parentNodePtr, childName)) {
    cb->error(Error::HYPERSPACE_FILE_EXISTS, "directory '/' exists");
    return;
  }

  assert(name[0] == '/' && name[strlen(name)-1] != '/');

  {
    boost::mutex::scoped_lock nodeLock(parentNodePtr->mutex);

    absName = m_base_dir + name;
    
    if (::mkdir(absName.c_str(), 0755) < 0) {
      report_error(cb);
      return;
    }
    else {
      HyperspaceEventPtr eventPtr( new EventNamed(EVENT_MASK_CHILD_NODE_ADDED, childName) );
      deliver_event_notifications(parentNodePtr.get(), eventPtr);
    }
  }

  cb->response_ok();
}



/**
 * Delete
 */
void Master::unlink(ResponseCallback *cb, uint64_t sessionId, const char *name) {
  std::string absName;
  struct stat statbuf;
  std::string childName;
  NodeDataPtr parentNodePtr;

  if (m_verbose) {
    HT_INFOF("unlink(sessionId=%lld, name=%s)", sessionId, name);
  }

  if (!strcmp(name, "/")) {
    cb->error(Error::HYPERSPACE_PERMISSION_DENIED, "Cannot remove '/' directory");
    return;
  }

  if (!find_parent_node(name, parentNodePtr, childName)) {
    cb->error(Error::HYPERSPACE_BAD_PATHNAME, name);
    return;
  }

  assert(name[0] == '/' && name[strlen(name)-1] != '/');

  {
    boost::mutex::scoped_lock nodeLock(parentNodePtr->mutex);

    absName = m_base_dir + name;

    if (stat(absName.c_str(), &statbuf) < 0) {
      report_error(cb);
      return;
    }
    else {
      if (statbuf.st_mode & S_IFDIR) {
	if (rmdir(absName.c_str()) < 0) {
	  report_error(cb);
	  return;
	}
      }
      else {
	if (::unlink(absName.c_str()) < 0) {
	  report_error(cb);
	  return;
	}
      }
    }

    // deliver event notifications
    {
      HyperspaceEventPtr eventPtr( new EventNamed(EVENT_MASK_CHILD_NODE_REMOVED, childName) );
      deliver_event_notifications(parentNodePtr.get(), eventPtr);
    }

  }

  cb->response_ok();
}



/**
 * Open
 */
void Master::open(ResponseCallbackOpen *cb, uint64_t sessionId, const char *name, uint32_t flags, uint32_t eventMask, std::vector<AttributeT> &initAttrs) {
  std::string absName;
  std::string childName;
  SessionDataPtr sessionPtr;
  NodeDataPtr nodePtr;
  NodeDataPtr parentNodePtr;
  HandleDataPtr handlePtr;
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

  if (m_verbose) {
    HT_INFOF("open(sessionId=%lld, fname=%s, flags=0x%x, eventMask=0x%x)", sessionId, name, flags, eventMask);
    cout << flush;
  }

  assert(name[0] == '/');

  absName = m_base_dir + name;

  if (!get_session(sessionId, sessionPtr)) {
    cb->error(Error::HYPERSPACE_EXPIRED_SESSION, "");
    return;
  }

  if (!find_parent_node(name, parentNodePtr, childName)) {
    cb->error(Error::HYPERSPACE_BAD_PATHNAME, name);
    return;
  }

  // If path name to open is "/", then create a dummy NodeData object for
  // the parent because the previous call will return the node itself as
  // the parent, causing the second of the following two mutex locks to hang
  if (!strcmp(name, "/"))
    parentNodePtr = new NodeData();

  get_node(name, nodePtr);

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
	report_error(cb);
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
      nodePtr->fd = ::open(absName.c_str(), oflags, 0644);
      if (nodePtr->fd < 0) {
	HT_ERRORF("open(%s, 0x%x, 0644) failed (errno=%d) - %s", absName.c_str(), oflags, errno, strerror(errno));
	report_error(cb);
	return;
      }

      // Read/create 'lock.generation' attribute

      len = FileUtils::fgetxattr(nodePtr->fd, "lock.generation", &nodePtr->lockGeneration, sizeof(uint64_t));
      if (len < 0) {
	if (errno == ENOATTR) {
	  nodePtr->lockGeneration = 1;
	  if (FileUtils::fsetxattr(nodePtr->fd, "lock.generation", &nodePtr->lockGeneration, sizeof(uint64_t), 0) == -1) {
	    HT_ERRORF("Problem creating extended attribute 'lock.generation' on file '%s' - %s",
			 name, strerror(errno));
	    DUMP_CORE;
	  }
	}
	else {
	  HT_ERRORF("Problem reading extended attribute 'lock.generation' on file '%s' - %s",
		       name, strerror(errno));
	  DUMP_CORE;
	}
	len = sizeof(int64_t);
      }
      assert(len == sizeof(int64_t));

      /**
       * Set the initial attributes
       */
      for (size_t i=0; i<initAttrs.size(); i++) {
	if (FileUtils::fsetxattr(nodePtr->fd, initAttrs[i].name, initAttrs[i].value, initAttrs[i].valueLen, 0) == -1) {
	  HT_ERRORF("Problem creating extended attribute '%s' on file '%s' - %s",
		       initAttrs[i].name, name, strerror(errno));
	  DUMP_CORE;
	}
      }

      if (flags & OPEN_FLAG_TEMP) {
	nodePtr->ephemeral = true;
	::unlink(absName.c_str());
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

    create_handle(&handle, handlePtr);

    handlePtr->node = nodePtr.get();
    handlePtr->openFlags = flags;
    handlePtr->eventMask = eventMask;
    handlePtr->sessionPtr = sessionPtr;

    sessionPtr->add_handle(handle);

    if (created) {
      HyperspaceEventPtr eventPtr( new EventNamed(EVENT_MASK_CHILD_NODE_ADDED, childName) );
      deliver_event_notifications(parentNodePtr.get(), eventPtr);
    }

    /**
     * If open flags LOCK_SHARED or LOCK_EXCLUSIVE, then obtain lock
     */
    if (lockMode != 0) {
      handlePtr->node->lockGeneration++;
      if (FileUtils::fsetxattr(handlePtr->node->fd, "lock.generation", &handlePtr->node->lockGeneration, sizeof(uint64_t), 0) == -1) {
	HT_ERRORF("Problem creating extended attribute 'lock.generation' on file '%s' - %s",
		     handlePtr->node->name.c_str(), strerror(errno));
	exit(1);
      }
      lockGeneration = handlePtr->node->lockGeneration;
      handlePtr->node->currentLockMode = lockMode;

      lock_handle(handlePtr, lockMode);

      // deliver notification to handles to this same node
      if (lockNotify) {
	HyperspaceEventPtr eventPtr( new EventLockAcquired(lockMode) );
	deliver_event_notifications(handlePtr->node, eventPtr);
      }
    }

    handlePtr->node->add_handle(handle, handlePtr);

  }

  cb->response(handle, created, lockGeneration);
}


/**
 *
 */
void Master::close(ResponseCallback *cb, uint64_t sessionId, uint64_t handle) {
  SessionDataPtr sessionPtr;
  int error;
  std::string errMsg;

  if (m_verbose) {
    HT_INFOF("close(session=%lld, handle=%lld)", sessionId, handle);
  }

  if (!get_session(sessionId, sessionPtr)) {
    cb->error(Error::HYPERSPACE_EXPIRED_SESSION, "");
    return;
  }

  {
    boost::mutex::scoped_lock slock(sessionPtr->mutex);
    size_t n = sessionPtr->handles.erase(handle);
    if (n) {
      if (!destroy_handle(handle, &error, errMsg)) {
	cb->error(error, errMsg);
	return;
      }
    }
  }

  if ((error = cb->response_ok()) != Error::OK) {
    HT_ERRORF("Problem sending back response - %s", Error::get_text(error));
  }
}



/**
 *
 */
void Master::attr_set(ResponseCallback *cb, uint64_t sessionId, uint64_t handle, const char *name, const void *value, size_t valueLen) {
  SessionDataPtr sessionPtr;
  HandleDataPtr handlePtr;
  int error;

  if (m_verbose) {
    HT_INFOF("attrset(session=%lld, handle=%lld, name=%s, valueLen=%d)", sessionId, handle, name, valueLen);
  }

  if (!get_session(sessionId, sessionPtr)) {
    cb->error(Error::HYPERSPACE_EXPIRED_SESSION, "");
    return;
  }

  if (!get_handle_data(handle, handlePtr)) {
    cb->error(Error::HYPERSPACE_INVALID_HANDLE, std::string("handle=") + handle);
    return;
  }

  {
    boost::mutex::scoped_lock nodeLock(handlePtr->node->mutex);

    if (FileUtils::fsetxattr(handlePtr->node->fd, name, value, valueLen, 0) == -1) {
      HT_ERRORF("Problem creating extended attribute '%s' on file '%s' - %s",
		   name, handlePtr->node->name.c_str(), strerror(errno));
      DUMP_CORE;
    }

    HyperspaceEventPtr eventPtr( new EventNamed(EVENT_MASK_ATTR_SET, name) );
    deliver_event_notifications(handlePtr->node, eventPtr);
  }

  if ((error = cb->response_ok()) != Error::OK) {
    HT_ERRORF("Problem sending back response - %s", Error::get_text(error));
  }

}


/**
 * 
 */
void Master::attr_get(ResponseCallbackAttrGet *cb, uint64_t sessionId, uint64_t handle, const char *name) {
  SessionDataPtr sessionPtr;
  HandleDataPtr handlePtr;
  int error;
  int alen;
  uint8_t *buf = 0;

  if (m_verbose) {
    HT_INFOF("attrget(session=%lld, handle=%lld, name=%s)", sessionId, handle, name);
  }

  if (!get_session(sessionId, sessionPtr)) {
    cb->error(Error::HYPERSPACE_EXPIRED_SESSION, "");
    return;
  }

  if (!get_handle_data(handle, handlePtr)) {
    cb->error(Error::HYPERSPACE_INVALID_HANDLE, std::string("handle=") + handle);
    return;
  }

  {
    boost::mutex::scoped_lock nodeLock(handlePtr->node->mutex);

    if ((alen = FileUtils::fgetxattr(handlePtr->node->fd, name, 0, 0)) < 0) {
      HT_ERRORF("Problem determining size of extended attribute '%s' on file '%s' - %s",
		   name, handlePtr->node->name.c_str(), strerror(errno));
      report_error(cb);
      return;
    }

    buf = new uint8_t [ alen + 8 ];

    if ((alen = FileUtils::fgetxattr(handlePtr->node->fd, name, buf, alen)) < 0) {
      HT_ERRORF("Problem determining size of extended attribute '%s' on file '%s' - %s",
		   name, handlePtr->node->name.c_str(), strerror(errno));
      DUMP_CORE;
    }
  }

  if ((error = cb->response(buf, (uint32_t)alen)) != Error::OK) {
    HT_ERRORF("Problem sending back response - %s", Error::get_text(error));
  }
  
}



void Master::attr_del(ResponseCallback *cb, uint64_t sessionId, uint64_t handle, const char *name) {
  SessionDataPtr sessionPtr;
  HandleDataPtr handlePtr;
  int error;

  if (m_verbose) {
    HT_INFOF("attrdel(session=%lld, handle=%lld, name=%s)", sessionId, handle, name);
  }

  if (!get_session(sessionId, sessionPtr)) {
    cb->error(Error::HYPERSPACE_EXPIRED_SESSION, "");
    return;
  }

  if (!get_handle_data(handle, handlePtr)) {
    cb->error(Error::HYPERSPACE_INVALID_HANDLE, std::string("handle=") + handle);
    return;
  }

  {
    boost::mutex::scoped_lock nodeLock(handlePtr->node->mutex);

    if (FileUtils::fremovexattr(handlePtr->node->fd, name) == -1) {
      HT_ERRORF("Problem removing extended attribute '%s' on file '%s' - %s",
		   name, handlePtr->node->name.c_str(), strerror(errno));
      report_error(cb);
      return;
    }

    HyperspaceEventPtr eventPtr( new EventNamed(EVENT_MASK_ATTR_DEL, name) );
    deliver_event_notifications(handlePtr->node, eventPtr);
  }

  if ((error = cb->response_ok()) != Error::OK) {
    HT_ERRORF("Problem sending back response - %s", Error::get_text(error));
  }

}


void Master::exists(ResponseCallbackExists *cb, uint64_t sessionId, const char *name) {
  std::string absName;
  int error;

  if (m_verbose) {
    HT_INFOF("exists(sessionId=%lld, name=%s)", sessionId, name);
  }

  assert(name[0] == '/' && name[strlen(name)-1] != '/');

  absName = m_base_dir + name;

  if ((error = cb->response( FileUtils::exists(absName.c_str()) )) != Error::OK) {
    HT_ERRORF("Problem sending back response - %s", Error::get_text(error));
  }
}


void Master::readdir(ResponseCallbackReaddir *cb, uint64_t sessionId, uint64_t handle) {
  std::string absName;
  SessionDataPtr sessionPtr;
  HandleDataPtr handlePtr;
  struct DirEntryT dentry;
  std::vector<struct DirEntryT> listing;

  if (m_verbose) {
    HT_INFOF("readdir(session=%lld, handle=%lld)", sessionId, handle);
  }

  if (!get_session(sessionId, sessionPtr)) {
    cb->error(Error::HYPERSPACE_EXPIRED_SESSION, "");
    return;
  }

  if (!get_handle_data(handle, handlePtr)) {
    cb->error(Error::HYPERSPACE_INVALID_HANDLE, std::string("handle=") + handle);
    return;
  }

  {
    boost::mutex::scoped_lock lock(handlePtr->node->mutex);

    absName = m_base_dir + handlePtr->node->name;

    DIR *dirp = opendir(absName.c_str());

    if (dirp == 0) {
      report_error(cb);
      HT_ERRORF("opendir('%s') failed - %s", absName.c_str(), strerror(errno));
      return;
    }

    struct dirent dent;
    struct dirent *dp;

    if (readdir_r(dirp, &dent, &dp) != 0) {
      report_error(cb);
      HT_ERRORF("readdir('%s') failed - %s", absName.c_str(), strerror(errno));
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
	report_error(cb);
	HT_ERRORF("readdir('%s') failed - %s", absName.c_str(), strerror(errno));
	(void)closedir(dirp);
	return;
      }
    }
    (void)closedir(dirp);

  }

  cb->response(listing);
}



void Master::lock(ResponseCallbackLock *cb, uint64_t sessionId, uint64_t handle, uint32_t mode, bool tryAcquire) {
  SessionDataPtr sessionPtr;
  HandleDataPtr handlePtr;
  bool notify = true;
  
  if (m_verbose) {
    HT_INFOF("lock(session=%lld, handle=%lld, mode=0x%x, tryAcquire=%d)", sessionId, handle, mode, tryAcquire);
  }

  if (!get_session(sessionId, sessionPtr)) {
    cb->error(Error::HYPERSPACE_EXPIRED_SESSION, "");
    return;
  }

  if (!get_handle_data(handle, handlePtr)) {
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
    if (FileUtils::fsetxattr(handlePtr->node->fd, "lock.generation", &handlePtr->node->lockGeneration, sizeof(uint64_t), 0) == -1) {
      HT_ERRORF("Problem creating extended attribute 'lock.generation' on file '%s' - %s",
		   handlePtr->node->name.c_str(), strerror(errno));
      exit(1);
    }
    handlePtr->node->currentLockMode = mode;

    lock_handle(handlePtr, mode);

    // deliver notification to handles to this same node
    if (notify) {
      HyperspaceEventPtr eventPtr( new EventLockAcquired(mode) );
      deliver_event_notifications(handlePtr->node, eventPtr);
    }

    cb->response(LOCK_STATUS_GRANTED, handlePtr->node->lockGeneration);
  }
}

/**
 * Assumes node is locked.
 */
void Master::lock_handle(HandleDataPtr &handlePtr, uint32_t mode) {
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
void Master::lock_handle_with_notification(HandleDataPtr &handlePtr, uint32_t mode, bool waitForNotify) {

  lock_handle(handlePtr, mode);

  // deliver notification to handles to this same node
  {
    HyperspaceEventPtr eventPtr( new EventLockGranted(mode, handlePtr->node->lockGeneration) );
    deliver_event_notification(handlePtr, eventPtr, waitForNotify);
  }

}



/**
 * 
 */
void Master::release(ResponseCallback *cb, uint64_t sessionId, uint64_t handle) {
  SessionDataPtr sessionPtr;
  HandleDataPtr handlePtr;

  if (m_verbose) {
    HT_INFOF("release(session=%lld, handle=%lld)", sessionId, handle);
  }

  if (!get_session(sessionId, sessionPtr)) {
    cb->error(Error::HYPERSPACE_EXPIRED_SESSION, "");
    return;
  }

  if (!get_handle_data(handle, handlePtr)) {
    cb->error(Error::HYPERSPACE_INVALID_HANDLE, std::string("handle=") + handle);
    return;
  }

  release_lock(handlePtr);

  cb->response_ok();
}


void Master::release_lock(HandleDataPtr &handlePtr, bool waitForNotify) {
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
      (void)count;
    }
    handlePtr->locked = false;
  }
  else
    return;

  // deliver LOCK_RELEASED notifications if no more locks held on node
  if (handlePtr->node->sharedLockHandles.empty()) {
    HT_INFO("About to deliver lock released notifications");
    HyperspaceEventPtr eventPtr( new EventLockReleased() );
    deliver_event_notifications(handlePtr->node, eventPtr, waitForNotify);
    HT_INFO("Finished delivering lock released notifications");
  }

  handlePtr->node->currentLockMode = 0;

  if (!handlePtr->node->pendingLockRequests.empty()) {
    HandleDataPtr nextHandlePtr;
    const LockRequest &frontLockReq = handlePtr->node->pendingLockRequests.front();
    if (frontLockReq.mode == LOCK_MODE_EXCLUSIVE) {
      nextMode = LOCK_MODE_EXCLUSIVE;
      if (get_handle_data(frontLockReq.handle, nextHandlePtr))
	nextLockHandles.push_back(nextHandlePtr);
      handlePtr->node->pendingLockRequests.pop_front();
    }
    else {
      nextMode = LOCK_MODE_SHARED;
      do {
	const LockRequest &lockreq = handlePtr->node->pendingLockRequests.front();
	if (lockreq.mode != LOCK_MODE_SHARED)
	  break;
	if (get_handle_data(lockreq.handle, nextHandlePtr))
	  nextLockHandles.push_back(nextHandlePtr);
	handlePtr->node->pendingLockRequests.pop_front();
      } while (!handlePtr->node->pendingLockRequests.empty());
    }

    if (!nextLockHandles.empty()) {

      handlePtr->node->lockGeneration++;
      if (FileUtils::fsetxattr(handlePtr->node->fd, "lock.generation", &handlePtr->node->lockGeneration, sizeof(uint64_t), 0) == -1) {
	HT_ERRORF("Problem creating extended attribute 'lock.generation' on file '%s' - %s",
		     handlePtr->node->name.c_str(), strerror(errno));
	exit(1);
      }

      handlePtr->node->currentLockMode = nextMode;

      for (size_t i=0; i<nextLockHandles.size(); i++) {
	assert(handlePtr->id != nextLockHandles[i]->id);
	lock_handle_with_notification(nextLockHandles[i], nextMode, waitForNotify);
      }

      // deliver notification to handles to this same node
      {
	HyperspaceEventPtr eventPtr( new EventLockAcquired(nextMode) );
	deliver_event_notifications(handlePtr->node, eventPtr, waitForNotify);
      }

    }

  }
  
}


/**
 * report_error
 */
void Master::report_error(ResponseCallback *cb) {
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
  else if (errno == EISDIR)
    cb->error(Error::HYPERSPACE_IS_DIRECTORY, errbuf);
  else {
    HT_ERRORF("Unknown error, errno = %d", errno);
    cb->error(Error::HYPERSPACE_IO_ERROR, errbuf);
  }
}



/**
 *
 */
void Master::normalize_name(std::string name, std::string &normal) {
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
void Master::deliver_event_notifications(NodeData *node, HyperspaceEventPtr &eventPtr, bool waitForNotify) {
  int notifications = 0;

  // log event

  for (HandleMapT::iterator iter = node->handleMap.begin(); iter != node->handleMap.end(); iter++) {
    //HT_INFOF("Delivering notification (%d == %d)", (*iter).second->eventMask, eventPtr->get_mask());
    if ((*iter).second->eventMask & eventPtr->get_mask()) {
      (*iter).second->sessionPtr->add_notification( new Notification((*iter).first, eventPtr) );
      m_keepalive_handler_ptr->deliver_event_notifications((*iter).second->sessionPtr->id);
      notifications++;
    }
  }

  if (waitForNotify && notifications)
    eventPtr->wait_for_notifications();
}


/**
 * Assumes node is locked.
 */
void Master::deliver_event_notification(HandleDataPtr &handlePtr, HyperspaceEventPtr &eventPtr, bool waitForNotify) {

  // log event

  handlePtr->sessionPtr->add_notification( new Notification(handlePtr->id, eventPtr) );
  m_keepalive_handler_ptr->deliver_event_notifications(handlePtr->sessionPtr->id);

  if (waitForNotify)
    eventPtr->wait_for_notifications();

}



/**
 *
 */
bool Master::find_parent_node(std::string normalName, NodeDataPtr &parentNodePtr, std::string &childName) {
  NodeMapT::iterator nodeIter;
  unsigned int lastSlash = normalName.rfind("/", normalName.length());
  std::string parentName;

  if (lastSlash > 0) {
    boost::mutex::scoped_lock lock(m_node_map_mutex);
    parentName = normalName.substr(0, lastSlash);
    childName = normalName.substr(lastSlash+1);
    nodeIter = m_node_map.find(parentName);
    if (nodeIter != m_node_map.end())
      parentNodePtr = (*nodeIter).second;
    else {
      parentNodePtr = new NodeData();
      parentNodePtr->name = parentName;
      m_node_map[parentName] = parentNodePtr;
    }
    return true;
  }
  else if (lastSlash == 0) {
    boost::mutex::scoped_lock lock(m_node_map_mutex);
    parentName = "/";
    nodeIter = m_node_map.find(parentName);
    assert (nodeIter != m_node_map.end());
    childName = normalName.substr(1);
    parentNodePtr = (*nodeIter).second;
    return true;
  }

  return false;
}


/**
 *
 */
bool Master::destroy_handle(uint64_t handle, int *errorp, std::string &errMsg, bool waitForNotify) {
  HandleDataPtr handlePtr;
  unsigned int refCount = 0;

  if (!remove_handle_data(handle, handlePtr)) {
    *errorp = Error::HYPERSPACE_INVALID_HANDLE;
    errMsg = std::string("handle=") + handle;
    return false;
  }

  {
    boost::mutex::scoped_lock lock(handlePtr->node->mutex);

    handlePtr->node->remove_handle(handlePtr->id);

    refCount = handlePtr->node->reference_count();
  }

  release_lock(handlePtr, waitForNotify);

  if (refCount == 0) {

    handlePtr->node->close();

    if (handlePtr->node->ephemeral) {
      std::string childName;
      NodeDataPtr parentNodePtr;

      if (find_parent_node(handlePtr->node->name, parentNodePtr, childName)) {
	HyperspaceEventPtr eventPtr( new EventNamed(EVENT_MASK_CHILD_NODE_REMOVED, childName) );
	deliver_event_notifications(parentNodePtr.get(), eventPtr, waitForNotify);
      }

      // remove node
      NodeMapT::iterator nodeIter = m_node_map.find(handlePtr->node->name);
      if (nodeIter != m_node_map.end())
	m_node_map.erase(nodeIter);
    }
  }

  return true;
}
