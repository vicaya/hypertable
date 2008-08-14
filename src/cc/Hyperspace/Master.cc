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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
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

#define HT_BDBTXN_BEGIN \
  do { \
    DbTxn *txn = m_bdb_fs->start_transaction(); \
    try

#define HT_BDBTXN_END_CB(_cb_) \
    catch (Exception &e) { \
      if (e.code() != Error::HYPERSPACE_BERKELEYDB_DEADLOCK) { \
        if (e.code() == Error::HYPERSPACE_BERKELEYDB_ERROR) \
          HT_ERROR_OUT << e << HT_END; \
        else \
          HT_WARNF("%s - %s", Error::get_text(e.code()), e.what()); \
        txn->abort(); \
        _cb_->error(e.code(), e.what()); \
        return; \
      } \
      HT_WARN("Berkeley DB deadlock encountered"); \
      poll(0, 0, (System::rand32() % 3000) + 1); \
      continue; \
    } \
    break; \
  } while (true)

#define HT_BDBTXN_END(...) \
    catch (Exception &e) { \
      if (e.code() != Error::HYPERSPACE_BERKELEYDB_DEADLOCK) { \
        if (e.code() == Error::HYPERSPACE_BERKELEYDB_ERROR) \
          HT_ERROR_OUT << e << HT_END; \
        else \
          HT_WARNF("%s - %s", Error::get_text(e.code()), e.what()); \
        txn->abort(); \
        return __VA_ARGS__; \
      } \
      HT_WARN("Berkeley DB deadlock encountered"); \
      poll(0, 0, (System::rand32() % 3000) + 1); \
      continue; \
    } \
    break; \
  } while (true)


/**
 * Sets up the m_base_dir variable to be the absolute path of the root of the
 * Hyperspace directory (with no trailing slash); Locks this root directory to
 * prevent concurrent masters and then reads/increments/writes the 32-bit
 * integer attribute 'generation'; Creates the server Keepalive handler.
 */
Master::Master(ConnectionManagerPtr &conn_mgr, PropertiesPtr &props,
               ServerKeepaliveHandlerPtr &keepalive_handler)
    : m_verbose(false), m_next_handle_number(1), m_next_session_id(1) {
  const char *dirname;
  std::string str;
  uint16_t port;

  m_verbose = props->get_bool("Hypertable.Verbose", false);

  m_lease_interval = props->get_int("Hyperspace.Lease.Interval",
                                    DEFAULT_LEASE_INTERVAL);

  m_keep_alive_interval = props->get_int("Hyperspace.KeepAlive.Interval",
                                         DEFAULT_KEEPALIVE_INTERVAL);

  if ((dirname = props->get("Hyperspace.Master.Dir", 0)) == 0) {
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
    m_base_dir = System::install_dir + "/" + str;

  if ((m_base_fd = ::open(m_base_dir.c_str(), O_RDONLY)) < 0) {
    HT_WARNF("Unable to open base directory '%s' - will create.",
             m_base_dir.c_str(), strerror(errno));
    if (!FileUtils::mkdirs(m_base_dir.c_str())) {
      HT_ERRORF("Unable to create base directory %s - %s",
                m_base_dir.c_str(), strerror(errno));
      exit(1);
    }
    if ((m_base_fd = ::open(m_base_dir.c_str(), O_RDONLY)) < 0) {
      HT_ERRORF("Unable to open base directory %s - %s",
                m_base_dir.c_str(), strerror(errno));
      exit(1);
    }
  }

  /**
   * Lock the base directory to prevent concurrent masters
   */
  if (flock(m_base_fd, LOCK_EX | LOCK_NB) != 0) {
    if (errno == EWOULDBLOCK) {
      HT_ERRORF("Base directory '%s' is locked by another process.",
                m_base_dir.c_str());
    }
    else {
      HT_ERRORF("Unable to lock base directory '%s' - %s",
                m_base_dir.c_str(), strerror(errno));
    }
    exit(1);
  }

  m_bdb_fs = new BerkeleyDbFilesystem(m_base_dir);

  /**
   * Load and increment generation number
   */
  get_generation_number();

  NodeDataPtr root_node = new NodeData();
  root_node->name = "/";
  m_node_map["/"] = root_node;

  port = props->get_int("Hyperspace.Master.Port", DEFAULT_MASTER_PORT);
  InetAddr::initialize(&m_local_addr, INADDR_ANY, port);

  m_keepalive_handler_ptr.reset(
      new ServerKeepaliveHandler(conn_mgr->get_comm(), this));
  keepalive_handler = m_keepalive_handler_ptr;

  if (m_verbose) {
    cout << "Hyperspace.Lease.Interval=" << m_lease_interval << endl;
    cout << "Hyperspace.KeepAlive.Interval=" << m_keep_alive_interval << endl;
    cout << "Hyperspace.Master.Dir=" << m_base_dir << endl;
    cout << "Generation=" << m_generation << endl;
  }
}


Master::~Master() {

  delete m_bdb_fs;

  ::close(m_base_fd);

  return;
}


uint64_t Master::create_session(struct sockaddr_in &addr) {
  boost::mutex::scoped_lock lock(m_session_map_mutex);
  SessionDataPtr session_data;
  uint64_t session_id = m_next_session_id++;
  session_data = new SessionData(addr, m_lease_interval, session_id);
  m_session_map[session_id] = session_data;
  m_session_heap.push_back(session_data);
  return session_id;
}


bool Master::get_session(uint64_t session_id, SessionDataPtr &session_data) {
  boost::mutex::scoped_lock lock(m_session_map_mutex);
  SessionMap::iterator iter = m_session_map.find(session_id);
  if (iter == m_session_map.end())
    return false;
  session_data = (*iter).second;
  return true;
}

void Master::destroy_session(uint64_t session_id) {
  boost::mutex::scoped_lock lock(m_session_map_mutex);
  SessionDataPtr session_data;
  SessionMap::iterator iter = m_session_map.find(session_id);
  if (iter == m_session_map.end())
    return;
  session_data = (*iter).second;
  m_session_map.erase(session_id);
  session_data->expire();
  // force it to top of expiration heap
  boost::xtime_get(&session_data->expire_time, boost::TIME_UTC);
}


int Master::renew_session_lease(uint64_t session_id) {
  boost::mutex::scoped_lock lock(m_session_map_mutex);

  SessionMap::iterator iter = m_session_map.find(session_id);
  if (iter == m_session_map.end())
    return Error::HYPERSPACE_EXPIRED_SESSION;

  if (!(*iter).second->renew_lease())
    return Error::HYPERSPACE_EXPIRED_SESSION;

  return Error::OK;
}

bool Master::next_expired_session(SessionDataPtr &session_data) {
  boost::mutex::scoped_lock lock(m_session_map_mutex);
  struct LtSessionData ascending;
  boost::xtime now;

  boost::xtime_get(&now, boost::TIME_UTC);

  if (m_session_heap.size() > 0) {
    std::make_heap(m_session_heap.begin(), m_session_heap.end(), ascending);
    if (m_session_heap[0]->is_expired(now)) {
      session_data = m_session_heap[0];
      std::pop_heap(m_session_heap.begin(), m_session_heap.end(), ascending);
      m_session_heap.resize(m_session_heap.size()-1);
      m_session_map.erase(session_data->id);
      return true;
    }
  }
  return false;
}


/**
 *
 */
void Master::remove_expired_sessions() {
  SessionDataPtr session_data;
  int error;
  std::string errmsg;

  while (next_expired_session(session_data)) {

    if (m_verbose) {
      HT_INFOF("Expiring session %lld", session_data->id);
    }

    session_data->expire();

    {
      boost::mutex::scoped_lock slock(session_data->mutex);
      HandleDataPtr handle_data;
      foreach(uint64_t handle, session_data->handles) {
        if (m_verbose) {
          HT_INFOF("Destroying handle %llu", (Llu)handle);
        }
        if (!destroy_handle(handle, &error, errmsg, false)) {
          HT_ERRORF("Problem destroying handle - %s (%s)",
                    Error::get_text(error), errmsg.c_str());
        }
      }
      session_data->handles.clear();
    }
  }

}


/**
 *
 */
void Master::create_handle(uint64_t *handlep, HandleDataPtr &handle_data) {
  boost::mutex::scoped_lock lock(m_handle_map_mutex);
  *handlep = ++m_next_handle_number;
  handle_data = new HandleData();
  handle_data->id = *handlep;
  handle_data->locked = false;
  m_handle_map[*handlep] = handle_data;
}

/**
 *
 */
bool Master::get_handle_data(uint64_t handle, HandleDataPtr &handle_data) {
  boost::mutex::scoped_lock lock(m_handle_map_mutex);
  HandleMap::iterator iter = m_handle_map.find(handle);
  if (iter == m_handle_map.end())
    return false;
  handle_data = (*iter).second;
  return true;
}

/**
 *
 */
bool Master::remove_handle_data(uint64_t handle, HandleDataPtr &handle_data) {
  boost::mutex::scoped_lock lock(m_handle_map_mutex);
  HandleMap::iterator iter = m_handle_map.find(handle);
  if (iter == m_handle_map.end())
    return false;
  handle_data = (*iter).second;
  m_handle_map.erase(iter);
  return true;
}



/**
 * Creates a directory with absolute path 'name'.  It locks the parent
 * node to prevent concurrent modifications.
 */
void
Master::mkdir(ResponseCallback *cb, uint64_t session_id, const char *name) {
  std::string abs_name;
  NodeDataPtr parent_node;
  std::string child_name;

  if (m_verbose) {
    HT_INFOF("mkdir(session_id=%lld, name=%s)", session_id, name);
  }

  if (!find_parent_node(name, parent_node, child_name)) {
    cb->error(Error::HYPERSPACE_FILE_EXISTS, "directory '/' exists");
    return;
  }

  assert(name[0] == '/' && name[strlen(name)-1] != '/');

  HT_BDBTXN_BEGIN {
    boost::mutex::scoped_lock node_lock(parent_node->mutex);

    m_bdb_fs->mkdir(txn, name);

    txn->commit(0);

    // deliver event notifications
    HyperspaceEventPtr event(new EventNamed(EVENT_MASK_CHILD_NODE_ADDED,
                                            child_name));
    deliver_event_notifications(parent_node.get(), event);
  }
  HT_BDBTXN_END_CB(cb);

  cb->response_ok();
}



/**
 * Delete
 */
void
Master::unlink(ResponseCallback *cb, uint64_t session_id, const char *name) {
  std::string child_name;
  NodeDataPtr parent_node;

  if (m_verbose) {
    HT_INFOF("unlink(session_id=%lld, name=%s)", session_id, name);
  }

  if (!strcmp(name, "/")) {
    cb->error(Error::HYPERSPACE_PERMISSION_DENIED,
              "Cannot remove '/' directory");
    return;
  }

  if (!find_parent_node(name, parent_node, child_name)) {
    cb->error(Error::HYPERSPACE_BAD_PATHNAME, name);
    return;
  }

  assert(name[0] == '/' && name[strlen(name)-1] != '/');

  HT_BDBTXN_BEGIN {
    boost::mutex::scoped_lock node_lock(parent_node->mutex);

    m_bdb_fs->unlink(txn, name);

    txn->commit(0);

    // deliver event notifications
    HyperspaceEventPtr event_ptr(new EventNamed(EVENT_MASK_CHILD_NODE_REMOVED,
                                                child_name));
    deliver_event_notifications(parent_node.get(), event_ptr);
  }
  HT_BDBTXN_END_CB(cb);

  cb->response_ok();
}



/**
 * Open
 */
void
Master::open(ResponseCallbackOpen *cb, uint64_t session_id, const char *name,
    uint32_t flags, uint32_t event_mask, std::vector<Attribute> &init_attrs) {
  std::string child_name;
  SessionDataPtr session_data;
  NodeDataPtr node_data;
  NodeDataPtr parent_node;
  HandleDataPtr handle_data;
  bool created = false;
  bool is_dir = false;
  bool existed;
  uint64_t handle;
  bool lock_notifiy = false;
  uint32_t lock_mode = 0;
  uint64_t lock_generation = 0;

  if (m_verbose) {
    HT_INFOF("open(session_id=%lld, fname=%s, flags=0x%x, event_mask=0x%x)",
             session_id, name, flags, event_mask);
  }

  assert(name[0] == '/');

  HT_BDBTXN_BEGIN {
    if (!get_session(session_id, session_data))
      HT_THROWF(Error::HYPERSPACE_EXPIRED_SESSION, "%llu", (Llu)session_id);

    if (!find_parent_node(name, parent_node, child_name))
      HT_THROW(Error::HYPERSPACE_BAD_PATHNAME, name);

    if (!init_attrs.empty() && !(flags & OPEN_FLAG_CREATE))
      HT_THROW(Error::HYPERSPACE_CREATE_FAILED,
               "initial attributes can only be supplied on CREATE");

    // If path name to open is "/", then create a dummy NodeData object for
    // the parent because the previous call will return the node itself as
    // the parent, causing the second of the following two mutex locks to hang
    if (!strcmp(name, "/"))
      parent_node = new NodeData();

    get_node(name, node_data);

    {
      boost::mutex::scoped_lock parent_lock(parent_node->mutex);
      boost::mutex::scoped_lock node_lock(node_data->mutex);

      if ((flags & OPEN_FLAG_LOCK_SHARED) == OPEN_FLAG_LOCK_SHARED) {
        if (node_data->exclusive_lock_handle != 0)
          HT_THROW(Error::HYPERSPACE_LOCK_CONFLICT, "");
        lock_mode = LOCK_MODE_SHARED;
        if (node_data->shared_lock_handles.empty())
          lock_notifiy = true;
      }
      else if ((flags & OPEN_FLAG_LOCK_EXCLUSIVE) == OPEN_FLAG_LOCK_EXCLUSIVE) {
        if (node_data->exclusive_lock_handle != 0 ||
            !node_data->shared_lock_handles.empty())
          HT_THROW(Error::HYPERSPACE_LOCK_CONFLICT, "");
        lock_mode = LOCK_MODE_EXCLUSIVE;
        lock_notifiy = true;
      }

      existed = m_bdb_fs->exists(txn, name, &is_dir);

      if (existed) {
        if ((flags & OPEN_FLAG_CREATE) && (flags & OPEN_FLAG_EXCL))
          HT_THROW(Error::HYPERSPACE_FILE_EXISTS, "mode=CREATE|EXCL");

        if ((flags & OPEN_FLAG_TEMP))
          HT_THROWF(Error::HYPERSPACE_FILE_EXISTS, "Unable to open TEMP file "
                    "'%s' because it already exists", name);
        // Read/create 'lock.generation' attribute
        if (node_data->lock_generation == 0) {
          if (!m_bdb_fs->get_xattr_i64(txn, name, "lock.generation",
                                       &node_data->lock_generation)) {
            node_data->lock_generation = 1;
            m_bdb_fs->set_xattr_i64(txn, name, "lock.generation",
                                    node_data->lock_generation);
          }
        }
      }
      else {
        if (!(flags & OPEN_FLAG_CREATE))
          HT_THROW(Error::HYPERSPACE_BAD_PATHNAME, name);

        m_bdb_fs->create(txn, name, (flags & OPEN_FLAG_TEMP));
        node_data->lock_generation = 1;
        m_bdb_fs->set_xattr_i64(txn, name, "lock.generation",
                                node_data->lock_generation);
        // Set the initial attributes
        for (size_t i=0; i<init_attrs.size(); i++)
          m_bdb_fs->set_xattr(txn, name, init_attrs[i].name,
                              init_attrs[i].value, init_attrs[i].value_len);
        if (flags & OPEN_FLAG_TEMP)
          node_data->ephemeral = true;
        created = true;
      }

      create_handle(&handle, handle_data);

      handle_data->node = node_data.get();
      handle_data->open_flags = flags;
      handle_data->event_mask = event_mask;
      handle_data->session_data = session_data;

      session_data->add_handle(handle);

      if (created) {
        HyperspaceEventPtr event(new EventNamed(EVENT_MASK_CHILD_NODE_ADDED,
                                                child_name));
        deliver_event_notifications(parent_node.get(), event);
      }

      /**
       * If open flags LOCK_SHARED or LOCK_EXCLUSIVE, then obtain lock
       */
      if (lock_mode != 0) {
        handle_data->node->lock_generation++;
        m_bdb_fs->set_xattr_i64(txn, name, "lock.generation",
                                handle_data->node->lock_generation);
        lock_generation = handle_data->node->lock_generation;
        handle_data->node->cur_lock_mode = lock_mode;

        lock_handle(handle_data, lock_mode);

        // deliver notification to handles to this same node
        if (lock_notifiy) {
          HyperspaceEventPtr event(new EventLockAcquired(lock_mode));
          deliver_event_notifications(handle_data->node, event);
        }
      }

      handle_data->node->add_handle(handle, handle_data);

      txn->commit(0);
    }
  }
  HT_BDBTXN_END_CB(cb);

  cb->response(handle, created, lock_generation);
}


/**
 *
 */
void Master::close(ResponseCallback *cb, uint64_t session_id, uint64_t handle) {
  SessionDataPtr session_data;
  int error;
  std::string errmsg;
  size_t n;

  if (m_verbose) {
    HT_INFOF("close(session=%lld, handle=%lld)", session_id, handle);
  }

  if (!get_session(session_id, session_data)) {
    cb->error(Error::HYPERSPACE_EXPIRED_SESSION, "");
    return;
  }

  {
    boost::mutex::scoped_lock slock(session_data->mutex);
    n = session_data->handles.erase(handle);
  }

  if (n) {
    if (!destroy_handle(handle, &error, errmsg)) {
      cb->error(error, errmsg);
      return;
    }
  }

  if ((error = cb->response_ok()) != Error::OK) {
    HT_ERRORF("Problem sending back response - %s", Error::get_text(error));
  }
}



/**
 *
 */
void
Master::attr_set(ResponseCallback *cb, uint64_t session_id, uint64_t handle,
                 const char *name, const void *value, size_t value_len) {
  SessionDataPtr session_data;
  HandleDataPtr handle_data;
  int error;

  if (m_verbose) {
    HT_INFOF("attrset(session=%lld, handle=%lld, name=%s, value_len=%d)",
             session_id, handle, name, value_len);
  }

  HT_BDBTXN_BEGIN {
    if (!get_session(session_id, session_data))
      HT_THROWF(Error::HYPERSPACE_EXPIRED_SESSION, "%llu", (Llu)session_id);

    if (!get_handle_data(handle, handle_data))
      HT_THROWF(Error::HYPERSPACE_INVALID_HANDLE, "handle=%llu", (Llu)handle);

    {
      boost::mutex::scoped_lock node_lock(handle_data->node->mutex);

      m_bdb_fs->set_xattr(txn, handle_data->node->name, name, value, value_len);

      HyperspaceEventPtr event(new EventNamed(EVENT_MASK_ATTR_SET, name));
      deliver_event_notifications(handle_data->node, event);
    }

    txn->commit(0);
  }
  HT_BDBTXN_END_CB(cb);

  if ((error = cb->response_ok()) != Error::OK)
    HT_ERRORF("Problem sending back response - %s", Error::get_text(error));
}


/**
 *
 */
void
Master::attr_get(ResponseCallbackAttrGet *cb, uint64_t session_id,
                 uint64_t handle, const char *name) {
  SessionDataPtr session_data;
  HandleDataPtr handle_data;
  int error;
  DynamicBuffer dbuf;

  if (m_verbose)
    HT_INFOF("attrget(session=%lld, handle=%lld, name=%s)",
             session_id, handle, name);

  HT_BDBTXN_BEGIN {
    if (!get_session(session_id, session_data))
      HT_THROWF(Error::HYPERSPACE_EXPIRED_SESSION, "%llu", (Llu)session_id);

    if (!get_handle_data(handle, handle_data))
      HT_THROWF(Error::HYPERSPACE_INVALID_HANDLE, "handle=%llu", (Llu)handle);

    {
      boost::mutex::scoped_lock node_lock(handle_data->node->mutex);
      if (!m_bdb_fs->get_xattr(txn, handle_data->node->name, name, dbuf))
        HT_THROW(Error::HYPERSPACE_ATTR_NOT_FOUND, name);
    }

    txn->commit(0);
  }
  HT_BDBTXN_END_CB(cb);

  StaticBuffer buffer(dbuf);

  if ((error = cb->response(buffer)) != Error::OK)
    HT_ERRORF("Problem sending back response - %s", Error::get_text(error));
}



void Master::attr_del(ResponseCallback *cb, uint64_t session_id, uint64_t handle, const char *name) {
  SessionDataPtr session_data;
  HandleDataPtr handle_data;
  int error;

  if (m_verbose)
    HT_INFOF("attrdel(session=%lld, handle=%lld, name=%s)",
             session_id, handle, name);

  HT_BDBTXN_BEGIN {
    if (!get_session(session_id, session_data))
      HT_THROWF(Error::HYPERSPACE_EXPIRED_SESSION, "%llu", (Llu)session_id);

    if (!get_handle_data(handle, handle_data))
      HT_THROWF(Error::HYPERSPACE_INVALID_HANDLE, "handle=%llu", (Llu)handle);

    {
      boost::mutex::scoped_lock node_lock(handle_data->node->mutex);

      m_bdb_fs->del_xattr(txn, handle_data->node->name, name);

      HyperspaceEventPtr event(new EventNamed(EVENT_MASK_ATTR_DEL, name));
      deliver_event_notifications(handle_data->node, event);
    }

    txn->commit(0);
  }
  HT_BDBTXN_END_CB(cb);

  if ((error = cb->response_ok()) != Error::OK)
    HT_ERRORF("Problem sending back response - %s", Error::get_text(error));
}


void
Master::exists(ResponseCallbackExists *cb, uint64_t session_id,
               const char *name) {
  int error;
  bool file_exists;

  if (m_verbose)
    HT_INFOF("exists(session_id=%lld, name=%s)", session_id, name);

  assert(name[0] == '/' && name[strlen(name)-1] != '/');

  HT_BDBTXN_BEGIN {
    file_exists = m_bdb_fs->exists(txn, name);
    txn->commit(0);
  }
  HT_BDBTXN_END_CB(cb);

  if ((error = cb->response(file_exists)) != Error::OK)
    HT_ERRORF("Problem sending back response - %s", Error::get_text(error));
}


void
Master::readdir(ResponseCallbackReaddir *cb, uint64_t session_id,
                uint64_t handle) {
  std::string abs_name;
  SessionDataPtr session_data;
  HandleDataPtr handle_data;
  DirEntry dentry;
  std::vector<DirEntry> listing;

  if (m_verbose)
    HT_INFOF("readdir(session=%lld, handle=%lld)", session_id, handle);

  HT_BDBTXN_BEGIN {
    if (!get_session(session_id, session_data))
      HT_THROWF(Error::HYPERSPACE_EXPIRED_SESSION, "%llu", (Llu)session_id);

    if (!get_handle_data(handle, handle_data))
      HT_THROWF(Error::HYPERSPACE_INVALID_HANDLE, "handle=%llu", (Llu)handle);

    {
      boost::mutex::scoped_lock lock(handle_data->node->mutex);

      m_bdb_fs->get_directory_listing(txn, handle_data->node->name, listing);
    }

    txn->commit(0);
  }
  HT_BDBTXN_END_CB(cb);

  cb->response(listing);
}


void
Master::lock(ResponseCallbackLock *cb, uint64_t session_id, uint64_t handle,
             uint32_t mode, bool try_lock) {
  SessionDataPtr session_data;
  HandleDataPtr handle_data;
  bool notify = true;

  if (m_verbose) {
    HT_INFOF("lock(session=%lld, handle=%lld, mode=0x%x, try_lock=%d)",
             session_id, handle, mode, try_lock);
  }

  if (!get_session(session_id, session_data)) {
    cb->error(Error::HYPERSPACE_EXPIRED_SESSION, "");
    return;
  }

  if (!get_handle_data(handle, handle_data)) {
    cb->error(Error::HYPERSPACE_INVALID_HANDLE,
              std::string("handle=") + handle);
    return;
  }

  if (!(handle_data->open_flags & OPEN_FLAG_LOCK)) {
    cb->error(Error::HYPERSPACE_MODE_RESTRICTION,
              "handle not open for locking");
    return;
  }

  if (!(handle_data->open_flags & OPEN_FLAG_WRITE)) {
    cb->error(Error::HYPERSPACE_MODE_RESTRICTION,
              "handle not open for writing");
    return;
  }

  {
    boost::mutex::scoped_lock lock(handle_data->node->mutex);
    NodeData::LockRequestList &pending_lock_reqs =
        handle_data->node->pending_lock_reqs;

    if (handle_data->node->cur_lock_mode == LOCK_MODE_EXCLUSIVE) {
      if (try_lock)
        cb->response(LOCK_STATUS_BUSY);
      else {
        pending_lock_reqs.push_back(LockRequest(handle, mode));
        cb->response(LOCK_STATUS_PENDING);
      }
      return;
    }
    else if (handle_data->node->cur_lock_mode == LOCK_MODE_SHARED) {
      if (mode == LOCK_MODE_EXCLUSIVE) {
        if (try_lock)
          cb->response(LOCK_STATUS_BUSY);
        else {
          pending_lock_reqs.push_back(LockRequest(handle, mode));
          cb->response(LOCK_STATUS_PENDING);
        }
        return;
      }

      assert(mode == LOCK_MODE_SHARED);

      if (!pending_lock_reqs.empty()) {
        if (try_lock)
          cb->response(LOCK_STATUS_BUSY);
        else {
	  pending_lock_reqs.push_back(LockRequest(handle, mode));
	  cb->response(LOCK_STATUS_PENDING);
	}
        return;
      }
    }

    // at this point we're OK to acquire the lock
    if (mode == LOCK_MODE_SHARED &&
        !handle_data->node->shared_lock_handles.empty())
      notify = false;

    handle_data->node->lock_generation++;

    HT_BDBTXN_BEGIN {
      m_bdb_fs->set_xattr_i64(txn, handle_data->node->name, "lock.generation",
                              handle_data->node->lock_generation);
      txn->commit(0);
    }
    HT_BDBTXN_END_CB(cb);

    handle_data->node->cur_lock_mode = mode;
    lock_handle(handle_data, mode);

    // deliver notification to handles to this same node
    if (notify) {
      HyperspaceEventPtr event_ptr(new EventLockAcquired(mode));
      deliver_event_notifications(handle_data->node, event_ptr);
    }

    cb->response(LOCK_STATUS_GRANTED, handle_data->node->lock_generation);
  }
}

/**
 * Assumes node is locked.
 */
void Master::lock_handle(HandleDataPtr &handle_data, uint32_t mode) {
  if (mode == LOCK_MODE_SHARED)
    handle_data->node->shared_lock_handles.insert(handle_data->id);
  else {
    assert(mode == LOCK_MODE_EXCLUSIVE);
    handle_data->node->exclusive_lock_handle = handle_data->id;
  }
  handle_data->locked = true;
}

/**
 * Assumes node is locked.
 */
void
Master::lock_handle_with_notification(HandleDataPtr &handle_data,
                                      uint32_t mode, bool wait_for_notify) {
  lock_handle(handle_data, mode);

  // deliver notification to handles to this same node
  HyperspaceEventPtr event(new EventLockGranted(mode,
                           handle_data->node->lock_generation));
  deliver_event_notification(handle_data, event, wait_for_notify);
}



/**
 *
 */
void
Master::release(ResponseCallback *cb, uint64_t session_id, uint64_t handle) {
  SessionDataPtr session_data;
  HandleDataPtr handle_data;

  if (m_verbose) {
    HT_INFOF("release(session=%lld, handle=%lld)", session_id, handle);
  }

  if (!get_session(session_id, session_data)) {
    cb->error(Error::HYPERSPACE_EXPIRED_SESSION, "");
    return;
  }

  if (!get_handle_data(handle, handle_data)) {
    cb->error(Error::HYPERSPACE_INVALID_HANDLE,
              std::string("handle=") + handle);
    return;
  }

  release_lock(handle_data);

  cb->response_ok();
}


void Master::release_lock(HandleDataPtr &handle_data, bool wait_for_notify) {
  boost::mutex::scoped_lock lock(handle_data->node->mutex);
  vector<HandleDataPtr> next_lock_handles;
  int next_mode = 0;

  if (handle_data->locked) {
    if (handle_data->node->exclusive_lock_handle != 0) {
      assert(handle_data->id == handle_data->node->exclusive_lock_handle);
      handle_data->node->exclusive_lock_handle = 0;
    }
    else {
      unsigned int count = handle_data->node->
          shared_lock_handles.erase(handle_data->id);
      HT_EXPECT(count, Error::FAILED_EXPECTATION);
    }
    handle_data->locked = false;
  }
  else
    return;

  // deliver LOCK_RELEASED notifications if no more locks held on node
  if (handle_data->node->shared_lock_handles.empty()) {
    HT_INFO("About to deliver lock released notifications");
    HyperspaceEventPtr event_ptr(new EventLockReleased());
    deliver_event_notifications(handle_data->node, event_ptr, wait_for_notify);
    HT_INFO("Finished delivering lock released notifications");
  }

  handle_data->node->cur_lock_mode = 0;

  if (!handle_data->node->pending_lock_reqs.empty()) {
    HandleDataPtr next_handle;
    const LockRequest &front_lock_req = handle_data->node->
        pending_lock_reqs.front();
    if (front_lock_req.mode == LOCK_MODE_EXCLUSIVE) {
      next_mode = LOCK_MODE_EXCLUSIVE;
      if (get_handle_data(front_lock_req.handle, next_handle))
        next_lock_handles.push_back(next_handle);
      handle_data->node->pending_lock_reqs.pop_front();
    }
    else {
      next_mode = LOCK_MODE_SHARED;
      do {
        const LockRequest &lockreq = handle_data->node->
            pending_lock_reqs.front();
        if (lockreq.mode != LOCK_MODE_SHARED)
          break;
        if (get_handle_data(lockreq.handle, next_handle))
          next_lock_handles.push_back(next_handle);
        handle_data->node->pending_lock_reqs.pop_front();
      } while (!handle_data->node->pending_lock_reqs.empty());
    }

    if (!next_lock_handles.empty()) {
      handle_data->node->lock_generation++;

      HT_BDBTXN_BEGIN {
        m_bdb_fs->set_xattr_i64(txn, handle_data->node->name, "lock.generation",
                                handle_data->node->lock_generation);
        txn->commit(0);
      }
      HT_BDBTXN_END();

      handle_data->node->cur_lock_mode = next_mode;

      for (size_t i=0; i<next_lock_handles.size(); i++) {
        assert(handle_data->id != next_lock_handles[i]->id);
        lock_handle_with_notification(next_lock_handles[i], next_mode,
                                      wait_for_notify);
      }

      // deliver notification to handles to this same node
      HyperspaceEventPtr event(new EventLockAcquired(next_mode));
      deliver_event_notifications(handle_data->node, event, wait_for_notify);
    }
  }
}


/**
 * Assumes node is locked.
 */
void
Master::deliver_event_notifications(NodeData *node,
    HyperspaceEventPtr &event_ptr, bool wait_for_notify) {
  int notifications = 0;

  // log event
  for (HandleMap::iterator iter = node->handle_map.begin();
       iter != node->handle_map.end(); iter++) {
    //HT_INFOF("Delivering notification (%d == %d)", (*iter).second->event_mask, event_ptr->get_mask());
    if ((*iter).second->event_mask & event_ptr->get_mask()) {
      (*iter).second->session_data->add_notification(
          new Notification((*iter).first, event_ptr));
      m_keepalive_handler_ptr->deliver_event_notifications(
          (*iter).second->session_data->id);
      notifications++;
    }
  }

  if (wait_for_notify && notifications)
    event_ptr->wait_for_notifications();
}


/**
 * Assumes node is locked.
 */
void
Master::deliver_event_notification(HandleDataPtr &handle_data,
    HyperspaceEventPtr &event_ptr, bool wait_for_notify) {

  // log event
  handle_data->session_data->add_notification(
      new Notification(handle_data->id, event_ptr));
  m_keepalive_handler_ptr->deliver_event_notifications(
      handle_data->session_data->id);

  if (wait_for_notify)
    event_ptr->wait_for_notifications();

}



/**
 *
 */
bool
Master::find_parent_node(const std::string &normal_name,
                         NodeDataPtr &parent_node, std::string &child_name) {
  NodeMap::iterator node_it;
  size_t last_slash = normal_name.rfind("/", normal_name.length());

  child_name.clear();

  if (last_slash > 0) {
    boost::mutex::scoped_lock lock(m_node_map_mutex);
    std::string parent_name(normal_name, 0, last_slash);
    child_name.append(normal_name, last_slash + 1,
                      normal_name.length() - last_slash - 1);
    node_it = m_node_map.find(parent_name);

    if (node_it != m_node_map.end())
      parent_node = (*node_it).second;
    else {
      parent_node = new NodeData();
      parent_node->name = parent_name;
      m_node_map[parent_name] = parent_node;
    }
    return true;
  }
  else if (last_slash == 0) {
    boost::mutex::scoped_lock lock(m_node_map_mutex);
    std::string parent_name = "/";
    node_it = m_node_map.find(parent_name);
    assert (node_it != m_node_map.end());
    child_name.append(normal_name, 1, normal_name.length() - 1);
    parent_node = (*node_it).second;
    return true;
  }

  return false;
}


/**
 *
 */
bool
Master::destroy_handle(uint64_t handle, int *errorp, std::string &errmsg,
                       bool wait_for_notify) {
  HandleDataPtr handle_data;
  unsigned int refcount = 0;

  if (!remove_handle_data(handle, handle_data)) {
    *errorp = Error::HYPERSPACE_INVALID_HANDLE;
    errmsg = std::string("handle=") + handle;
    return false;
  }

  {
    boost::mutex::scoped_lock lock(handle_data->node->mutex);

    handle_data->node->remove_handle(handle_data->id);
    refcount = handle_data->node->reference_count();
  }

  release_lock(handle_data, wait_for_notify);

  if (refcount == 0) {

    if (handle_data->node->ephemeral) {
      std::string child_name;
      NodeDataPtr parent_node;

      if (find_parent_node(handle_data->node->name, parent_node, child_name)) {
        HyperspaceEventPtr event(new EventNamed(EVENT_MASK_CHILD_NODE_REMOVED,
                                                child_name));
        deliver_event_notifications(parent_node.get(), event, wait_for_notify);
      }

      // remove file from database
      HT_BDBTXN_BEGIN {
        m_bdb_fs->unlink(txn, handle_data->node->name);
        txn->commit(0);
      }
      HT_BDBTXN_END(false);

      // remove node
      NodeMap::iterator node_it = m_node_map.find(handle_data->node->name);

      if (node_it != m_node_map.end())
        m_node_map.erase(node_it);
    }
  }

  return true;
}


/**
 */
void Master::get_generation_number() {

  HT_BDBTXN_BEGIN {
    if (!m_bdb_fs->get_xattr_i32(txn, "/hyperspace/metadata", "generation",
                                 &m_generation))
      m_generation = 0;

    m_generation++;
    m_bdb_fs->set_xattr_i32(txn, "/hyperspace/metadata", "generation",
                            m_generation);
    txn->commit(0);
  }
  HT_BDBTXN_END();

}
