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

#ifndef HYPERSPACE_MASTER_H
#define HYPERSPACE_MASTER_H

#include <queue>
#include <vector>
#include <ext/hash_map>

#include <boost/thread/mutex.hpp>

#include "Common/atomic.h"
#include "Common/Properties.h"
#include "Common/ReferenceCount.h"
#include "Common/SockAddrMap.h"
#include "Common/StringExt.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/ResponseCallback.h"

#include "NodeData.h"
#include "HandleData.h"
#include "Protocol.h"
#include "ResponseCallbackOpen.h"
#include "ResponseCallbackExists.h"
#include "ResponseCallbackAttrGet.h"
#include "ResponseCallbackLock.h"
#include "ResponseCallbackReaddir.h"
#include "ServerKeepaliveHandler.h"
#include "SessionData.h"

using namespace Hypertable;

namespace Hyperspace {

  class Master : public ReferenceCount {
  public:

    static const uint32_t DEFAULT_MASTER_PORT        = 38040;
    static const uint32_t DEFAULT_LEASE_INTERVAL     = 20;
    static const uint32_t DEFAULT_KEEPALIVE_INTERVAL = 10;
    static const uint32_t DEFAULT_GRACEPERIOD        = 60;

    Master(ConnectionManagerPtr &connManagerPtr, PropertiesPtr &propsPtr, ServerKeepaliveHandlerPtr &keepaliveHandlerPtr);
    ~Master();

    void mkdir(ResponseCallback *cb, uint64_t sessionId, const char *name);
    void unlink(ResponseCallback *cb, uint64_t sessionId, const char *name);
    void open(ResponseCallbackOpen *cb, uint64_t sessionId, const char *name, uint32_t flags, uint32_t eventMask, std::vector<AttributeT> &initAttrs);
    void close(ResponseCallback *cb, uint64_t sessionId, uint64_t handle);
    void attr_set(ResponseCallback *cb, uint64_t sessionId, uint64_t handle, const char *name, const void *value, size_t valueLen);
    void attr_get(ResponseCallbackAttrGet *cb, uint64_t sessionId, uint64_t handle, const char *name);
    void attr_del(ResponseCallback *cb, uint64_t sessionId, uint64_t handle, const char *name);
    void exists(ResponseCallbackExists *cb, uint64_t sessionId, const char *name);
    void readdir(ResponseCallbackReaddir *cb, uint64_t sessionId, uint64_t handle);
    void lock(ResponseCallbackLock *cb, uint64_t sessionId, uint64_t handle, uint32_t mode, bool tryAcquire);
    void release(ResponseCallback *cb, uint64_t sessionId, uint64_t handle);

    /**
     * Creates a new session by allocating a new SessionData object, obtaining a
     * new session ID and inserting the object into the Session map.
     *
     * @param addr Address from which the UDP client is sending
     * @return the new session ID
     */
    uint64_t create_session(struct sockaddr_in &addr);

    /**
     * Obtains the SessionData object for the given id from the session map.
     *
     * @param sessionId Session ID to lookup
     * @param sessionPtr Reference to SessionData smart pointer
     * @return true if found, false otherwise
     */
    bool get_session(uint64_t sessionId, SessionDataPtr &sessionPtr);

    void destroy_session(uint64_t sessionId);

    /**
     * Attempts to renew the session lease for session with the given ID.  If
     * the session cannot be found or if it is expired, the method returns
     * Error::HYPERSPACE_EXPIRED_SESSION otherwise, it renews the session
     * lease by invoking the RenewLease method of the SessionData object.
     *
     * @param sessionId Session ID to renew
     * @return Error::OK if successful
     */
    int renew_session_lease(uint64_t sessionId);

    bool next_expired_session(SessionDataPtr &sessionPtr);
    void remove_expired_sessions();

    void create_handle(uint64_t *handlep, HandleDataPtr &handlePtr);
    bool get_handle_data(uint64_t sessionId, HandleDataPtr &handlePtr);
    bool remove_handle_data(uint64_t sessionId, HandleDataPtr &handlePtr);

    void get_datagram_send_address(struct sockaddr_in *addr) { memcpy(addr, &m_local_addr, sizeof(m_local_addr)); }

  private:

    void report_error(ResponseCallback *cb);
    void normalize_name(std::string name, std::string &normal);
    void deliver_event_notifications(NodeData *node, HyperspaceEventPtr &eventPtr, bool waitForNotify=true);
    void deliver_event_notification(HandleDataPtr &handlePtr, HyperspaceEventPtr &eventPtr, bool waitForNotify=true);

    /**
     * Locates the parent 'node' of the given pathname.  It determines the name of the parent
     * node by stripping off the characters incuding and after the last '/' character.  It then
     * looks up the name in the m_node_map and sets the pointer reference 'parentNodePtr' to it.
     * As a side effect, it also saves the child name (e.g. characters after the last '/' character
     * to the string reference childName.  NOTE: This method locks the m_node_map_mutex.
     *
     * @param normalName Normalized (e.g. no trailing '/') name of path to find parent of
     * @param parentNodePtr pointer reference to hold return pointer
     * @param childName reference to string to hold the child directory entry name
     * @return true if parent node found, false otherwise
     */
    bool find_parent_node(std::string normalName, NodeDataPtr &parentNodePtr, std::string &childName);
    bool destroy_handle(uint64_t handle, int *errorp, std::string &errMsg, bool waitForNotify=true);
    void release_lock(HandleDataPtr &handlePtr, bool waitForNotify=true);
    void lock_handle(HandleDataPtr &handlePtr, uint32_t mode);
    void lock_handle_with_notification(HandleDataPtr &handlePtr, uint32_t mode, bool waitForNotify=true);

    /**
     * Safely obtains the node with the given pathname from the NodeMap,
     * creating one and inserting it into the map if it is not found.
     *
     * @param name pathname of node
     * @param nodePtr Reference of node smart pointer to hold return node
     */
    void get_node(std::string name, NodeDataPtr &nodePtr) {
      boost::mutex::scoped_lock lock(m_node_map_mutex);
      NodeMapT::iterator iter = m_node_map.find(name);
      if (iter != m_node_map.end()) {
	nodePtr = (*iter).second;
	return;
      }
      nodePtr = new NodeData();
      nodePtr->name = name;
      m_node_map[name] = nodePtr;
    }

    typedef __gnu_cxx::hash_map<std::string, NodeDataPtr> NodeMapT;
    typedef __gnu_cxx::hash_map<uint64_t, HandleDataPtr>  HandleMapT;
    typedef __gnu_cxx::hash_map<uint64_t, SessionDataPtr> SessionMapT;

    bool          m_verbose;
    uint32_t      m_lease_interval;
    uint32_t      m_keep_alive_interval;
    NodeMapT      m_node_map;
    HandleMapT    m_handle_map;
    SessionMapT   m_session_map;
    boost::mutex  m_node_map_mutex;
    boost::mutex  m_handle_map_mutex;
    boost::mutex  m_session_map_mutex;
    std::string   m_base_dir;
    int           m_base_fd;
    uint32_t      m_generation;
    uint64_t      m_next_handle_number;
    uint64_t      m_next_session_id;
    ServerKeepaliveHandlerPtr m_keepalive_handler_ptr;
    struct sockaddr_in m_local_addr;
    std::vector<SessionDataPtr> m_session_heap;

  };
  typedef boost::intrusive_ptr<Master> MasterPtr;

}

#endif // HYPERSPACE_MASTER_H
