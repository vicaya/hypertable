/**
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERSPACE_MASTER_H
#define HYPERSPACE_MASTER_H

#include <queue>
#include <vector>

#include "Common/atomic.h"
#include "Common/Mutex.h"
#include "Common/Properties.h"
#include "Common/ReferenceCount.h"
#include "Common/SockAddrMap.h"
#include "Common/StringExt.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/ResponseCallback.h"

#include "BerkeleyDbFilesystem.h"
#include "Protocol.h"
#include "ResponseCallbackOpen.h"
#include "ResponseCallbackExists.h"
#include "ResponseCallbackAttrGet.h"
#include "ResponseCallbackAttrExists.h"
#include "ResponseCallbackAttrList.h"
#include "ResponseCallbackLock.h"
#include "ResponseCallbackReaddir.h"
#include "ServerKeepaliveHandler.h"

namespace Hyperspace {

  class Master : public ReferenceCount {
  public:

    enum { TIMER_INTERVAL_MS=1000 };

    Master(ConnectionManagerPtr &, PropertiesPtr &,
           ServerKeepaliveHandlerPtr &, ApplicationQueuePtr &app_queue_ptr);
    ~Master();

    // Hyperspace command implementations
    void mkdir(ResponseCallback *cb, uint64_t session_id, const char *name);
    void unlink(ResponseCallback *cb, uint64_t session_id, const char *name);
    void open(ResponseCallbackOpen *cb, uint64_t session_id, const char *name,
              uint32_t flags, uint32_t event_mask,
              std::vector<Attribute> &init_attrs);
    void close(ResponseCallback *cb, uint64_t session_id, uint64_t handle);
    void attr_set(ResponseCallback *cb, uint64_t session_id, uint64_t handle,
                  const char *name, const void *value, size_t value_len);
    void attr_get(ResponseCallbackAttrGet *cb, uint64_t session_id,
                  uint64_t handle, const char *name);
    void attr_del(ResponseCallback *cb, uint64_t session_id, uint64_t handle,
                  const char *name);
    void attr_exists(ResponseCallbackAttrExists *cb, uint64_t session_id, uint64_t handle,
                     const char *name);
    void attr_list(ResponseCallbackAttrList *cb,
                   uint64_t session_id, uint64_t handle);
    void exists(ResponseCallbackExists *cb, uint64_t session_id,
                const char *name);
    void readdir(ResponseCallbackReaddir *cb, uint64_t session_id,
                 uint64_t handle);
    void lock(ResponseCallbackLock *cb, uint64_t session_id, uint64_t handle,
              uint32_t mode, bool try_lock);
    void release(ResponseCallback *cb, uint64_t session_id, uint64_t handle);

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
     * @param session_id Session ID to lookup
     * @param session_data Reference to SessionData smart pointer
     * @return true if found, false otherwise
     */
    bool get_session(uint64_t session_id, SessionDataPtr &session_data);

    void destroy_session(uint64_t session_id);
    void initialize_session(uint64_t session_id, const String &name);

    /**
     * Attempts to renew the session lease for session with the given ID.  If
     * the session cannot be found or if it is expired, the method returns
     * Error::HYPERSPACE_EXPIRED_SESSION otherwise, it renews the session
     * lease by invoking the RenewLease method of the SessionData object.
     *
     * @param session_id Session ID to renew
     * @return Error::OK if successful
     */
    int renew_session_lease(uint64_t session_id);

    bool next_expired_session(SessionDataPtr &, boost::xtime &now);
    void remove_expired_sessions();


    void get_datagram_send_address(struct sockaddr_in *addr) {
      memcpy(addr, &m_local_addr, sizeof(m_local_addr));
    }

    void tick() {
      ScopedLock lock(m_last_tick_mutex);
      boost::xtime now;
      boost::xtime_get(&now, boost::TIME_UTC);
      m_lease_credit = xtime_diff_millis(m_last_tick, now);
      if (m_lease_credit < 5000)
        m_lease_credit = 0;
      memcpy(&m_last_tick, &now, sizeof(boost::xtime));
    }

  private:

    void get_generation_number();

    void normalize_name(std::string name, std::string &normal);
    void deliver_event_notifications(HyperspaceEventPtr &event_ptr,
        NotificationMap &handles_to_sessions, bool wait_for_notify = true);
    void persist_event_notifications(DbTxn *txn, uint64_t event_id,
                                     NotificationMap &handles_to_sessions);
    void persist_event_notifications(DbTxn *txn, uint64_t event_id, uint64_t handle);
    bool validate_and_create_node_data(DbTxn *txn, const String &node);
    /**
     * Locates the parent 'node' of the given pathname.  It determines the name
     * of the parent node by stripping off the characters incuding and after
     * the last '/' character.  It then looks up the name in the m_node_map and
     * sets the pointer reference 'parent_node' to it. As a side effect, it
     * also saves the child name (e.g. characters after the last '/' character
     * to the string reference child_name.  NOTE: This method locks the
     * m_node_map_mutex.
     *
     * @param normal_name Normalized (e.g. no trailing '/') name of path to
     *        find parent of
     * @param parent_node pointer reference to hold return pointer
     * @param child_name reference to string to hold the child directory entry
     *        name
     * @return true if parent node found, false otherwise
     */
    bool find_parent_node(const std::string &normal_name,
                          std::string &parent_name, std::string &child_name);
    bool destroy_handle(uint64_t handle, int *errorp, std::string &errmsg,
                        bool wait_for_notify=true);
    void release_lock(DbTxn *txn, uint64_t handle, const String &node,
        HyperspaceEventPtr &release_event, NotificationMap &release_notifications);
    void lock_handle(DbTxn *txn, uint64_t handle, uint32_t mode, String &node = "");
    void lock_handle(DbTxn *txn, uint64_t handle, uint32_t mode, const String &node);
    void lock_handle_with_notification(uint64_t handle, uint32_t mode,
                                       bool wait_for_notify=true);
    void grant_pending_lock_reqs(DbTxn *txn, const String &node,
        HyperspaceEventPtr &lock_granted_event, NotificationMap &lock_granted_notifications,
        HyperspaceEventPtr &lock_acquired_event, NotificationMap &lock_acquired_notifications);
    void recover_state();

    typedef std::vector<SessionDataPtr> SessionDataVec;
    typedef hash_map<uint64_t, SessionDataPtr> SessionMap;

    bool          m_verbose;
    uint32_t      m_lease_interval;
    uint32_t      m_keep_alive_interval;
    std::string   m_base_dir;
    int           m_base_fd;
    uint32_t      m_generation;
    uint64_t      m_next_handle_number;
    uint64_t      m_next_session_id;
    ServerKeepaliveHandlerPtr m_keepalive_handler_ptr;
    struct sockaddr_in m_local_addr;
    SessionDataVec m_session_heap;
    SessionMap m_session_map;

    Mutex         m_session_map_mutex;
    Mutex         m_last_tick_mutex;
    boost::xtime  m_last_tick;
    uint64_t      m_lease_credit;

    // BerkeleyDB state
    BerkeleyDbFilesystem *m_bdb_fs;

  };

  typedef boost::intrusive_ptr<Master> MasterPtr;

} // namespace Hyperspace

#endif // HYPERSPACE_MASTER_H
