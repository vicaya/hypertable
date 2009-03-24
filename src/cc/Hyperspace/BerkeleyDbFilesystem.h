/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#ifndef HT_BERKELEYDBFILESYSTEM_H
#define HT_BERKELEYDBFILESYSTEM_H

#include "Common/Compat.h"
#include <vector>

#include <db_cxx.h>

#include "Common/String.h"
#include "Common/DynamicBuffer.h"
#include "Common/InetAddr.h"
#include "Common/HashMap.h"

#include "DirEntry.h"
#include "StateDbKeys.h"

namespace Hyperspace {
  using namespace Hypertable;
  using namespace std;

  // handle id --> session id
  typedef hash_map<uint64_t, uint64_t> NotificationMap;

  class LockRequest {
  public:
    LockRequest(uint64_t h=0, int m=0) : handle(h), mode(m) { return; }
    uint64_t handle;
    int mode;
  };

  enum {
    SESSION_ID,
    HANDLE_ID,
    EVENT_ID
  };

  class BerkeleyDbFilesystem {
  public:
    BerkeleyDbFilesystem(const String &basedir, bool force_recover=false);
    ~BerkeleyDbFilesystem();

    /**
     * Creates a new BerkeleyDB transaction within the context of a parent
     * transaction.
     *
     * @param parent_txn Parent transaction (use NULL if there is no parent)
     * @return pointer to a new BerkeleyDB txn object
     */
    DbTxn *start_transaction(DbTxn *parent_txn=NULL);

    bool get_xattr_i32(DbTxn *txn, const String &fname,
                       const String &aname, uint32_t *valuep);
    void set_xattr_i32(DbTxn *txn, const String &fname,
                       const String &aname, uint32_t value);
    bool get_xattr_i64(DbTxn *txn, const String &fname,
                       const String &aname, uint64_t *valuep);
    void set_xattr_i64(DbTxn *txn, const String &fname,
                       const String &aname, uint64_t value);
    void set_xattr(DbTxn *txn, const String &fname,
                   const String &aname, const void *value, size_t value_len);
    bool get_xattr(DbTxn *txn, const String &fname, const String &aname,
                   Hypertable::DynamicBuffer &vbuf);
    bool exists_xattr(DbTxn *txn, const String &fname, const String &aname);
    void del_xattr(DbTxn *txn, const String &fname, const String &aname);
    void mkdir(DbTxn *txn, const String &name);
    void unlink(DbTxn *txn, const String &name);
    bool exists(DbTxn *txn, String fname, bool *is_dir_p=0);
    void create(DbTxn *txn, const String &fname, bool temp);
    void get_directory_listing(DbTxn *txn, String fname,
                               std::vector<DirEntry> &listing);
    void get_all_names(DbTxn *txn, std::vector<String> &names);
    bool list_xattr(DbTxn *txn, const String& fname, std::vector<String> &anames);
    /**
     * Persists a new event in the StateDB
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param type Event type
     * @param id Event id
     * @param mask Event mask
     */
    void create_event(DbTxn *txn, uint32_t type, uint64_t id, uint32_t mask);
    // for named event
    void create_event(DbTxn *txn, uint32_t type, uint64_t id, uint32_t mask,
                      const String &name);
    void create_event(DbTxn *txn, uint32_t type, uint64_t id, uint32_t mask, uint32_t mode);
    void create_event(DbTxn *txn, uint32_t type, uint64_t id, uint32_t mask,
                      uint32_t mode, uint64_t generation);

    /**
     * Remove specified event from the StateDB
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param id Event id
     */
    void delete_event(DbTxn *txn, uint64_t id);

    /**
     * Set the handles that are affected by this event
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param id Event id
     * @param handles array of handles that are affected by this event
     */
    void set_event_notification_handles(DbTxn *txn, uint64_t id,
                                        const std::vector<uint64_t> &handles);

    /**
     * Check if the specified event is in the StateDB
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param id Event id
     * @return true if found false ow
     */
    bool event_exists(DbTxn *txn, uint64_t id);

    /**
     * Persist a new SessionData object in the StateDB
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param addr Stringified remote host address
     * @param lease_interval Session lease interval
     * @param id Session id
     * @param name Name of remote executable
     */
    void create_session(DbTxn *txn, uint64_t id, const String &addr);

    /**
     * Delete info for specified session from StateDB
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param id Session id
     */
    void delete_session(DbTxn *txn, uint64_t id);

    /**
     * Delete info for specified session from StateDB
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param id Session id
     */
    void expire_session(DbTxn *txn, uint64_t id);


    /**
     * Store new handle opened by specified session
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param id Session id
     * @param handle_id Handle id
     */
    void add_session_handle(DbTxn *txn, uint64_t id, uint64_t handle_id);

    /**
     * return all the handles a session has open
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param id Session id
     * @param handles vector into which open handle ids will be inserted
     */
    void get_session_handles(DbTxn *txn, uint64_t id, vector<uint64_t> &handles);


    /**
     * Remove a handle from a session
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param id Session id
     * @param handle_id Handle id
     * @return true if handle was open and has been deleted
     */
    bool delete_session_handle(DbTxn *txn, uint64_t id, uint64_t handle_id);


    /**
     * Check if specified session exists in StateDB
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param id Session id
     * @return true if session existsn StateDB, false ow
     */
    bool session_exists(DbTxn *txn, uint64_t id);

    /**
     * Get name of session executable
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param id Session id
     * @return remote session executable
     */
    String get_session_name(DbTxn *txn, uint64_t id);

    /**
     * Set name of session executable
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param id Session id
     * @return name of the session executable
     */
    void set_session_name(DbTxn *txn, uint64_t id, const String &name);


    /**
     * Persist a new handle in StateDB
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param id Handle id
     * @param node_name Name of node associated with this handle
     * @param open_flags Open flags for this handle
     * @param event_mask Event notification mask
     * @param session_id Id of session associated with this handle
     * @param locked true if node is locked
     */
    void create_handle(DbTxn *txn, uint64_t id, String node_name,
        uint32_t open_flags, uint32_t event_mask, uint64_t session_id,
        bool locked);

    /**
     * Delete all info for this handle from StateDB
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param id Handle id
     */
    void delete_handle(DbTxn *txn, uint64_t id);

    /**
     * Set open flags for handle
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param id Handle id
     * @param open_flags new flags
     */
    void set_handle_open_flags(DbTxn *txn, uint64_t id, uint32_t open_flags);

    /**
     * Get open flags for handle
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param id Handle id
     * @return open_flags for handle
     */
    uint32_t get_handle_open_flags(DbTxn *txn, uint64_t id);


    /**
     * Set event mask for handle
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param id Handle id
     * @param event_mask new event mask
     */
    void set_handle_event_mask(DbTxn *txn, uint64_t id, uint32_t event_mask);

    /**
     * Get event mask for handle
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param id Handle id
     * @return event mask
     */
    uint32_t get_handle_event_mask(DbTxn *txn, uint64_t id);

    /**
     * Set the node associated with this handle
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param id Handle id
     * @param node_name name of node assoc with this handle
     */
    void set_handle_node(DbTxn *txn, uint64_t id, const String &node_name);

    /**
     * Get the node associated with this handle
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param id Handle id
     * @param node_name name of node assoc with this handle, if not found node_name = ""
     */
    void get_handle_node(DbTxn *txn, uint64_t id, String &node_name);

    /**
     * Get the session associated with this handle
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param id Handle id
     * @return session id for this handle
     */
    uint64_t get_handle_session(DbTxn *txn, uint64_t id);


    /**
     * Set the locked-ness this handle
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param id Handle id
     * @param locked
     */
    void set_handle_locked(DbTxn *txn, uint64_t id, bool locked);

    /**
     * Get the locked-ness this handle
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param id Handle id
     * @return true if handle is locked
     */
    bool handle_is_locked(DbTxn *txn, uint64_t id);

    /**
     * Check if info for this handle is in the StateDb
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param id Handle id
     * @return true if handle exists in StateDb
     */
    bool handle_exists(DbTxn *txn, uint64_t id);

    /**
     * Persist a new node in StateDB
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param name Node name
     * @param ephemeral true if node is ephemeral
     * @param lock_generation node lock generation
     * @param cur_lock_mode node lock mode
     * @param exclusive_handle handle id of exclusive lock handle
     */
    void create_node(DbTxn *txn, const String &name, bool ephemeral=false,
        uint64_t lock_generation=0, uint32_t cur_lock_mode=0, uint64_t exclusive_handle=0);

    /**
     * Set the lock generation this node
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param name Node name
     * @param lock_generation
     */
    void set_node_lock_generation(DbTxn *txn, const String &name, uint64_t lock_generation);

    /**
     * Increment the lock generation this node
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param name Node name
     * @return current lock generation (after increment)
     */
    uint64_t incr_node_lock_generation(DbTxn *txn, const String &name);


    /**
     * Set the node ephemeral-ness
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param name Node name
     * @param ephemeral
     */
    void set_node_ephemeral(DbTxn *txn, const String &name, bool ephemeral);

    /**
     * Set the node ephemeral-ness
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param name Node name
     * @return true if node is ephemeral
     */
    bool node_is_ephemeral(DbTxn *txn, const String &name);

    /**
     * Set the node current lock mode
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param name Node name
     * @param lock_mode
     */
    void set_node_cur_lock_mode(DbTxn *txn, const String &name, uint32_t lock_mode);

    /**
     * Get the node current lock mode
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param name Node name
     * @return lock_mode
     */
    uint32_t get_node_cur_lock_mode(DbTxn *txn, const String &name);

    /**
     * Set the node exclusive_lock_handle
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param name Node name
     * @param exclusive_lock_handle
     */
    void set_node_exclusive_lock_handle(DbTxn *txn, const String &name,
                                        uint64_t exclusive_lock_handle);

    /**
     * Get the node exclusive_lock_handle
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param name Node name
     * @param exclusive_lock_handle
     */
    uint64_t get_node_exclusive_lock_handle(DbTxn *txn, const String &name);


    /**
     * Add a handle to the node
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param name Node name
     * @param handle
     */
    void add_node_handle(DbTxn *txn, const String &name, uint64_t handle);

    /**
     * Remove a handle from the node
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param name Node name
     * @param handle
     */
    void delete_node_handle(DbTxn *txn, const String &name, uint64_t handle);

    /**
     * Returns whether any handles have this node open
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param name Node name
     * @return true if at least one handle has this node open
     */
    bool node_has_open_handles(DbTxn *txn, const String &name);

    /**
     * Check if a node has any pending lock requests from non-expired handles
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param name Node name
     * @return true if node has at least one pending lock request
     */
    bool node_has_pending_lock_request(DbTxn *txn, const String &name);

    /**
     * Check if a node has any pending lock requests from non-expired handles
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param name Node name
     * @param front_req will contain the first pending request if this method returns true
     * @return true if node has at least one pending lock request
     */
    bool get_node_pending_lock_request(DbTxn *txn, const String &name,
                                             LockRequest &front_req);


    /**
     * Add a lock request to the node
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param name Node name
     * @param handle handle requesting lock
     * @param mode requested lock mode
     */
    void add_node_pending_lock_request(DbTxn *txn, const String &name,
                                       uint64_t handle, uint32_t mode );
    /**
     * Remove a lock request to the node
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param name Node name
     * @param handle handle requesting lock
     */
    void delete_node_pending_lock_request(DbTxn *txn, const String &name, uint64_t handle);

    /**
     * Add a shared lock handle
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param name Node name
     * @param handle handle requesting lock
     */
    void add_node_shared_lock_handle(DbTxn *txn, const String &name, uint64_t handle);

    /**
     * Get a map of(handle id,  session id) for notifications registered for a certain
     * event occuring to a node
     *
     * @param txn BerkeleyDB txn
     * @param name Node name
     * @param event_mask indicates the event that is ocurring
     * @param handles_to_sessions map specifying notifications to be sent
     * @return true if there are some notifications that need to be sent out
     */
    bool get_node_event_notification_map(DbTxn *txn, const String &name, uint32_t event_mask,
        NotificationMap &handles_to_sessions);

    /**
     * Get all open handles for a node
     *
     * @param txn BerkeleyDB txn
     * @param name Node name
     * @param handles_to_sessions map specifying notifications to be sent
     * @return true if there are some notifications that need to be sent out
     */
    void get_node_handles(DbTxn *txn, const String &name, std::vector<uint64_t> &handles);

    /**
     * Remove node shared lock handle
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param name Node name
     * @param handle_id to be deleted
     */
    void delete_node_shared_lock_handle(DbTxn *txn, const String &name, uint64_t handle_id);

    /**
     * Delete all info for this node from StateDB
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param name Node name
     */
    void delete_node(DbTxn *txn, const String &name);

    /**
     * Check if info for this node is in the StateDb
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param name Node name
     * @return true if node exists in StateDb
     */
    bool node_exists(DbTxn *txn, const String &name);

    /**
     * Check if node has any shared lock handles
     *
     * @param txn BerkeleyDB txn for this DB update
     * @param name Node name
     * @return true if node has shared lock handles
     */
    bool node_has_shared_lock_handles(DbTxn *txn, const String &name);

    enum {
      SESSION_ID = 0,
      HANDLE_ID,
      EVENT_ID
    };

    uint64_t get_next_id_i64(DbTxn *txn, int id_type, bool increment = false);

    static const char NODE_ATTR_DELIM = 0x01;


  private:
    void build_attr_key(DbTxn *, String &keystr,
                        const String &aname, Dbt &key);

    String m_base_dir;
    DbEnv  m_env;
    Db    *m_db;
    Db    *m_state_db;
  };

} // namespace Hyperspace

#endif // HT_BERKELEYDBFILESYSTEM_H
