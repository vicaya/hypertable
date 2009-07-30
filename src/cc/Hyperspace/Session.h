/** -*- c++ -*-
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

#ifndef HYPERSPACE_SESSION_H
#define HYPERSPACE_SESSION_H

#include <vector>

#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>

#include "AsyncComm/Comm.h"
#include "AsyncComm/CommBuf.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/DispatchHandler.h"

#include "Common/DynamicBuffer.h"
#include "Common/ReferenceCount.h"
#include "Common/Timer.h"
#include "Common/Properties.h"
#include "Common/String.h"
#include "Common/HashMap.h"

#include "ClientKeepaliveHandler.h"
#include "HandleCallback.h"
#include "LockSequencer.h"
#include "Protocol.h"
#include "DirEntry.h"
#include "HsCommandInterpreter.h"

namespace Hyperspace {

  using namespace std;
  class HsCommandInterpreter;
  /**
   * The following flags (bit masks) are ORed together
   * and passed in as the flags argument to Open().
   * \anchor OpenFlags
   */
  enum {
    /** Open file for reading */
    OPEN_FLAG_READ           = 0x00001,
    /** Open file for writing */
    OPEN_FLAG_WRITE          = 0x00002,
    /** Open file for locking */
    OPEN_FLAG_LOCK           = 0x00004,
    /** Create file if it does not exist */
    OPEN_FLAG_CREATE         = 0x00008,
    /** Error if create and file exists */
    OPEN_FLAG_EXCL           = 0x00010,
    /** Used in conjunction with CREATE to create an ephemeral file */
    OPEN_FLAG_TEMP           = 0x00020,
    /** Atomically open and lock file shared, fail if can't */
    OPEN_FLAG_LOCK_SHARED    = 0x00044,
    /** atomically open and lock file exclusive, fail if can't */
    OPEN_FLAG_LOCK_EXCLUSIVE = 0x00084
  };

  /**
   * A callback object derived from this class gets
   * passed into the constructor of Hyperspace.  Session
   * state changes get reported to the application via
   * this callback.
   */
  class SessionCallback {
  public:
    SessionCallback() : m_id(0){}
    virtual ~SessionCallback() { return; }
    virtual void safe() = 0;
    virtual void expired() = 0;
    virtual void jeopardy() = 0;
    virtual void disconnected() = 0;
    virtual void reconnected() = 0;
    void set_id(uint32_t id) {m_id = id;}
    uint32_t get_id() {return m_id;}
  private:
    uint32_t m_id;
  };

  /*
   * %Hyperspace session.  Provides the API for %Hyperspace, a namespace and
   * lock service.  This service is modeled after
   * <a href="http://labs.google.com/papers/chubby.html">Chubby</a>.
   * Presently it is implemented as just a single server, but ultimately
   * it will get re-written using a distributed consensus protocol for high
   * availablility.  For now, it provides the same functionality (albeit less
   * available) and the same API.
   * This allows us to get the system up and running and since the API is the
   * same as the API for the highly-available version, minimal code changes
   * will be needed when we swap out this one for the highly available one.
   * <p>
   * %Session establishes a session with the master which includes a TCP
   * connection and the initiation of regular heartbeat UDP messages.  As soon
   * as the master receives the first heartbeat message from the client
   * %Session object, it creates the session and grants a lease. Each heartbeat
   * that the master receives from the client causes the master to extend the
   * lease.  In this mode of operation, the session is in the SAFE state.  If
   * the master does not receive a heartbeat message before the lease
   * expiration time, then the session transitions to the EXPIRED state and
   * the master drops the session.
   * <p>
   * Whenever the client receives a heartbeat UDP response message, it advances
   * its conservative estimate of what it thinks the lease expiration time is.
   * If the lease expires, then the client will put the session in the JEOPARDY
   * state and will continue sending heartbeats for a period of time known as
   * the 'grace period'. During the JEOPARDY state, all %Hyperspace commands
   * are suspended.  If a heartbeat response message is received during the
   * grace period, then it will switch back to SAFE mode and allow pending
   * commands to proceed.  Otherwise, the session expires.
   * <p>
   * The following set of properties are available to configure the protocol
   * (default values are shown):
   * <pre>
   * Hyperspace.Lease.Interval=2000
   * Hyperspace.KeepAlive.Interval=1000
   * Hyperspace.GracePeriod=6000
   * </pre>
   */
  class Session : public ReferenceCount {

  public:

    /** %Session state values
     * \anchor SessionState
     */
    enum State {
      /** session has expired */
      STATE_EXPIRED,
      /** session is in jeopardy */
      STATE_JEOPARDY,
      /** session is OK */
      STATE_SAFE,
      /** attempting to reconnect session */
      STATE_DISCONNECTED
    };

    /** Constructor.  Establishes a connection to %Hyperspace master and
     * initiates keepalive pings.  The location of the master is determined by
     * the following two properties of the config file:
     * <pre>
     * Hyperspace.Master.host
     * Hyperspace.Master.port
     * </pre>
     * The session callback is used to notify the application of session state
     * changes.
     *
     * @param comm pointer to the Comm object
     * @param props reference to config properties
     */
    Session(Comm *comm, PropertiesPtr &props);

    virtual ~Session();

    /**
     * Register a new session callback
     */
    void add_callback(SessionCallback *cb);

    /**
     * De-register session callback
     */
    bool remove_callback(SessionCallback *cb);

    /** Opens a file.  The open mode
     * is determined by the bits in the flags argument and the callback
     * argument is registered as the callback for this handle.  The events
     * that should be reported on this handle are determined by the event
     * mask inside callback (see HandleCallback#get_event_mask).
     *
     * @param name pathname of file to open
     * @param flags OR'ed together set of open flags (see \ref OpenFlags)
     * @param callback smart pointer to handle callback
     * @param timer maximum wait timer
     * @return opened file handle
     */
    uint64_t open(const std::string &name, uint32_t flags,
                  HandleCallbackPtr &callback, Timer *timer = 0);

    /** Creates a file.  This method is basically the same as the #open method
     * except that it implicitly sets the OPEN_FLAG_CREATE and OPEN_FLAG_EXCL
     * open flags and supplies a set of initial attributes to be set when the
     * file is created.  The flags argument controls other open modes and the
     * callback argument is registered as the callback for this handle.  The
     * events that should be reported on this handle are determined by the
     * event mask inside callback (see HandleCallback#get_event_mask).
     *
     * @param name pathname of file to create
     * @param flags OR'ed together set of open flags (see \ref OpenFlags)
     * @param callback smart pointer to handle callback
     * @param init_attrs vector of attributes to be atomically set when the
     *        file is created
     * @param timer maximum wait timer
     * @return Error::OK on success or error code on failure
     */
    uint64_t create(const std::string &name, uint32_t flags,
                    HandleCallbackPtr &callback,
                    std::vector<Attribute> &init_attrs, Timer *timer=0);

    /*
    int cancel(uint64_t handle);
    int poison(uint64_t handle);
    */

    /** Closes a file handle.
     *
     * @param handle file handle to close
     * @param timer maximum wait timer
     */
    void close(uint64_t handle, Timer *timer=0);

    /**
     * Creates a directory.  The name
     * argument should be the absolute path to the file.  All of the directories
     * up to, but not including, the last path component must be valid.
     * Otherwise, Error::HYPERSPACE_BAD_PATHNAME will be returned.
     *
     * @param name absolute pathname of directory to create
     * @param timer maximum wait timer
     */
    void mkdir(const std::string &name, Timer *timer=0);

    /** Sets an extended attribute of a file.
     *
     * @param handle file handle
     * @param name name of extended attribute
     * @param value pointer to new value
     * @param value_len length of new value
     * @param timer maximum wait timer
     */
    void attr_set(uint64_t handle, const std::string &name,
                  const void *value, size_t value_len, Timer *timer=0);

    /** Lists all extended attributes of a file.
     *
     * @param handle file handle
     * @param anames vector of atribute names
     */
    void attr_list(uint64_t handle, vector<String> &anames,Timer *timer=0);

    bool attr_exists(uint64_t handle, const std::string& name,Timer *timer=0);

    /** Gets an extended attribute of a file.  A '\0' character is written
     * just past the end of the value, but not included in the value size.
     * If the value is a character string, it can be accessed easily by
     * simply casting the base pointer:  (const char *)value.base
     *
     * @param handle file handle
     * @param name name of extended attribute
     * @param value reference to DynamicBuffer to hold returned value
     * @param timer maximum wait timer
     */
    void attr_get(uint64_t handle, const std::string &name,
                  DynamicBuffer &value, Timer *timer=0);

    /** Deletes an extended attribute of a file.
     *
     * @param handle file handle
     * @param name name of extended attribute
     * @param timer maximum wait timer
     */
    void attr_del(uint64_t handle, const std::string &name, Timer *timer=0);

    /** Checks for the existence of a file.
     *
     * @param name absolute name of file or directory to check for
     * @param timer maximum wait timer
     * @return true if exists, false otherwise
     */
    bool exists(const std::string &name, Timer *timer=0);

    /** Removes a file or directory.  Directory must be empty, otherwise
     * Error::HYPERSPACE_IO_ERROR will be returned.
     *
     * @param name absolute path name of file or directory to delete
     * @param timer maximum wait timer
     */
    void unlink(const std::string &name, Timer *timer=0);

    /** Gets a directory listing.  The listing comes back as a vector of
     * DireEntry which contains a name and boolean flag indicating if the
     * entry is an element or not.
     *
     * @param handle handle of directory to scan
     * @param listing reference to vector of DirEntry structures to hold result
     * @param timer maximum wait timer
     */
    void readdir(uint64_t handle, std::vector<DirEntry> &listing,
                 Timer *timer=0);


    /** Locks a file.  The mode argument indicates the type of lock to be
     * acquired and takes a value of either LOCK_MODE_SHARED
     * or LOCK_MODE_EXCLUSIVE (see \ref LockMode).  Upon success, the structure
     * pointed to by sequencerp will get filled in with information about the
     * lock, including a generation number.  Some services operate on resources
     * on behalf of clients, but require that the client have the resource
     * locked. The LockSequencer object can be passed by the client to the
     * service in each request so that the service can call check_sequencer
     * (not yet implemented) to verify that the client indeed has the current
     * up-to-date lock.
     *
     * @param handle handle of file or directory to lock
     * @param mode lock mode (see \ref LockMode)
     * @param sequencerp address of LockSequencer return structure
     * @param timer maximum wait timer
     */
    void lock(uint64_t handle, uint32_t mode,
              LockSequencer *sequencerp, Timer *timer=0);

    /** Attempts to lock a file.  The mode argument indicates the type of lock
     * to be acquired and takes a value of either LOCK_MODE_SHARED or
     * LOCK_MODE_EXCLUSIVE (see \ref LockMode).  The result of the attempt will
     * get returned in the statusp argument and will contain either
     * LOCK_STATUS_BUSY or LOCK_STATUS_GRANTED.  Upon success, the structure
     * pointed to by sequencerp will get filled in with information about the
     * lock, including a generation number.  Some services operate on resources
     * on behalf of clients, but require that the client have the resource
     * locked.  The LockSequencer object can be passed by the client to the
     * service in each request so that the service can call check_sequencer
     * (not yet implemented) to verify that the client indeed has the current
     * up-to-date lock.
     *
     * @param handle handle of file or directory to lock
     * @param mode lock mode (see \ref LockMode)
     * @param statusp address of variable to hold the status of the attempt
     *        (see \ref LockStatus)
     * @param sequencerp address of LockSequencer return structure
     * @param timer maximum wait timer
     */
    void try_lock(uint64_t handle, uint32_t mode, uint32_t *statusp,
                  LockSequencer *sequencerp, Timer *timer=0);

    /** Releases any file handle locks.
     *
     * @param handle locked file or directory handle
     * @param timer maximum wait timer
     */
    void release(uint64_t handle, Timer *timer=0);

    /** Gets the lock sequencer of a locked file or directory handle.
     *
     * @param handle locked file or directory handle
     * @param sequencerp address of LockSequencer return structure
     * @param timer maximum wait timer
     */
    void get_sequencer(uint64_t handle, LockSequencer *sequencerp,
                       Timer *timer=0);

    /** Checks to see if a lock sequencer is valid.
     *
     * <b>NOTE: This method is not yet implemented and always returns
     * Error::OK</b>
     *
     * @param sequencer lock sequencer to validate
     * @param timer maximum wait timer
     */
    void check_sequencer(LockSequencer &sequencer, Timer *timer=0);

    /** Check the status of the Hyperspace master server
     *
     * @param timer maximum wait timer
     * @return Error::OK on if server is up and ok or error code on failure
     */
    int status(Timer *timer=0);

    /** Waits for session state to change to STATE_SAFE.
     *
     * @param max_wait_ms maximum milliseconds to wait for connection
     * @return true if connected, false otherwise
     */
    bool wait_for_connection(uint32_t max_wait_ms);

    /** Waits for session state to change to STATE_SAFE.
     *
     * @param timer maximum wait timer
     * @return true if connected, false otherwise
     */
    bool wait_for_connection(Timer &timer);

    /** Sets verbose flag.  Turns on or off (default) verbose logging
     *
     * @param verbose value of verbose flag
     */
    void set_verbose_flag(bool verbose) { m_verbose = verbose; }

    /** Transions state (internal method)
     *
     * @param state new state (see \ref SessionState)
     * @return old state (see \ref SessionState)
     */
    int state_transition(int state);

    /** Returns current state (internal method)
     *
     * @return current state (see \ref SessionState)
     */
    int get_state();

    /**
     * Checks for session expiration (internal method)
     *
     * @return true if expired, false otherwise
     */
    bool expired();

    /**
     * Creates a new Hyperspace command interpreter
     *
     * @return HsCommandInterpreter* ptr to created Hperspace interpreter object
     */
     HsCommandInterpreter* create_hs_interpreter();

    void advance_expire_time(boost::xtime now) {
      ScopedLock lock(m_mutex);
      m_expire_time = now;
      xtime_add_millis(m_expire_time, m_lease_interval);
    }

  private:

    typedef hash_map<uint64_t, SessionCallback *> CallbackMap;

    bool wait_for_safe();
    int send_message(CommBufPtr &, DispatchHandler *, Timer *timer);
    void normalize_name(const std::string &name, std::string &normal);
    uint64_t open(ClientHandleStatePtr &, CommBufPtr &, Timer *timer);

    Mutex                     m_mutex;
    boost::condition          m_cond;
    Comm                      *m_comm;
    PropertiesPtr             m_cfg;
    bool                      m_verbose;
    bool                      m_silent;
    bool                      m_reconnect;
    int                       m_state;
    uint32_t                  m_grace_period;
    uint32_t                  m_lease_interval;
    uint32_t                  m_timeout_ms;
    boost::xtime              m_expire_time;
    InetAddr                  m_master_addr;
    ClientKeepaliveHandlerPtr m_keepalive_handler_ptr;
    CallbackMap               m_callbacks;
    uint64_t                  m_last_callback_id;
    Mutex                     m_callback_mutex;
  };

  typedef boost::intrusive_ptr<Session> SessionPtr;

} // namespace Hyperspace

#endif // HYPERSPACE_SESSION_H
