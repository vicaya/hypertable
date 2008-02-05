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

#include "ClientKeepaliveHandler.h"
#include "HandleCallback.h"
#include "LockSequencer.h"
#include "Protocol.h"

using namespace Hypertable;
using namespace Hyperspace;

namespace Hyperspace {

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
    virtual ~SessionCallback() { return; }
    virtual void safe() = 0;
    virtual void expired() = 0;
    virtual void jeopardy() = 0;
  };

  /**
   * %Hyperspace session.  Provides the API for %Hyperspace, a namespace and
   * lock service.  This service is modeled after <a href="http://labs.google.com/papers/chubby.html">
   * Chubby</a>.  Presently it is implemented as just a single server, but ultimately
   * it will get re-written using a distributed consensus protocol for high availablility.
   * For now, it provides the same functionality (albeit less available) and the same API.
   * This allows us to get the system up and running and since the API is the same as the
   * API for the highly-available version, minimal code changes will be needed when we
   * swap out this one for the highly available one.
   * <p>
   * %Session establishes a session with the master which includes a TCP connection and
   * the initiation of regular heartbeat UDP messages.  As soon as the master receives the
   * first heartbeat message from the client %Session object, it creates the session
   * and grants a lease.  Each heartbeat that the master receives from the client
   * causes the master to extend the lease.  In this mode of operation, the session 
   * is in the SAFE state.  If the master does not receive a heartbeat message before
   * the lease expiration time, then the session transitions to the EXPIRED state and
   * the master drops the session.
   * <p>
   * Whenever the client receives a heartbeat UDP response message, it advances its
   * conservative estimate of what it thinks the lease expiration time is.  If the
   * lease expires, then the client will put the session in the JEOPARDY state and will
   * continue sending heartbeats for a period of time known as the 'grace period'.
   * During the JEOPARDY state, all %Hyperspace commands are suspended.  If
   * a heartbeat response message is received during the grace period, then it will switch
   * back to SAFE mode and allow pending commands to proceed.  Otherwise, the session expires.
   * <p>
   * The following set of properties are available to configure the protocol (default
   * values are shown):
   * <pre>
   * Hyperspace.Lease.Interval=20
   * Hyperspace.KeepAlive.Interval=10
   * Hyperspace.GracePeriod=60
   * </pre>
   */
  class Session : public ReferenceCount {

  public:

    /** %Session state values
     * \anchor SessionState
     */
    enum {
      /** session has expired */
      STATE_EXPIRED,
      /** session is in jeopardy */
      STATE_JEOPARDY,
      /** session is OK */
      STATE_SAFE
    };

    /** Constructor.  Establishes a connection to %Hyperspace master and initiates keepalive
     * pings.  The location of the master is determined by the following two properties of
     * the config file:
     * <pre>
     * Hyperspace.Master.host
     * Hyperspace.Master.port
     * </pre>
     * The session callback is used to notify the application of session state changes.
     *
     * @param comm pointer to the Comm object
     * @param props_ptr smart pointer to properties object
     * @param callback session state callback
     */
    Session(Comm *comm, PropertiesPtr &props_ptr, SessionCallback *callback=0);

    virtual ~Session();

    /** Opens a file.  The open mode
     * is determined by the bits in the flags argument and the callbackPtr
     * argument is registered as the callback for this handle.  The events
     * that should be reported on this handle are determined by the event
     * mask inside callbackPtr (see HandleCallback#get_event_mask).
     *
     * @param name pathname of file to open
     * @param flags OR'ed together set of open flags (see \ref OpenFlags)
     * @param callbackPtr smart pointer to handle callback
     * @param handlep address of variable to hold returned handle
     * @return Error::OK on success or error code on failure
     */
    int open(std::string name, uint32_t flags, HandleCallbackPtr &callbackPtr, uint64_t *handlep);

    /** Creates a file.  This method is basically
     * the same as the #open method except that it implicitly sets the 
     * OPEN_FLAG_CREATE and OPEN_FLAG_EXCL open flags and supplies a set of
     * initial attributes to be set when the file is created.  The flags argument
     * controls other open modes and the callbackPtr
     * argument is registered as the callback for this handle.  The events
     * that should be reported on this handle are determined by the event
     * mask inside callbackPtr (see HandleCallback#get_event_mask).
     *
     * @param name pathname of file to create
     * @param flags OR'ed together set of open flags (see \ref OpenFlags)
     * @param callbackPtr smart pointer to handle callback
     * @param initAttrs vector of attributes to be atomically set when the file is created
     * @param handlep address of variable to hold returned handle
     * @return Error::OK on success or error code on failure
     */
    int create(std::string name, uint32_t flags, HandleCallbackPtr &callbackPtr, std::vector<AttributeT> &initAttrs, uint64_t *handlep);

    /*
    int cancel(uint64_t handle);
    int poison(uint64_t handle);
    */

    /** Closes a file handle.
     *
     * @param handle file handle to close
     */
    int close(uint64_t handle);

    /**
     * Creates a directory.  The name
     * argument should be the absolute path to the file.  All of the directories
     * up to, but not including, the last path component must be valid.  Otherwise,
     * Error::HYPERSPACE_BAD_PATHNAME will be returned.
     *
     * @param name absolute pathname of directory to create
     * @return Error::OK on success or error code on failure
     */
    int mkdir(std::string name);

    /** Sets an extended attribute of a file.
     *
     * @param handle file handle
     * @param name name of extended attribute
     * @param value pointer to new value
     * @param value_len length of new value
     * @return Error::OK on success or error code on failure
     */
    int attr_set(uint64_t handle, std::string name, const void *value, size_t value_len);

    /** Gets an extended attribute of a file.
     *
     * @param handle file handle
     * @param name name of extended attribute
     * @param value reference to DynamicBuffer to hold returned value
     * @return Error::OK on success or error code on failure
     */
    int attr_get(uint64_t handle, std::string name, DynamicBuffer &value);

    /** Deletes an extended attribute of a file.
     *
     * @param handle file handle
     * @param name name of extended attribute
     * @return Error::OK on success or error code on failure
     */
    int attr_del(uint64_t handle, std::string name);

    /** Checks for the existence of a file.
     *
     * @param name absolute name of file or directory to check for
     * @param existsp address of boolean variable to hold result
     * @return Error::OK on success or error code on failure
     */
    int exists(std::string name, bool *existsp);

    /** Removes a file or directory.  Directory must be empty, otherwise 
     * Error::HYPERSPACE_IO_ERROR will be returned.
     *
     * @param name absolute path name of file or directory to delete
     * @return Error::OK on success or error code on failure
     */
    int unlink(std::string name);

    /** Gets a directory listing.  The listing comes back as a vector of DireEntryT
     * structures which contains a name and boolean flag indicating if the entry is an
     * element or not.
     * 
     * @param handle handle of directory to scan
     * @param listing reference to vector of DirEntryT structures to hold result
     * @return Error::OK on success or error code on failure
     */
    int readdir(uint64_t handle, std::vector<struct DirEntryT> &listing);


    /** Locks a file.  The mode argument indicates
     * the type of lock to be acquired and takes a value of either LOCK_MODE_SHARED
     * or LOCK_MODE_EXCLUSIVE (see \ref LockMode).  Upon success, the structure
     * pointed to by sequencerp will get filled in with information about the
     * lock, including a generation number.  Some services operate on resources
     * on behalf of clients, but require that the client have the resource locked.
     * The LockSequencerT object can be passed by the client to the service in
     * each request so that the service can call check_sequencer (not yet implemented)
     * to verify that the client indeed has the current up-to-date lock.
     *
     * @param handle handle of file or directory to lock
     * @param mode lock mode (see \ref LockMode)
     * @param sequencerp address of LockSequencerT return structure
     * @return Error::OK on success or error code on failure     
     */
    int lock(uint64_t handle, uint32_t mode, struct LockSequencerT *sequencerp);

    /** Attempts to lock a file.  The mode argument
     * indicates the type of lock to be acquired and takes a value of either LOCK_MODE_SHARED
     * or LOCK_MODE_EXCLUSIVE (see \ref LockMode).  The result of the attempt
     * will get returned in the statusp argument and will contain either
     * LOCK_STATUS_BUSY or LOCK_STATUS_GRANTED.  Upon success, the structure
     * pointed to by sequencerp will get filled in with information about the
     * lock, including a generation number.  Some services operate on resources
     * on behalf of clients, but require that the client have the resource locked.
     * The LockSequencerT object can be passed by the client to the service in
     * each request so that the service can call #check_sequencer (not yet implemented)
     * to verify that the client indeed has the current up-to-date lock.
     *
     * @param handle handle of file or directory to lock
     * @param mode lock mode (see \ref LockMode)
     * @param statusp address of variable to hold the status of the attempt (see \ref LockStatus)
     * @param sequencerp address of LockSequencerT return structure
     * @return Error::OK on success or error code on failure     
     */
    int try_lock(uint64_t handle, uint32_t mode, uint32_t *statusp, struct LockSequencerT *sequencerp);

    /** Releases any file handle locks.
     *
     * @param handle locked file or directory handle
     * @return Error::OK on success or error code on failure
     */
    int release(uint64_t handle);

    /** Gets the lock sequencer of a locked file or directory handle.
     *
     * @param handle locked file or directory handle
     * @param sequencerp address of LockSequencerT return structure
     * @return Error::OK on success or error code on failure
     */
    int get_sequencer(uint64_t handle, struct LockSequencerT *sequencerp);

    /** Checks to see if a lock sequencer is valid.
     *
     * <b>NOTE: This method is not yet implemented and always returns Error::OK</b>
     *
     * @param sequencer lock sequencer to validate
     * @return Error::OK on success or error code on failure
     */
    int check_sequencer(struct LockSequencerT &sequencer);

    /** Check the status of the Hyperspace master server
     *
     * @return Error::OK on if server is up and ok or error code on failure
     */
    int status();

    /** Waits for session state to change to STATE_SAFE.
     *
     * @param max_wait_secs maximum seconds to wait for connection
     * @return true if connected, false otherwise
     */
    bool wait_for_connection(long max_wait_secs);

    /** Sets silent flag.  Turns on or off log output
     *
     * @param silent value of silent flag
     */
    void set_silent_flag(bool silent) { m_silent = silent; }

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

  private:

    bool wait_for_safe();
    int send_message(CommBufPtr &cbufPtr, DispatchHandler *handler);
    void normalize_name(std::string name, std::string &normal);
    int open(ClientHandleStatePtr &handleStatePtr, CommBufPtr &cbufPtr, uint64_t *handlep);

    boost::mutex m_mutex;
    boost::condition m_cond;
    Comm *m_comm;
    bool m_verbose;
    bool m_silent;
    int  m_state;
    uint32_t m_grace_period;
    uint32_t m_lease_interval;
    uint32_t m_timeout;
    boost::xtime m_expire_time;
    struct sockaddr_in m_master_addr;
    ClientKeepaliveHandlerPtr m_keepalive_handler_ptr;
    SessionCallback *m_session_callback;
  };
  typedef boost::intrusive_ptr<Session> SessionPtr;
  

}


#endif // HYPERSPACE_SESSION_H

