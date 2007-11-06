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

using namespace hypertable;
using namespace Hyperspace;

namespace Hyperspace {

  /**
   * The following flags (bit masks) are ORed together
   * and passed in as the flags argument to Open().
   */
  enum {
    OPEN_FLAG_READ           = 0x00001, // open file for reading
    OPEN_FLAG_WRITE          = 0x00002, // open file for writing (modifications)
    OPEN_FLAG_LOCK           = 0x00004, // open file for locking
    OPEN_FLAG_CREATE         = 0x00008, // create file if it does not exist
    OPEN_FLAG_EXCL           = 0x00010, // error if create and file exists
    OPEN_FLAG_TEMP           = 0x00020, // used in conjunction with CREATE to create an ephemeral file
    OPEN_FLAG_LOCK_SHARED    = 0x00044, // atomically open and lock file shared, fail if can't
    OPEN_FLAG_LOCK_EXCLUSIVE = 0x00084  // atomically open and lock file exclusive, fail if can't
  };

  /**
   * A callback object derived from this class gets
   * passed into the constructor of Hyperspace.  Session
   * state changes get reported to the application via
   * this callback.
   */
  class SessionCallback {
  public:
    virtual void safe() = 0;
    virtual void expired() = 0;
    virtual void jeopardy() = 0;
  };

  /**
   * This class encapsulates a Hyperspace session and
   * provides the Hyperspace API.
   */
  class Session : public ReferenceCount {

  public:

    enum { STATE_EXPIRED, STATE_JEOPARDY, STATE_SAFE };

    Session(Comm *comm, PropertiesPtr &propsPtr, SessionCallback *callback=0);
    virtual ~Session();

    int open(std::string name, uint32_t flags, HandleCallbackPtr &callbackPtr, uint64_t *handlep);
    int create(std::string name, uint32_t flags, HandleCallbackPtr &callbackPtr, std::vector<AttributeT> &initAttrs, uint64_t *handlep);
    int cancel(uint64_t handle);
    int close(uint64_t handle);
    int poison(uint64_t handle);
    int mkdir(std::string name);
    int attr_set(uint64_t handle, std::string name, const void *value, size_t valueLen);
    int attr_get(uint64_t handle, std::string name, DynamicBuffer &value);
    int attr_del(uint64_t handle, std::string name);
    int exists(std::string name, bool *existsp);
    int unlink(std::string name);
    int readdir(uint64_t handle, vector<struct DirEntryT> &listing);
    int lock(uint64_t handle, uint32_t mode, struct LockSequencerT *sequencerp);
    int try_lock(uint64_t handle, uint32_t mode, uint32_t *statusp, struct LockSequencerT *sequencerp);
    int release(uint64_t handle);
    int get_sequencer(uint64_t handle, struct LockSequencerT *sequencerp);
    int check_sequencer(struct LockSequencerT &sequencer);
    int status();

    // Session state methods
    int state_transition(int state);
    int get_state();
    bool expired();
    bool wait_for_connection(long maxWaitSecs);
    void set_verbose_flag(bool verbose) { m_verbose = verbose; }

  private:

    bool wait_for_safe();
    int send_message(CommBufPtr &cbufPtr, DispatchHandler *handler);
    void normalize_name(std::string name, std::string &normal);
    int open(ClientHandleStatePtr &handleStatePtr, CommBufPtr &cbufPtr, uint64_t *handlep);

    boost::mutex       m_mutex;
    boost::condition   m_cond;
    Comm *m_comm;
    bool m_verbose;
    int  m_state;
    uint32_t m_grace_period;
    uint32_t m_lease_interval;
    uint32_t m_timeout;
    boost::xtime m_expire_time;
    struct sockaddr_in m_master_addr;
    ClientKeepaliveHandlerPtr  m_keepalive_handler_ptr;
    SessionCallback           *m_session_callback;
  };
  typedef boost::intrusive_ptr<Session> SessionPtr;
  

}


#endif // HYPERSPACE_SESSION_H

