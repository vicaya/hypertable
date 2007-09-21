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

#include "ClientKeepaliveHandler.h"
#include "ClientSessionState.h"
#include "Protocol.h"

using namespace hypertable;
using namespace Hyperspace;

namespace Hyperspace {

  /**
   * The following flags (bit masks) are ORed together
   * and passed in as the flags argument to Open().
   */
  enum {
    OPEN_FLAG_READ   = 0x00001, // open file for reading                                                                                                 
    OPEN_FLAG_WRITE  = 0x00002, // open file for writing (modifications)                                                                                 
    OPEN_FLAG_LOCK   = 0x00004, // open file for locking                                                                                                 
    OPEN_FLAG_CREATE = 0x00008, // create file if it does not exist                                                                                      
    OPEN_FLAG_TEMP   = 0x00010  // used in conjunction with CREATE to create an ephemeral file
  };

  /**
   * The following event masks are ORed together and
   * passed in as the eventMask argument to Open()
   * to indicate which events should be reported to
   * the application for the opened handle.
   */
  enum {
    EVENT_MASK_ATTR_MODIFIED      = 0x0001,
    EVENT_MASK_CHILD_NODE_CHANGE  = 0x0002,
    EVENT_MASK_LOCK_ACQUIRED      = 0x0004,
    EVENT_MASK_MASTER_FAILOVER    = 0x0008,
    EVENT_MASK_HANDLE_INVALIDATED = 0x0010,
    EVENT_MASK_CONFLICTING_LOCK   = 0x0020,
    EVENT_MASK_REQUEST_COMPLETE   = 0x0040,
    EVENT_MASK_REQUEST_ERROR      = 0x0080
  };

  /**
   * Node metadata
   */
  struct NodeMetadataT {
    uint64_t generation;
    uint64_t lockGeneration;
    uint64_t mtime;
  };

  /**
   * Listing information for each node within a
   * directory.  A vector of these objects gets
   * passed back to the application via a call to
   * Readdir()
   */
  struct DirListingT {
    std::string   name;
    struct NodeMetadataT metadata;
  };


  /**
   * Lock sequencer.  This object gets created with
   * each lock acquisition and gets passed to each
   * service that expects to be protected by the
   * lock.  The service will check the validity
   * of this sequencer with a call to CheckSequencer
   * and will reject requests if the sequencer is no
   * longer valid.
   */
  struct LockSequencerT {
    std::string name;
    uint32_t mode;
    uint32_t generation;
  };

  /**
   * A callback object derived from this class gets
   * passed into the constructor of Hyperspace.  Session
   * state changes get reported to the application via
   * this callback.
   */
  class SessionCallback {
  public:
    virtual void Safe() = 0;
    virtual void Expired() = 0;
    virtual void Jeopardy() = 0;
  };


  /**
   * A callback object derived from this class gets passed
   * into each Open() call.  Node state changes get reported
   * to the application via this callback.
   */
  class HandleCallback {
  public:
    HandleCallback(int eventMask) : mEventMask(eventMask) { return; }
    virtual void AttrModified(std::string name) = 0;
    virtual void ChildNodeChange() = 0;
    virtual void LockAcquired() = 0;
    virtual void MasterFailover() = 0;
    virtual void HandleInvalidated() = 0;
    virtual void ConflictingLockRequest() = 0;
    int GetEventMask() { return mEventMask; }
  protected:
    int mEventMask;
  };


  /**
   * This class encapsulates a Hyperspace session and
   * provides the Hyperspace API.
   */
  class Session {

  public:

    Session(Comm *comm, PropertiesPtr &propsPtr, SessionCallback *callback);

    int Open(std::string name, int flags, HandleCallback *callback, uint64_t *handlep);
    int Cancel(uint64_t handle);
    int Close(uint64_t handle);
    int Poison(uint64_t handle);
    int Mkdir(std::string name);
    int AttrSet(uint64_t handle, std::string name, uint8_t *value, size_t valueLen);
    int AttrGet(uint64_t handle, std::string name, DynamicBuffer &value);
    int AttrDel(uint64_t handle, std::string name);
    int Exists(std::string name);
    int Delete(std::string name);
    int Readdir(uint64_t handle, std::string name, std::vector<struct DirListingT> &listing);
    int Acquire(uint64_t handle, int mode, struct LockSequencerT &sequencer);
    int TryAcquire(uint64_t handle, int mode, struct LockSequencerT &sequencer);
    int Release(uint64_t handle);
    int CheckSequencer(struct LockSequencerT &sequencer);
    int Status();

    bool WaitForConnection(long maxWaitSecs);

    static const uint32_t DEFAULT_CLIENT_PORT = 38550;

  private:

    int SendMessage(CommBufPtr &cbufPtr, DispatchHandler *handler);

    boost::mutex       mMutex;
    boost::condition   mCond;
    Comm *mComm;
    bool mVerbose;
    struct sockaddr_in mMasterAddr;
    ClientKeepaliveHandler *mKeepaliveHandler;
    ClientSessionStatePtr   mSessionStatePtr;
    SessionCallback        *mSessionCallback;
  };

}

#if 0

/**
 * Forward declarations
 */
namespace hypertable {
  class DispatchHandler;
  class Comm;
  class CommBuf;
  class Properties;
}
namespace boost {
  class thread;
}

using namespace hypertable;

namespace hypertable {

  class HyperspaceClient {
  public:

    HyperspaceClient(ConnectionManager *connManager, struct sockaddr_in &addr, time_t timeout);

    HyperspaceClient(ConnectionManager *connManager, Properties *props);

    bool WaitForConnection(long maxWaitSecs);

    int Mkdirs(const char *name, DispatchHandler *handler, uint32_t *msgIdp);
    int Mkdirs(const char *name);

    int Create(const char *name, DispatchHandler *handler, uint32_t *msgIdp);
    int Create(const char *name);

    int AttrSet(const char *fname, const char *aname, const char *avalue, DispatchHandler *handler, uint32_t *msgIdp);
    int AttrSet(const char *fname, const char *aname, const char *avalue);

    int AttrGet(const char *fname, const char *aname, DispatchHandler *handler, uint32_t *msgIdp);
    int AttrGet(const char *fname, const char *aname, DynamicBuffer &out);

    int AttrDel(const char *fname, const char *aname, DispatchHandler *handler, uint32_t *msgIdp);
    int AttrDel(const char *fname, const char *aname);

    int Exists(const char *name, DispatchHandler *handler, uint32_t *msgIdp);
    int Exists(const char *name);

    int Delete(const char *name, DispatchHandler *handler, uint32_t *msgIdp);
    int Delete(const char *name);

    int Status();

    HyperspaceProtocol *Protocol() { return mProtocol; }
    
  private:

    int SendMessage(CommBufPtr &cbufPtr, DispatchHandler *handler, uint32_t *msgIdp=0);

    Comm                 *mComm;
    ConnectionManager    *mConnManager;
    struct sockaddr_in    mAddr;
    time_t                mTimeout;
  };
}

#endif


#endif // HYPERSPACE_SESSION_H

