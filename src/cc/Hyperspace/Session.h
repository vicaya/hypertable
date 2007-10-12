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
    virtual void Safe() = 0;
    virtual void Expired() = 0;
    virtual void Jeopardy() = 0;
  };

  /**
   * This class encapsulates a Hyperspace session and
   * provides the Hyperspace API.
   */
  class Session {

  public:

    enum { STATE_EXPIRED, STATE_JEOPARDY, STATE_SAFE };

    Session(Comm *comm, PropertiesPtr &propsPtr, SessionCallback *callback);

    int Open(std::string name, uint32_t flags, HandleCallbackPtr &callbackPtr, uint64_t *handlep);
    int Create(std::string name, uint32_t flags, HandleCallbackPtr &callbackPtr, std::vector<AttributeT> &initAttrs, uint64_t *handlep);
    int Cancel(uint64_t handle);
    int Close(uint64_t handle);
    int Poison(uint64_t handle);
    int Mkdir(std::string name);
    int AttrSet(uint64_t handle, std::string name, const void *value, size_t valueLen);
    int AttrGet(uint64_t handle, std::string name, DynamicBuffer &value);
    int AttrDel(uint64_t handle, std::string name);
    int Exists(std::string name, bool *existsp);
    int Delete(std::string name);
    int Readdir(uint64_t handle, vector<struct DirEntryT> &listing);
    int Lock(uint64_t handle, uint32_t mode, struct LockSequencerT *sequencerp);
    int TryLock(uint64_t handle, uint32_t mode, uint32_t *statusp, struct LockSequencerT *sequencerp);
    int Release(uint64_t handle);
    int GetSequencer(uint64_t handle, struct LockSequencerT *sequencerp);
    int CheckSequencer(struct LockSequencerT &sequencer);
    int Status();

    // Session state methods
    int StateTransition(int state);
    int GetState();
    bool Expired();
    bool WaitForConnection(long maxWaitSecs);

    static const uint32_t DEFAULT_CLIENT_PORT = 38550;

  private:

    bool WaitForSafe();
    int SendMessage(CommBufPtr &cbufPtr, DispatchHandler *handler);
    void NormalizeName(std::string name, std::string &normal);
    int Open(ClientHandleStatePtr &handleStatePtr, CommBufPtr &cbufPtr, uint64_t *handlep);

    boost::mutex       mMutex;
    boost::condition   mCond;
    Comm *mComm;
    bool mVerbose;
    int  mState;
    uint32_t mGracePeriod;
    boost::xtime mExpireTime;
    struct sockaddr_in mMasterAddr;
    ClientKeepaliveHandler *mKeepaliveHandler;
    SessionCallback        *mSessionCallback;
  };

}


#endif // HYPERSPACE_SESSION_H

