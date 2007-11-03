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

#ifndef HYPERSPACE_CLIENTKEEPALIVEHANDLER_H
#define HYPERSPACE_CLIENTKEEPALIVEHANDLER_H

#include <cassert>
#include <ext/hash_map>

#include <boost/thread/mutex.hpp>

#include "Common/Properties.h"
#include "Common/StringExt.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/DispatchHandler.h"

#include "ClientConnectionHandler.h"
#include "ClientHandleState.h"
#include "HandleCallback.h"

namespace Hyperspace {

  class Session;

  /**
   * 
   */
  class ClientKeepaliveHandler : public DispatchHandler {

  public:
    ClientKeepaliveHandler(Comm *comm, PropertiesPtr &propsPtr, Session *session);
    virtual ~ClientKeepaliveHandler();

    virtual void handle(hypertable::EventPtr &eventPtr);

    void RegisterHandle(ClientHandleStatePtr &handleStatePtr) {
      boost::mutex::scoped_lock lock(mMutex);
      HandleMapT::iterator iter = mHandleMap.find(handleStatePtr->handle);
      assert(iter == mHandleMap.end());
      mHandleMap[handleStatePtr->handle] = handleStatePtr;
    }

    void UnregisterHandle(uint64_t handle) {
      boost::mutex::scoped_lock lock(mMutex);
      mHandleMap.erase(handle);
    }

    bool GetHandleState(uint64_t handle, ClientHandleStatePtr &handleStatePtr) {
      boost::mutex::scoped_lock lock(mMutex);
      HandleMapT::iterator iter = mHandleMap.find(handle);
      if (iter == mHandleMap.end())
	return false;
      handleStatePtr = (*iter).second;
      return true;
    }

    void ExpireSession();

  private:
    boost::mutex       mMutex;
    boost::xtime       mLastKeepAliveSendTime;
    boost::xtime       mJeopardyTime;
    Comm *mComm;
    uint32_t mLeaseInterval;
    uint32_t mKeepAliveInterval;
    struct sockaddr_in mMasterAddr;
    struct sockaddr_in mLocalAddr;
    bool mVerbose;
    Session *mSession;
    uint64_t mSessionId;
    ClientConnectionHandlerPtr mConnHandlerPtr;
    uint64_t mLastKnownEvent;

    typedef __gnu_cxx::hash_map<uint64_t, ClientHandleStatePtr> HandleMapT;
    HandleMapT  mHandleMap;
  };
  typedef boost::intrusive_ptr<ClientKeepaliveHandler> ClientKeepaliveHandlerPtr;
  

}

#endif // HYPERSPACE_CLIENTKEEPALIVEHANDLER_H

