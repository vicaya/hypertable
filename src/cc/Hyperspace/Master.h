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
#include "Common/SockAddrMap.h"
#include "Common/StringExt.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/ResponseCallback.h"

#include "NodeData.h"
#include "HandleData.h"
#include "ResponseCallbackOpen.h"
#include "ServerKeepaliveHandler.h"
#include "SessionData.h"

using namespace hypertable;

namespace Hyperspace {

  class Master {
  public:

    Master(ConnectionManager *connManager, PropertiesPtr &propsPtr, ServerKeepaliveHandlerPtr &keepaliveHandlerPtr);
    ~Master();
    
    uint64_t CreateSession(struct sockaddr_in &addr);
    bool GetSessionData(uint64_t sessionId, SessionDataPtr &sessionPtr);
    int RenewSessionLease(uint64_t sessionId);
    void RemoveExpiredSessions();

    void CreateHandle(uint64_t *handlep, HandleDataPtr &handlePtr);
    bool GetHandleData(uint64_t sessionId, HandleDataPtr &handlePtr);

    void GetDatagramSendAddress(struct sockaddr_in *addr) { memcpy(addr, &mLocalAddr, sizeof(mLocalAddr)); }

    uint32_t GetLeaseInterval() { return mLeaseInterval; }

    void Mkdir(ResponseCallback *cb, uint64_t sessionId, const char *name);
    void Open(ResponseCallbackOpen *cb, uint64_t sessionId, const char *name, uint32_t flags, uint32_t eventMask);
    void AttrSet(ResponseCallback *cb, uint64_t sessionId, uint64_t handle, const char *name, const void *value, size_t valueLen);

    static const uint32_t DEFAULT_MASTER_PORT        = 38551;
    static const uint32_t DEFAULT_LEASE_INTERVAL     = 12;
    static const uint32_t DEFAULT_KEEPALIVE_INTERVAL = 7;
    static const uint32_t DEFAULT_GRACEPERIOD        = 45;

  private:

    void ReportError(ResponseCallback *cb);
    void NormalizeName(std::string name, std::string &normal);
    void DeliverEventNotifications(NodeData *node, int eventMask, std::string name);

    typedef __gnu_cxx::hash_map<std::string, NodeDataPtr> NodeMapT;
    typedef __gnu_cxx::hash_map<uint64_t, HandleDataPtr>  HandleMapT;
    typedef __gnu_cxx::hash_map<uint64_t, SessionDataPtr> SessionMapT;

    bool          mVerbose;
    uint32_t      mLeaseInterval;
    uint32_t      mKeepAliveInterval;
    NodeMapT      mNodeMap;
    HandleMapT    mHandleMap;
    SessionMapT   mSessionMap;
    boost::mutex  mNodeMapMutex;
    boost::mutex  mHandleMapMutex;
    boost::mutex  mSessionMapMutex;
    std::string   mBaseDir;
    int           mBaseFd;
    uint32_t      mGeneration;
    uint64_t      mNextHandleNumber;
    uint64_t      mNextSessionId;
    uint64_t      mNextEventId;
    ServerKeepaliveHandlerPtr mKeepaliveHandlerPtr;
    struct sockaddr_in mLocalAddr;

    struct ltSessionData {
      bool operator()(const SessionDataPtr &sd1, const SessionDataPtr &sd2) const {
	return xtime_cmp(sd1->expireTime, sd2->expireTime) >= 0;
      }
    };

    std::vector<SessionDataPtr> mSessionHeap;

  };

}

#endif // HYPERSPACE_MASTER_H
