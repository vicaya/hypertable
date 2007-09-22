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
#include "SessionData.h"

using namespace hypertable;

namespace Hyperspace {

  class Master {
  public:

    Master(ConnectionManager *connManager, PropertiesPtr &propsPtr);
    ~Master();
    
    void CreateSession(struct sockaddr_in &addr, SessionDataPtr &sessionPtr);
    bool GetSession(uint32_t sessionId, SessionDataPtr &sessionPtr);

    void CreateHandle(uint64_t *handlep, HandleDataPtr &handlePtr);

    uint32_t GetLeaseInterval() { return mLeaseInterval; }

    void Mkdir(ResponseCallback *cb, uint32_t sessionId, const char *name);
    void Open(ResponseCallbackOpen *cb, uint32_t sessionId, const char *name, uint32_t flags, uint32_t eventMask);

    static const uint32_t DEFAULT_MASTER_PORT        = 38551;
    static const uint32_t DEFAULT_LEASE_INTERVAL     = 12;
    static const uint32_t DEFAULT_KEEPALIVE_INTERVAL = 7;

    static atomic_t msNextSessionId;

  private:

    void ReportError(ResponseCallback *cb);
    void NormalizeName(std::string name, std::string &normal);

    typedef __gnu_cxx::hash_map<std::string, NodeDataPtr> NodeMapT;
    typedef __gnu_cxx::hash_map<uint64_t, HandleDataPtr>  HandleMapT;
    typedef __gnu_cxx::hash_map<uint32_t, SessionDataPtr> SessionMapT;

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
  };

}

#endif // HYPERSPACE_MASTER_H
