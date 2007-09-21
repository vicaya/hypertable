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

#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/ResponseCallback.h"

#include "SessionData.h"

using namespace hypertable;

namespace Hyperspace {

  class Master {
  public:

    Master(ConnectionManager *connManager, PropertiesPtr &propsPtr);
    ~Master();
    
    void CreateSession(struct sockaddr_in &addr, SessionDataPtr &sessionPtr);
    bool GetSession(uint32_t sessionId, SessionDataPtr &sessionPtr);
    uint32_t GetLeaseInterval() { return mLeaseInterval; }

    void Mkdir(ResponseCallback *cb, const char *name);

    static const uint32_t DEFAULT_MASTER_PORT        = 38551;
    static const uint32_t DEFAULT_LEASE_INTERVAL     = 12;
    static const uint32_t DEFAULT_KEEPALIVE_INTERVAL = 7;

    static atomic_t msNextSessionId;

  private:

    void ReportError(ResponseCallback *cb);

    typedef __gnu_cxx::hash_map<uint32_t, SessionDataPtr> SessionMapT;

    boost::mutex        mMutex;
    bool mVerbose;
    uint32_t mLeaseInterval;
    uint32_t mKeepAliveInterval;
    SessionMapT mSessionMap;
    std::string mBaseDir;
    int mBaseFd;
    uint32_t mGeneration;
  };

}

#endif // HYPERSPACE_MASTER_H
