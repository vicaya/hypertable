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

#include "DfsBroker/Lib/Client.h"
#include "Hypertable/Lib/Schema.h"

#include "Master.h"

using namespace hypertable;
using namespace Hyperspace;
using namespace std;

const uint32_t Master::DEFAULT_MASTER_PORT;
const uint32_t Master::DEFAULT_LEASE_INTERVAL;
const uint32_t Master::DEFAULT_KEEPALIVE_INTERVAL;

atomic_t Master::msNextSessionId = ATOMIC_INIT(1);

Master::Master(ConnectionManager *connManager, PropertiesPtr &propsPtr) : mMutex(), mVerbose(false) {

  mVerbose = propsPtr->getPropertyBool("verbose", false);

  mLeaseInterval = (uint32_t)propsPtr->getPropertyInt("Hyperspace.Lease.Interval", DEFAULT_LEASE_INTERVAL);

  mKeepAliveInterval = (uint32_t)propsPtr->getPropertyInt("Hyperspace.KeepAlive.Interval", DEFAULT_KEEPALIVE_INTERVAL);

  if (mVerbose) {
    cout << "Hyperspace.Lease.Interval=" << mLeaseInterval << endl;
    cout << "Hyperspace.KeepAlive.Interval=" << mKeepAliveInterval << endl;
  }

}


Master::~Master() {
  return;
}


void Master::CreateSession(struct sockaddr_in &addr, SessionStatePtr &sessionPtr) {
  boost::mutex::scoped_lock lock(mMutex);
  uint32_t sessionId = atomic_inc_return(&msNextSessionId);
  sessionPtr = new SessionState(addr, mLeaseInterval, sessionId);
  mSessionMap[sessionId] = sessionPtr;
  return;
}


bool Master::GetSession(uint32_t sessionId, SessionStatePtr &sessionPtr) {
  boost::mutex::scoped_lock lock(mMutex);
  SessionMapT::iterator iter = mSessionMap.find(sessionId);
  if (iter == mSessionMap.end())
    return false;
  sessionPtr = (*iter).second;
  return true;
}
