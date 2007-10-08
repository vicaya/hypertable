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
#include "ResponseCallbackExists.h"
#include "ResponseCallbackAttrGet.h"
#include "ResponseCallbackLock.h"
#include "ResponseCallbackReaddir.h"
#include "ServerKeepaliveHandler.h"
#include "SessionData.h"

using namespace hypertable;

namespace Hyperspace {

  class Master {
  public:

    static const uint32_t DEFAULT_MASTER_PORT        = 38551;
    static const uint32_t DEFAULT_LEASE_INTERVAL     = 12;
    static const uint32_t DEFAULT_KEEPALIVE_INTERVAL = 7;
    static const uint32_t DEFAULT_GRACEPERIOD        = 45;

    Master(ConnectionManager *connManager, PropertiesPtr &propsPtr, ServerKeepaliveHandlerPtr &keepaliveHandlerPtr);
    ~Master();

    void Mkdir(ResponseCallback *cb, uint64_t sessionId, const char *name);
    void Delete(ResponseCallback *cb, uint64_t sessionId, const char *name);
    void Open(ResponseCallbackOpen *cb, uint64_t sessionId, const char *name, uint32_t flags, uint32_t eventMask);
    void Close(ResponseCallback *cb, uint64_t sessionId, uint64_t handle);
    void AttrSet(ResponseCallback *cb, uint64_t sessionId, uint64_t handle, const char *name, const void *value, size_t valueLen);
    void AttrGet(ResponseCallbackAttrGet *cb, uint64_t sessionId, uint64_t handle, const char *name);
    void AttrDel(ResponseCallback *cb, uint64_t sessionId, uint64_t handle, const char *name);
    void Exists(ResponseCallbackExists *cb, uint64_t sessionId, const char *name);
    void Readdir(ResponseCallbackReaddir *cb, uint64_t sessionId, uint64_t handle);
    void Lock(ResponseCallbackLock *cb, uint64_t sessionId, uint64_t handle, uint32_t mode, bool tryAcquire);
    void Release(ResponseCallback *cb, uint64_t sessionId, uint64_t handle);

    /**
     * Creates a new session by allocating a new SessionData object, obtaining a
     * new session ID and inserting the object into the Session map.
     *
     * @param addr Address from which the UDP client is sending
     * @return the new session ID
     */
    uint64_t CreateSession(struct sockaddr_in &addr);

    /**
     * Obtains the SessionData object for the given id from the session map.
     *
     * @param sessionId Session ID to lookup
     * @param sessionPtr Reference to SessionData smart pointer
     * @return true if found, false otherwise
     */
    bool GetSession(uint64_t sessionId, SessionDataPtr &sessionPtr);

    /**
     * Attempts to renew the session lease for session with the given ID.  If
     * the session cannot be found or if it is expired, the method returns
     * Error::HYPERSPACE_EXPIRED_SESSION otherwise, it renews the session
     * lease by invoking the RenewLease method of the SessionData object.
     *
     * @param sessionId Session ID to renew
     * @return Error::OK if successful
     */
    int RenewSessionLease(uint64_t sessionId);

    bool NextExpiredSession(SessionDataPtr &sessionPtr);
    void RemoveExpiredSessions();

    void CreateHandle(uint64_t *handlep, HandleDataPtr &handlePtr);
    bool GetHandleData(uint64_t sessionId, HandleDataPtr &handlePtr);
    bool RemoveHandleData(uint64_t sessionId, HandleDataPtr &handlePtr);

    void GetDatagramSendAddress(struct sockaddr_in *addr) { memcpy(addr, &mLocalAddr, sizeof(mLocalAddr)); }

  private:

    void ReportError(ResponseCallback *cb);
    void NormalizeName(std::string name, std::string &normal);
    void DeliverEventNotifications(NodeData *node, HyperspaceEventPtr &eventPtr, bool waitForNotify=true);
    void DeliverEventNotification(HandleDataPtr &handlePtr, HyperspaceEventPtr &eventPtr, bool waitForNotify=true);

    /**
     * Locates the parent 'node' of the given pathname.  It determines the name of the parent
     * node by stripping off the characters incuding and after the last '/' character.  It then
     * looks up the name in the mNodeMap and sets the pointer reference 'parentNodePtr' to it.
     * As a side effect, it also saves the child name (e.g. characters after the last '/' character
     * to the string reference childName.  NOTE: This method locks the mNodeMapMutex.
     *
     * @param normalName Normalized (e.g. no trailing '/') name of path to find parent of
     * @param parentNodePtr pointer reference to hold return pointer
     * @param 
     */
    bool FindParentNode(std::string normalName, NodeDataPtr &nodePtr, std::string &nodeName);
    bool DestroyHandle(uint64_t handle, int *errorp, std::string &errMsg, bool waitForNotify=true);
    void ReleaseLock(HandleDataPtr &handlePtr, bool waitForNotify=true);
    void LockHandle(HandleDataPtr &handlePtr, uint32_t mode, bool waitForNotify=true);
    void LockHandleWithNotification(HandleDataPtr &handlePtr, uint32_t mode, bool waitForNotify=true);

    /**
     * Safely obtains the node with the given pathname from the NodeMap,
     * creating one and inserting it into the map if it is not found.
     *
     * @param name pathname of node
     * @param nodePtr Reference of node smart pointer to hold return node
     */
    void GetNode(std::string name, NodeDataPtr &nodePtr) {
      boost::mutex::scoped_lock lock(mNodeMapMutex);
      NodeMapT::iterator iter = mNodeMap.find(name);
      if (iter != mNodeMap.end()) {
	nodePtr = (*iter).second;
	return;
      }
      nodePtr = new NodeData();
      nodePtr->name = name;
      mNodeMap[name] = nodePtr;
    }

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
    ServerKeepaliveHandlerPtr mKeepaliveHandlerPtr;
    struct sockaddr_in mLocalAddr;
    std::vector<SessionDataPtr> mSessionHeap;

  };

}

#endif // HYPERSPACE_MASTER_H
