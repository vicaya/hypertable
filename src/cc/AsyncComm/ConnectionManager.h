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

#ifndef HYPERTABLE_CONNECTIONMANAGER_H
#define HYPERTABLE_CONNECTIONMANAGER_H

#include <queue>
#include <string>

#include <boost/shared_ptr.hpp>
#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/xtime.hpp>

extern "C" {
#include <time.h>
#include <sys/time.h>
}

#include "Common/ReferenceCount.h"
#include "Common/SockAddrMap.h"

#include "DispatchHandler.h"

namespace hypertable {

  class Comm;
  class Event;

  /**
   * This class is designed to create and maintain comm layer connections to
   * a set of remote IP addresses.  If any of the connections gets broken, then
   * this class will continuously attempt to re-establish the connection, pausing
   * for a while in between attempts.
   */
  class ConnectionManager : public DispatchHandler {

  public:

    class ConnectionState : public ReferenceCount {
    public:
      bool                connected;
      struct sockaddr_in  addr;
      time_t              timeout;
      boost::mutex        mutex;
      boost::condition    cond;
      boost::xtime        nextRetry;
      std::string         serviceName;
    };
    typedef boost::intrusive_ptr<ConnectionState> ConnectionStatePtr;


    struct ltConnectionState {
      bool operator()(const ConnectionStatePtr &cs1, const ConnectionStatePtr &cs2) const {
	return xtime_cmp(cs1->nextRetry, cs2->nextRetry) >= 0;
      }
    };

    typedef struct {
      Comm              *comm;
      boost::mutex       mutex;
      boost::condition   retryCond;
      boost::thread     *thread;
      SockAddrMapT<ConnectionStatePtr>  connMap;
      std::priority_queue<ConnectionStatePtr, std::vector<ConnectionStatePtr>, ltConnectionState> retryQueue;
      bool quietMode;
    } SharedImplT;

    /**
     * Constructor.  Creates a thread to do connection retry attempts.
     *
     * @param comm Pointer to the comm object
     */
    ConnectionManager(Comm *comm) {
      mImpl = new SharedImplT;
      mImpl->comm = comm;
      mImpl->thread = new boost::thread(*this);
      mImpl->quietMode = false;
    }

    /**
     * Copy Constructor.  Shares the implementation with object being copied.
     */
    ConnectionManager(const ConnectionManager &cm) {
      mImpl = cm.mImpl;
    }

    /**
     * Destructor.  Implement me!
     */
    virtual ~ConnectionManager() { return; }

    /**
     * Adds a connection to the connection manager.  The address structure addr
     * holds an address that the connection manager should maintain a connection to.
     * This method first checks to see if the address is already registered with the
     * connection manager and returns immediately if it is.  Otherwise, it adds the
     * address to an internal connection map, attempts to establish a connection to
     * the address, and then returns.  From here on out, the internal manager thread
     * will maintian the connection by continually re-establishing the connection if
     * it ever gets broken.
     *
     * @param addr The IP address to maintain a connection to
     * @param timeout The timeout value (in seconds) that gets passed into Comm::Connect and also used as the waiting period betweeen connection attempts
     * @param serviceName The name of the serivce at the other end of the connection used for descriptive log messages
     */
    void Add(struct sockaddr_in &addr, time_t timeout, const char *serviceName);

    /**
     * This method blocks until the connection to the given address is established.
     * The given address must have been previously added with a call to Add.  If
     * the connection is not established within maxWaitSecs, then the method returns
     * false.
     *
     * @param addr the address of the connection to wait for
     * @param maxWaitSecs The maximum time to wait for the connection before returning
     * @return true if connected, false otherwise
     */
    bool WaitForConnection(struct sockaddr_in &addr, long maxWaitSecs);

    /**
     * Returns the Comm object associated with this connection manager
     *
     * @return the assocated comm object
     */
    Comm *GetComm() { return mImpl->comm; }

    /**
     * This method sets a 'quietMode' flag which can disable the generation
     * of log messages upon failed connection attempts.  It is set to false
     * by default.
     *
     * @param mode The new value for the quietMode flag
     */
    void SetQuietMode(bool mode) { mImpl->quietMode = mode; }

    /**
     * This is the comm layer dispatch callback method.  It should only get
     * called by the AsyncComm subsystem.
     */
    virtual void handle(EventPtr &event);

    /**
     * This is the Boost thread "run" method.  It is called by the manager thread
     * when it starts up.
     */
    void operator()();

  private:

    void SendConnectRequest(ConnectionState *connState);

    SharedImplT *mImpl;

  };

}


#endif // HYPERTABLE_CONNECTIONMANAGER_H
