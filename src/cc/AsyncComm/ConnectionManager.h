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

#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/xtime.hpp>


extern "C" {
#include <time.h>
#include <sys/time.h>
}

#include "Common/SockAddrMap.h"

#include "DispatchHandler.h"

namespace hypertable {

  class Comm;
  class Event;

  class ConnectionManager : public DispatchHandler {

  public:

    typedef struct {
      bool                connected;
      struct sockaddr_in  addr;
      time_t              timeout;
      boost::mutex        mutex;
      boost::condition    cond;
      boost::xtime        nextRetry;
      std::string         serviceName;
    } ConnectionStateT;

    struct ltConnectionState {
      bool operator()(const ConnectionStateT *cs1, const ConnectionStateT *cs2) const {
	return xtime_cmp(cs1->nextRetry, cs2->nextRetry) >= 0;
      }
    };

    typedef struct {
      Comm              *comm;
      boost::mutex       mutex;
      boost::condition   retryCond;
      boost::thread     *thread;
      SockAddrMapT<ConnectionStateT *>  connMap;
      std::priority_queue<ConnectionStateT *, std::vector<ConnectionStateT *>, ltConnectionState> retryQueue;
      bool quietMode;
    } SharedImplT;

    ConnectionManager(Comm *comm) {
      mImpl = new SharedImplT;
      mImpl->comm = comm;
      mImpl->thread = new boost::thread(*this);
      mImpl->quietMode = false;
    }
    
    ConnectionManager(const ConnectionManager &cm) {
      mImpl = cm.mImpl;
    }

    virtual ~ConnectionManager() { /** implement me! **/ return; }

    void Add(struct sockaddr_in &addr, time_t timeout, const char *serviceName);

    virtual void handle(EventPtr &event);

    bool WaitForConnection(struct sockaddr_in &addr, long maxWaitSecs);

    void operator()();

    Comm *GetComm() { return mImpl->comm; }

    void SetQuietMode(bool mode) { mImpl->quietMode = mode; }

  private:

    void SendConnectRequest(ConnectionStateT *connState);

    SharedImplT *mImpl;

  };

}


#endif // HYPERTABLE_CONNECTIONMANAGER_H
