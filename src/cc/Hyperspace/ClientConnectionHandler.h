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

#ifndef HYPERSPACE_CLIENTCONNECTIONHANDLER_H
#define HYPERSPACE_CLIENTCONNECTIONHANDLER_H

#include <boost/thread/mutex.hpp>

#include "Common/InetAddr.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/DispatchHandler.h"

#include "ClientSessionState.h"

using namespace hypertable;

namespace Hyperspace {

  class SessionCallback;

  class ClientConnectionHandler : public DispatchHandler {
  public:

    enum { DISCONNECTED, CONNECTING, HANDSHAKING, CONNECTED };

    ClientConnectionHandler(Comm *comm, Hyperspace::SessionCallback *sessionCallback, ClientSessionStatePtr &sessionStatePtr);
    virtual void handle(EventPtr &eventPtr);

    void SetSessionId(uint64_t id) { mSessionId = id; }

    bool Disconnected() { 
      boost::mutex::scoped_lock lock(mMutex);
      return mState == DISCONNECTED;
    }

    int InitiateConnection(struct sockaddr_in &addr, long timeout) {
      boost::mutex::scoped_lock lock(mMutex);
      int error;
      mState = CONNECTING;
      if ((error = mComm->Connect(addr, timeout, this)) != Error::OK) {
	std::string str;
	LOG_VA_ERROR("Problem establishing TCP connection with Hyperspace.Master at %s - %s",
		     InetAddr::StringFormat(str, addr), Error::GetText(error));
	mComm->CloseSocket(addr);
	mState = DISCONNECTED;
      }
      return error;
    }

    void SetVerboseMode(bool verbose) { mVerbose = verbose; }

  private:
    boost::mutex       mMutex;
    Comm *mComm;
    Hyperspace::SessionCallback *mSessionCallback;
    ClientSessionStatePtr mSessionStatePtr;
    uint64_t mSessionId;
    int mState;
    bool mVerbose;
  };

}

#endif // HYPERSPACE_CLIENTCONNECTIONHANDLER_H
