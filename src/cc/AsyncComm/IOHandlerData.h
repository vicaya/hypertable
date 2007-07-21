/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */


#ifndef HYPERTABLE_IOHANDLERDATA_H
#define HYPERTABLE_IOHANDLERDATA_H

#include <list>

#include <boost/shared_ptr.hpp>

extern "C" {
#include <netdb.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
}

#include "Common/atomic.h"

#include "CommBuf.h"
#include "EventQueue.h"
#include "IOHandler.h"
#include "RequestCache.h"

using namespace std;


namespace hypertable {

  /**
   */
  class IOHandlerData : public IOHandler {

  public:

    IOHandlerData(int sd, struct sockaddr_in &addr, CallbackHandler *cbh, ConnectionMap &cm, EventQueue *eq) 
      : IOHandler(sd, cbh, cm, eq), mAddr(addr), mRequestCache(), mTimeout(0), mSendQueue() {
      mConnected = false;
      ResetIncomingMessageState();
      mMessage = 0;
      mMessagePtr = 0;
      mMessageRemaining = 0;
      mShutdown = false;
    }

    void ResetIncomingMessageState() {
      mGotHeader = false;
      mMessageHeaderRemaining = sizeof(Message::HeaderT);
    }

    void SetTimeout(time_t timeout) {
      mTimeout = timeout;
    }

    struct sockaddr_in &GetAddress() { return mAddr; }

    int GetFd() { return mSd; }

    int SendMessage(CommBuf *cbuf, CallbackHandler *cbHandler=0);

    int FlushSendQueue();

#if defined(__APPLE__)
    virtual bool HandleEvent(struct kevent *event);
#elif defined(__linux__)
    virtual bool HandleEvent(struct epoll_event *event);
#else
    ImplementMe;
#endif

    bool HandleWriteReadiness();

    void Shutdown() { mShutdown = true; }

  private:
    struct sockaddr_in  mAddr;
    bool                mConnected;
    boost::mutex        mMutex;
    Message::HeaderT    mMessageHeader;
    size_t              mMessageHeaderRemaining;
    bool                mGotHeader;
    uint8_t            *mMessage;
    uint8_t            *mMessagePtr;
    size_t              mMessageRemaining;
    bool                mShutdown;
    RequestCache        mRequestCache;
    time_t              mTimeout;
    list<CommBuf>       mSendQueue;
  };

  typedef boost::shared_ptr<IOHandlerData> IOHandlerDataPtr;
}

#endif // HYPERTABLE_IOHANDLERDATA_H
