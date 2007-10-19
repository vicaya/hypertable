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

#ifndef HYPERTABLE_IOHANDLERDATA_H
#define HYPERTABLE_IOHANDLERDATA_H

#include <list>

extern "C" {
#include <netdb.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
}

#include "Common/atomic.h"

#include "CommBuf.h"
#include "IOHandler.h"
#include "RequestCache.h"

using namespace std;


namespace hypertable {

  /**
   */
  class IOHandlerData : public IOHandler {

  public:

    IOHandlerData(int sd, struct sockaddr_in &addr, DispatchHandler *dh, HandlerMap &hmap) 
      : IOHandler(sd, addr, dh, hmap), mRequestCache(), mSendQueue() {
      mConnected = false;
      ResetIncomingMessageState();
      mId = atomic_inc_return(&msNextConnectionId);
    }

    void ResetIncomingMessageState() {
      mGotHeader = false;
      mMessageHeaderRemaining = sizeof(Header::HeaderT);
      mMessage = 0;
      mMessagePtr = 0;
      mMessageRemaining = 0;
    }

    int SendMessage(CommBufPtr &cbufPtr, time_t timeout=0, DispatchHandler *dispatchHandler=0);

    int FlushSendQueue();

#if defined(__APPLE__)
    virtual bool HandleEvent(struct kevent *event);
#elif defined(__linux__)
    virtual bool HandleEvent(struct epoll_event *event);
#else
    ImplementMe;
#endif

    bool HandleWriteReadiness();

    int ConnectionId() { return mId; }

  private:

    static atomic_t msNextConnectionId;

    bool                mConnected;
    boost::mutex        mMutex;
    Header::HeaderT     mMessageHeader;
    size_t              mMessageHeaderRemaining;
    bool                mGotHeader;
    uint8_t            *mMessage;
    uint8_t            *mMessagePtr;
    size_t              mMessageRemaining;
    RequestCache        mRequestCache;
    list<CommBufPtr>    mSendQueue;
    int                 mId;
  };

  typedef boost::intrusive_ptr<IOHandlerData> IOHandlerDataPtr;
}

#endif // HYPERTABLE_IOHANDLERDATA_H
