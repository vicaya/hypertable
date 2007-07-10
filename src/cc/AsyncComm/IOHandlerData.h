/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
