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

#ifndef HYPERTABLE_IOHANDLERDATAGRAM_H
#define HYPERTABLE_IOHANDLERDATAGRAM_H

#include <list>
#include <utility>

extern "C" {
#include <netdb.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
}

#include "CommBuf.h"
#include "IOHandler.h"

using namespace std;

namespace hypertable {

  /**
   */
  class IOHandlerDatagram : public IOHandler {

  public:

    IOHandlerDatagram(int sd, struct sockaddr_in &addr, DispatchHandler *dh, HandlerMap &hmap) : IOHandler(sd, addr, dh, hmap), mSendQueue() {
      mMessage = new uint8_t [ 65536 ];
    }

    int SendMessage(struct sockaddr_in &addr, CommBufPtr &cbufPtr);

    int FlushSendQueue();

#if defined(__APPLE__)
    virtual bool HandleEvent(struct kevent *event);
#elif defined(__linux__)
    virtual bool HandleEvent(struct epoll_event *event);
#else
    ImplementMe;
#endif

    int HandleWriteReadiness();

  private:

    typedef std::pair<struct sockaddr_in, CommBufPtr> SendRecT;

    boost::mutex        mMutex;
    uint8_t            *mMessage;
    list<SendRecT> mSendQueue;
  };

  typedef boost::intrusive_ptr<IOHandlerDatagram> IOHandlerDatagramPtr;
}

#endif // HYPERTABLE_IOHANDLERDATAGRAM_H
