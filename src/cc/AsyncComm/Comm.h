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

#ifndef HYPERTABLE_COMMENGINE_H
#define HYPERTABLE_COMMENGINE_H

#include <string>

#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/xtime.hpp>

extern "C" {
#include <stdint.h>
}

#include "DispatchHandler.h"
#include "CommBuf.h"
#include "ConnectionHandlerFactory.h"
#include "HandlerMap.h"

using namespace std;

namespace hypertable {

  class Comm {

  public:

    Comm();

    ~Comm();

    int Connect(struct sockaddr_in &addr, DispatchHandler *defaultHandler);

    int Connect(struct sockaddr_in &addr, struct sockaddr_in &localAddr, DispatchHandler *defaultHandler);

    int Listen(struct sockaddr_in &addr, ConnectionHandlerFactory *hfactory, DispatchHandler *defaultHandler=0);

    int SendRequest(struct sockaddr_in &addr, time_t timeout, CommBufPtr &cbufPtr, DispatchHandler *responseHandler);

    int SendResponse(struct sockaddr_in &addr, CommBufPtr &cbufPtr);

    int GetLocalAddress(struct sockaddr_in addr, struct sockaddr_in *localAddr);

    int CreateDatagramReceiveSocket(struct sockaddr_in *addr, DispatchHandler *handler);

    int SendDatagram(struct sockaddr_in &addr, struct sockaddr_in &sendAddr, CommBufPtr &cbufPtr);

    int SetTimer(uint64_t durationMillis, DispatchHandler *handler);

    int SetTimerAbsolute(boost::xtime expireTime, DispatchHandler *handler);

    int CloseSocket(struct sockaddr_in &addr);

  private:

    int ConnectSocket(int sd, struct sockaddr_in &addr, DispatchHandler *defaultHandler);

    boost::mutex  mMutex;
    std::string   mAppName;
    HandlerMap    mHandlerMap;
    Reactor      *mTimerReactor;
  };

  typedef boost::shared_ptr<Comm> CommPtr;

}

#endif // HYPERTABLE_COMMENGINE_H
