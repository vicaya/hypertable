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

    int Connect(struct sockaddr_in &addr, time_t timeout, DispatchHandler *defaultHandler);

    int Listen(uint16_t port, ConnectionHandlerFactory *hfactory, DispatchHandler *defaultHandler=0);

    int SendRequest(struct sockaddr_in &addr, CommBufPtr &cbufPtr, DispatchHandler *responseHandler);

    int SendResponse(struct sockaddr_in &addr, CommBufPtr &cbufPtr);

    int OpenDatagramReceivePort(uint16_t port, DispatchHandler *handler);

    int SendDatagram(struct sockaddr_in &addr, uint16_t sendPort, CommBufPtr &cbufPtr);

  private:
    boost::mutex  mMutex;
    std::string   mAppName;
    HandlerMap    mHandlerMap;
  };

  typedef boost::shared_ptr<Comm> CommPtr;

}

#endif // HYPERTABLE_COMMENGINE_H
