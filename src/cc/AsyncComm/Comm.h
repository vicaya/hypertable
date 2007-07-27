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

#ifndef HYPERTABLE_COMMENGINE_H
#define HYPERTABLE_COMMENGINE_H

#include <string>

#include <boost/thread/mutex.hpp>

extern "C" {
#include <stdint.h>
}

#include "CallbackHandler.h"
#include "CommBuf.h"
#include "ConnectionHandlerFactory.h"
#include "HandlerMap.h"

using namespace std;

namespace hypertable {

  class Comm {

  public:

    Comm(uint32_t dispatcherCount=0);

    ~Comm();

    int Connect(struct sockaddr_in &addr, time_t timeout, CallbackHandler *defaultHandler);

    int Listen(uint16_t port, ConnectionHandlerFactory *hfactory, CallbackHandler *defaultHandler=0);

    int SendRequest(struct sockaddr_in &addr, CommBufPtr &cbufPtr, CallbackHandler *responseHandler);

    int SendResponse(struct sockaddr_in &addr, CommBufPtr &cbufPtr);

  private:
    boost::mutex  mMutex;
    std::string   mAppName;
    HandlerMap    mHandlerMap;
  };

}

#endif // HYPERTABLE_COMMENGINE_H
