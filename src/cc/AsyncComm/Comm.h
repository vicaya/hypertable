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


#ifndef HYPERTABLE_COMMENGINE_H
#define HYPERTABLE_COMMENGINE_H

#include <cassert>
#include <iostream>

#include <queue>
#include <ext/hash_map>
using namespace std;

#include <boost/thread/mutex.hpp>

extern "C" {
#include <stdint.h>
}

#include "CallbackHandler.h"
#include "ConnectionMap.h"
#include "ConnectionHandlerFactory.h"
#include "EventQueue.h"
#include "CommBuf.h"

namespace hypertable {

  class Comm {

  public:

    Comm(uint32_t handlerCount);

    int Connect(struct sockaddr_in &addr, time_t timeout, CallbackHandler *defaultHandler);

    int Listen(uint16_t port, ConnectionHandlerFactory *hfactory, CallbackHandler *defaultHandler=0);

    int SendRequest(struct sockaddr_in &addr, CommBuf *cbuf, CallbackHandler *responseHandler);

    int SendResponse(struct sockaddr_in &addr, CommBuf *cbuf);

  private:
    const char      *mAppName;
    ConnectionMap    mConnMap;
    boost::mutex     mMutex;
    EventQueue      *mEventQueue;
  };

}

#endif // HYPERTABLE_COMMENGINE_H
