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


#ifndef HYPERTABLE_IOHANDLERACCEPT_H
#define HYPERTABLE_IOHANDLERACCEPT_H

#include "IOHandler.h"
#include "ConnectionHandlerFactory.h"

extern "C" {
#include <stdint.h>
}

namespace hypertable {

  /**
   *
   */
  class IOHandlerAccept : public IOHandler {

  public:

    IOHandlerAccept(int sd, CallbackHandler *cbh, ConnectionMap &cm, EventQueue *eq, ConnectionHandlerFactory *hfactory) : IOHandler(sd, cbh, cm, eq) {
      mHandlerFactory = hfactory;
      return;
    }

    virtual ~IOHandlerAccept() {
      return;
    }

#if defined(__APPLE__)
    virtual bool HandleEvent(struct kevent *event);
#elif defined(__linux__)
    virtual bool HandleEvent(struct epoll_event *event);
#else
    ImplementMe;
#endif

    bool HandleIncomingConnection();

  private:
    ConnectionHandlerFactory *mHandlerFactory;
  };

}


#endif // HYPERTABLE_IOHANDLERACCEPT_H

