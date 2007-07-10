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


#include <cassert>
#include <cstdio>
#include <iostream>
#include <set>
using namespace std;

extern "C" {
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/time.h>
#if defined(__APPLE__)
#include <sys/event.h>
#endif
}

#include "Common/Error.h"
#include "Common/Logger.h"

#include "IOHandlerData.h"
#include "Reactor.h"
using namespace hypertable;

const int Reactor::READ_READY   = 0x01;
const int Reactor::WRITE_READY  = 0x02;


/**
 */
Reactor::Reactor() {
#if defined(__linux__)
  if ((pollFd = epoll_create(256)) < 0) {
    perror("epoll_create");
    exit(1);
  }
#elif defined(__APPLE__)
  kQueue = kqueue();
#endif  
}


void Reactor::HandleTimeouts() {
  struct timeval tval;

  if (gettimeofday(&tval, 0) < 0) {
    LOG_VA_ERROR("gettimeofday() failed : %s", strerror(errno));
    return;
  }
  
  IOHandler       *handler;
  CallbackHandler *cb;

  while ((cb = mRequestCache.GetNextTimeout(tval.tv_sec, handler)) != 0) {
    handler->DeliverEvent( new Event(Event::ERROR, ((IOHandlerData *)handler)->GetAddress(), Error::COMM_REQUEST_TIMEOUT), cb );
  }
}
  

