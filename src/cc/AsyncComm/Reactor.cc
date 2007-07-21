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
  

