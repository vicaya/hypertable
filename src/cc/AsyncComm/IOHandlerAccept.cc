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


#include <iostream>
using namespace std;

extern "C" {
#include <errno.h>
#include <netinet/tcp.h>
}

#define DISABLE_LOG_DEBUG 1

#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/Logger.h"

#include "ConnectionMap.h"
#include "IOHandlerAccept.h"
#include "IOHandlerData.h"
#include "ReactorFactory.h"
using namespace hypertable;


/**
 *
 */
#if defined(__APPLE__)
bool IOHandlerAccept::HandleEvent(struct kevent *event) {
  //DisplayEvent(event);
  if (event->filter == EVFILT_READ)  
    return HandleIncomingConnection();
  return true;
}
#elif defined(__linux__)
bool IOHandlerAccept::HandleEvent(struct epoll_event *event) {
  //DisplayEvent(event);
  return HandleIncomingConnection();
}
#else
  ImplementMe;
#endif



bool IOHandlerAccept::HandleIncomingConnection() {
  int sd;
  struct sockaddr_in addr;
  socklen_t addrLen = sizeof(sockaddr_in);
  int one = 1;

  if ((sd = accept(mSd, (struct sockaddr *)&addr, &addrLen)) < 0) {
    LOG_VA_ERROR("accept() failure: %s", strerror(errno));
    return false;
  }

  LOG_VA_DEBUG("Just accepted incoming connection, fd=%d (%s:%d)", mSd, inet_ntoa(addr.sin_addr), ntohs(addr.sin_port));

  // Set to non-blocking
  FileUtils::SetFlags(sd, O_NONBLOCK);

#if defined(__linux__)
  if (setsockopt(sd, SOL_TCP, TCP_NODELAY, &one, sizeof(one)) < 0)
    LOG_VA_WARN("setsockopt(TCP_NODELAY) failure: %s", strerror(errno));
#elif defined(__APPLE__)
  if (setsockopt(sd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one)) < 0)
    LOG_VA_WARN("setsockopt(SO_NOSIGPIPE) failure: %s", strerror(errno));
#endif

  int bufsize = 4*32768;

  if (setsockopt(sd, SOL_SOCKET, SO_SNDBUF, (char *)&bufsize, sizeof(bufsize)) < 0) {
    LOG_VA_WARN("setsockopt(SO_SNDBUF) failed - %s", strerror(errno));
  }
  if (setsockopt(sd, SOL_SOCKET, SO_RCVBUF, (char *)&bufsize, sizeof(bufsize)) < 0) {
    LOG_VA_WARN("setsockopt(SO_RCVBUF) failed - %s", strerror(errno));
  }

  IOHandlerDataPtr dataHandlerPtr( new IOHandlerData(sd, addr, mHandlerFactory->newInstance(), mConnMap, mEventQueue) );
  mConnMap.Insert(dataHandlerPtr);
  dataHandlerPtr->StartPolling();

  DeliverEvent( new Event(Event::CONNECTION_ESTABLISHED, addr, Error::OK) );

  return false;
 }
