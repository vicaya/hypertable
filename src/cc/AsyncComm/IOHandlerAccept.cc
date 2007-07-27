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

#include "HandlerMap.h"
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
  if (mShutdown)
    return true;
  if (event->filter == EVFILT_READ)  
    return HandleIncomingConnection();
  return true;
}
#elif defined(__linux__)
bool IOHandlerAccept::HandleEvent(struct epoll_event *event) {
  //DisplayEvent(event);
  if (mShutdown)
    return true;
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

  IOHandlerDataPtr dataHandlerPtr( new IOHandlerData(sd, addr, mHandlerFactory->newInstance(), mHandlerMap) );
  mHandlerMap.InsertDataHandler(dataHandlerPtr);
  dataHandlerPtr->StartPolling();

  DeliverEvent( new Event(Event::CONNECTION_ESTABLISHED, dataHandlerPtr->ConnectionId(), addr, Error::OK) );

  return false;
 }
