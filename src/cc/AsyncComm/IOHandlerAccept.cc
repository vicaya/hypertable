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
using namespace Hypertable;


/**
 *
 */
#if defined(__APPLE__)
bool IOHandlerAccept::handle_event(struct kevent *event) {
  //DisplayEvent(event);
  if (event->filter == EVFILT_READ)  
    return handle_incoming_connection();
  return true;
}
#elif defined(__linux__)
bool IOHandlerAccept::handle_event(struct epoll_event *event) {
  //DisplayEvent(event);
  return handle_incoming_connection();
}
#else
  ImplementMe;
#endif



bool IOHandlerAccept::handle_incoming_connection() {
  int sd;
  struct sockaddr_in addr;
  socklen_t addrLen = sizeof(sockaddr_in);
  int one = 1;
  IOHandlerData *dataHandler;

  if ((sd = accept(m_sd, (struct sockaddr *)&addr, &addrLen)) < 0) {
    HT_ERRORF("accept() failure: %s", strerror(errno));
    return false;
  }

  HT_DEBUGF("Just accepted incoming connection, fd=%d (%s:%d)", m_sd, inet_ntoa(addr.sin_addr), ntohs(addr.sin_port));

  // Set to non-blocking
  FileUtils::set_flags(sd, O_NONBLOCK);

#if defined(__linux__)
  if (setsockopt(sd, SOL_TCP, TCP_NODELAY, &one, sizeof(one)) < 0)
    HT_WARNF("setsockopt(TCP_NODELAY) failure: %s", strerror(errno));
#elif defined(__APPLE__)
  if (setsockopt(sd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one)) < 0)
    HT_WARNF("setsockopt(SO_NOSIGPIPE) failure: %s", strerror(errno));
#endif

  int bufsize = 4*32768;

  if (setsockopt(sd, SOL_SOCKET, SO_SNDBUF, (char *)&bufsize, sizeof(bufsize)) < 0) {
    HT_WARNF("setsockopt(SO_SNDBUF) failed - %s", strerror(errno));
  }
  if (setsockopt(sd, SOL_SOCKET, SO_RCVBUF, (char *)&bufsize, sizeof(bufsize)) < 0) {
    HT_WARNF("setsockopt(SO_RCVBUF) failed - %s", strerror(errno));
  }

  DispatchHandlerPtr dhp;
  m_handler_factory_ptr->get_instance(dhp);

  dataHandler = new IOHandlerData(sd, addr, dhp);

  IOHandlerPtr handlerPtr( dataHandler );
  m_handler_map_ptr->insert_handler(dataHandler);
  dataHandler->start_polling();

  deliver_event( new Event(Event::CONNECTION_ESTABLISHED, dataHandler->connection_id(), addr, Error::OK) );

  return false;
 }
