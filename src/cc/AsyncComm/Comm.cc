/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

//#define HT_DISABLE_LOG_DEBUG

#include "Common/Compat.h"

#include <cassert>
#include <iostream>
using namespace std;

extern "C" {
#if defined(__APPLE__)
#include <arpa/inet.h>
#endif
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
}

#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/FileUtils.h"
#include "Common/System.h"

#include "ReactorFactory.h"
#include "ReactorRunner.h"
#include "Comm.h"
#include "IOHandlerAccept.h"
#include "IOHandlerData.h"

using namespace Hypertable;

atomic_t Comm::ms_next_request_id = ATOMIC_INIT(1);

Comm *Comm::ms_instance = NULL;
Mutex Comm::ms_mutex;

Comm::Comm() {
  if (ReactorFactory::ms_reactors.size() == 0) {
    HT_ERROR("ReactorFactory::initialize must be called before creating "
             "AsyncComm::comm object");
    HT_ABORT;
  }
  ReactorFactory::get_reactor(m_timer_reactor_ptr);
  m_handler_map_ptr = ReactorRunner::ms_handler_map_ptr;
}


Comm::~Comm() {
  set<IOHandler *> handlers;
  m_handler_map_ptr->decomission_all(handlers);

  foreach(IOHandler *handler, handlers)
    handler->shutdown();

  // wait for all decomissioned handlers to get purged by Reactor
  // (I think this is no longer needed, since the reactors should have been
  //  shut down before destroying the Comm layer)
  // m_handler_map_ptr->wait_for_empty();
}


void Comm::destroy() {
  ReactorFactory::destroy();

  if (ms_instance) {
    delete ms_instance;
    ms_instance = 0;
  }
}


int
Comm::connect(struct sockaddr_in &addr,
              DispatchHandlerPtr &default_handler_ptr) {
  int sd;

  if (m_handler_map_ptr->contains_handler(addr))
    return Error::COMM_ALREADY_CONNECTED;

  if ((sd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
    HT_ERRORF("socket: %s", strerror(errno));
    return Error::COMM_SOCKET_ERROR;
  }

  return connect_socket(sd, addr, default_handler_ptr);
}


int
Comm::connect(struct sockaddr_in &addr, struct sockaddr_in &local_addr,
              DispatchHandlerPtr &default_handler_ptr) {
  int sd;

  if (m_handler_map_ptr->contains_handler(addr))
    return Error::COMM_ALREADY_CONNECTED;

  if ((sd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
    HT_ERRORF("socket: %s", strerror(errno));
    return Error::COMM_SOCKET_ERROR;
  }

  // bind socket to local address
  if ((bind(sd, (const sockaddr *)&local_addr, sizeof(sockaddr_in))) < 0) {
    HT_ERRORF( "bind: %s: %s", InetAddr::format(local_addr).c_str(),
              strerror(errno));
    return Error::COMM_BIND_ERROR;
  }

  return connect_socket(sd, addr, default_handler_ptr);
}


void
Comm::listen(struct sockaddr_in &addr, ConnectionHandlerFactoryPtr &chf_ptr) {
  DispatchHandlerPtr null_handler(0);
  listen(addr, chf_ptr, null_handler);
}


int Comm::set_alias(struct sockaddr_in &addr, struct sockaddr_in &alias) {
  ScopedLock lock(ms_mutex);
  return m_handler_map_ptr->set_alias(addr, alias);
}


void
Comm::listen(struct sockaddr_in &addr, ConnectionHandlerFactoryPtr &chf_ptr,
             DispatchHandlerPtr &default_handler_ptr) {
  IOHandlerPtr handler;
  IOHandlerAccept *accept_handler;
  int one = 1;
  int sd;

  if ((sd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
    HT_THROW(Error::COMM_SOCKET_ERROR, strerror(errno));

#if defined(__linux__)
  if (setsockopt(sd, SOL_TCP, TCP_NODELAY, &one, sizeof(one)) < 0)
    HT_ERRORF("setting TCP_NODEPLAY: %s", strerror(errno));

#elif defined(__APPLE__)
  if (setsockopt(sd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one)) < 0)
    HT_WARNF("setsockopt(SO_NOSIGPIPE) failure: %s", strerror(errno));
#endif

  if (setsockopt(sd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0)
    HT_ERRORF("setting SO_REUSEADDR: %s", strerror(errno));

  if ((bind(sd, (const sockaddr *)&addr, sizeof(sockaddr_in))) < 0)
    HT_THROWF(Error::COMM_BIND_ERROR, "binding to %s: %s",
              InetAddr::format(addr).c_str(), strerror(errno));

  if (::listen(sd, 64) < 0)
    HT_THROWF(Error::COMM_LISTEN_ERROR, "listening: %s", strerror(errno));

  handler = accept_handler = new IOHandlerAccept(sd, addr, default_handler_ptr,
                                                 m_handler_map_ptr, chf_ptr);
  m_handler_map_ptr->insert_handler(accept_handler);
  accept_handler->start_polling();
}


int
Comm::send_request(struct sockaddr_in &addr, time_t timeout,
                   CommBufPtr &cbuf_ptr, DispatchHandler *resp_handler) {
  ScopedLock lock(ms_mutex);
  IOHandlerDataPtr data_handler;
  Header::Common *mheader = (Header::Common *)cbuf_ptr->data.base;
  int error = Error::OK;

  cbuf_ptr->reset_data_pointers();

  if (!m_handler_map_ptr->lookup_data_handler(addr, data_handler)) {
    HT_ERRORF("No connection for %s", InetAddr::format(addr).c_str());
    return Error::COMM_NOT_CONNECTED;
  }

  mheader->flags |= Header::FLAGS_BIT_REQUEST;
  if (resp_handler == 0) {
    mheader->flags |= Header::FLAGS_BIT_IGNORE_RESPONSE;
    mheader->id = 0;
  }
  else {
    mheader->id = atomic_inc_return(&ms_next_request_id);
    if (mheader->id == 0)
      mheader->id = atomic_inc_return(&ms_next_request_id);
  }

  if ((error = data_handler->send_message(cbuf_ptr, timeout, resp_handler))
      != Error::OK)
    data_handler->shutdown();

  return error;
}


int Comm::send_response(struct sockaddr_in &addr, CommBufPtr &cbuf_ptr) {
  ScopedLock lock(ms_mutex);
  IOHandlerDataPtr data_handler;
  Header::Common *mheader = (Header::Common *)cbuf_ptr->data.base;
  int error = Error::OK;

  cbuf_ptr->reset_data_pointers();

  if (!m_handler_map_ptr->lookup_data_handler(addr, data_handler)) {
    HT_ERRORF("No connection for %s", InetAddr::format(addr).c_str());
    return Error::COMM_NOT_CONNECTED;
  }

  mheader->flags &= Header::FLAGS_MASK_REQUEST;

  if ((error = data_handler->send_message(cbuf_ptr)) != Error::OK)
    data_handler->shutdown();

  return error;
}


void
Comm::create_datagram_receive_socket(struct sockaddr_in *addr, int tos,
                                     DispatchHandlerPtr &dhp) {
  IOHandlerPtr handler;
  IOHandlerDatagram *dg_handler;
  int one = 1;
  int sd;

  if ((sd = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
    HT_THROWF(Error::COMM_SOCKET_ERROR, "%s", strerror(errno));

  // Set to non-blocking
  FileUtils::set_flags(sd, O_NONBLOCK);

  int bufsize = 4*32768;

  if (setsockopt(sd, SOL_SOCKET, SO_SNDBUF, (char *)&bufsize, sizeof(bufsize))
      < 0) {
    HT_ERRORF("setsockopt(SO_SNDBUF) failed - %s", strerror(errno));
  }
  if (setsockopt(sd, SOL_SOCKET, SO_RCVBUF, (char *)&bufsize, sizeof(bufsize))
      < 0) {
    HT_ERRORF("setsockopt(SO_RCVBUF) failed - %s", strerror(errno));
  }

  if (setsockopt(sd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0) {
    HT_WARNF("setsockopt(SO_REUSEADDR) failure: %s", strerror(errno));
  }

  if (tos) {
    int opt = tos;
    setsockopt(sd, SOL_IP, IP_TOS, &opt, sizeof(opt));
    opt = tos;
    setsockopt(sd, SOL_SOCKET, SO_PRIORITY, &opt, sizeof(opt));    
  }

  // bind socket
  if ((bind(sd, (const sockaddr *)addr, sizeof(sockaddr_in))) < 0)
    HT_THROWF(Error::COMM_BIND_ERROR, "binding to %s: %s",
              InetAddr::format(*addr).c_str(), strerror(errno));

  handler = dg_handler = new IOHandlerDatagram(sd, *addr, dhp);

  handler->get_local_address(addr);

  m_handler_map_ptr->insert_datagram_handler(dg_handler);
  dg_handler->start_polling();
}


int
Comm::send_datagram(struct sockaddr_in &addr, struct sockaddr_in &send_addr,
                    CommBufPtr &cbuf_ptr) {
  ScopedLock lock(ms_mutex);
  IOHandlerDatagramPtr dg_handler;
  Header::Common *mheader = (Header::Common *)cbuf_ptr->data.base;
  int error = Error::OK;

  cbuf_ptr->reset_data_pointers();

  if (!m_handler_map_ptr->lookup_datagram_handler(send_addr, dg_handler)) {
    HT_ERRORF("Datagram send/local address %s not registered",
              InetAddr::format(send_addr).c_str());
    HT_ABORT;
  }

  mheader->flags |= (Header::FLAGS_BIT_REQUEST|
                     Header::FLAGS_BIT_IGNORE_RESPONSE);

  if ((error = dg_handler->send_message(addr, cbuf_ptr)) != Error::OK)
    dg_handler->shutdown();

  return error;
}


int Comm::set_timer(uint64_t duration_millis, DispatchHandler *handler) {
  ExpireTimer timer;
  boost::xtime_get(&timer.expire_time, boost::TIME_UTC);
  timer.expire_time.sec += duration_millis / 1000LL;
  timer.expire_time.nsec += (duration_millis % 1000LL) * 1000000LL;
  timer.handler = handler;
  m_timer_reactor_ptr->add_timer(timer);
  return Error::OK;
}


int
Comm::set_timer_absolute(boost::xtime expire_time, DispatchHandler *handler) {
  ExpireTimer timer;
  memcpy(&timer.expire_time, &expire_time, sizeof(boost::xtime));
  timer.handler = handler;
  m_timer_reactor_ptr->add_timer(timer);
  return Error::OK;
}


int
Comm::get_local_address(struct sockaddr_in addr,
                        struct sockaddr_in *local_addr) {
  ScopedLock lock(ms_mutex);
  IOHandlerDataPtr data_handler;

  if (!m_handler_map_ptr->lookup_data_handler(addr, data_handler)) {
    HT_ERRORF("No connection for %s", InetAddr::format(addr).c_str());
    return Error::COMM_NOT_CONNECTED;
  }

  data_handler->get_local_address(local_addr);

  return Error::OK;
}


int Comm::close_socket(struct sockaddr_in &addr) {
  IOHandlerPtr handler;

  if (!m_handler_map_ptr->decomission_handler(addr, handler))
    return Error::COMM_NOT_CONNECTED;

  handler->shutdown();

  return Error::OK;
}


/**
 *  ----- Private methods -----
 */

int
Comm::connect_socket(int sd, struct sockaddr_in &addr,
                     DispatchHandlerPtr &default_handler_ptr) {
  IOHandlerPtr handler;
  IOHandlerData *data_handler;
  int one = 1;

  // Set to non-blocking
  FileUtils::set_flags(sd, O_NONBLOCK);

#if defined(__linux__)
  if (setsockopt(sd, SOL_TCP, TCP_NODELAY, &one, sizeof(one)) < 0)
    HT_ERRORF("setsockopt(TCP_NODELAY) failure: %s", strerror(errno));

#elif defined(__APPLE__)
  if (setsockopt(sd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one)) < 0)
    HT_WARNF("setsockopt(SO_NOSIGPIPE) failure: %s", strerror(errno));
#endif

  handler = data_handler = new IOHandlerData(sd, addr, default_handler_ptr);
  m_handler_map_ptr->insert_handler(data_handler);

  while (::connect(sd, (struct sockaddr *)&addr, sizeof(struct sockaddr_in))
          < 0) {
    if (errno == EINTR) {
      poll(0, 0, 1000);
      continue;
    }
    else if (errno == EINPROGRESS) {
      data_handler->start_polling();
      data_handler->add_poll_interest(Reactor::READ_READY|Reactor::WRITE_READY);
      return Error::OK;
    }
    HT_ERRORF("connecting to %s: %s", InetAddr::format(addr).c_str(),
              strerror(errno));
    return Error::COMM_CONNECT_ERROR;
  }

  data_handler->start_polling();

  return Error::OK;
}
