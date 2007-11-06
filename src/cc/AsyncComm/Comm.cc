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

//#define DISABLE_LOG_DEBUG

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
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
}

#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/FileUtils.h"
#include "Common/System.h"

#include "ReactorFactory.h"
#include "Comm.h"
#include "IOHandlerAccept.h"
#include "IOHandlerData.h"

using namespace hypertable;


/**
 */
Comm::Comm() {
  if (ReactorFactory::ms_reactors.size() == 0) {
    LOG_ERROR("ReactorFactory::initialize must be called before creating AsyncComm::comm object");
    DUMP_CORE;
  }
  m_timer_reactor = ReactorFactory::get_reactor();
}



/**
 *
 */
Comm::~Comm() {

  set<IOHandler *> handlers;
  m_handler_map.decomission_all(handlers);
  for (set<IOHandler *>::iterator iter = handlers.begin(); iter != handlers.end(); ++iter)
    (*iter)->shutdown();

  // wait for all decomissioned handlers to get purged by Reactor
  m_handler_map.wait_for_empty();
}



/**
 */
int Comm::connect(struct sockaddr_in &addr, DispatchHandlerPtr &defaultHandlerPtr) {
  int sd;

  if (m_handler_map.contains_handler(addr))
    return Error::COMM_ALREADY_CONNECTED;

  if ((sd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
    cerr << "socket() failure: " << strerror(errno) << endl;
    exit(1);
  }

  return connect_socket(sd, addr, defaultHandlerPtr);
}



/**
 *
 */
int Comm::connect(struct sockaddr_in &addr, struct sockaddr_in &localAddr, DispatchHandlerPtr &defaultHandlerPtr) {
  int sd;

  if (m_handler_map.contains_handler(addr))
    return Error::COMM_ALREADY_CONNECTED;

  if ((sd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
    LOG_VA_ERROR("socket() failure: %s", strerror(errno));
    exit(1);
  }

  // bind socket to local address
  if ((bind(sd, (const sockaddr *)&localAddr, sizeof(sockaddr_in))) < 0) {
    LOG_VA_ERROR("bind() failure: %s", strerror(errno));
    exit(1);
  }

  return connect_socket(sd, addr, defaultHandlerPtr);
}


/**
 *
 */
int Comm::listen(struct sockaddr_in &addr, ConnectionHandlerFactoryPtr &chfPtr) {
  DispatchHandlerPtr nullHandlerPtr(0);
  return listen(addr, chfPtr, nullHandlerPtr);
}


/**
 *
 */
int Comm::listen(struct sockaddr_in &addr, ConnectionHandlerFactoryPtr &chfPtr, DispatchHandlerPtr &defaultHandlerPtr) {
  IOHandlerPtr handlerPtr;
  IOHandlerAccept *acceptHandler;
  int one = 1;
  int sd;

  if ((sd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
    cerr << "socket() failure: " << strerror(errno) << endl;
    exit(1);
  }

#if defined(__linux__)
  if (setsockopt(sd, SOL_TCP, TCP_NODELAY, &one, sizeof(one)) < 0)
    cerr << "setsockopt(TCP_NODELAY) failure: " << strerror(errno) << endl;
#elif defined(__APPLE__)
  if (setsockopt(sd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one)) < 0)
    LOG_VA_WARN("setsockopt(SO_NOSIGPIPE) failure: %s", strerror(errno));
#endif

  if (setsockopt(sd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0)
    cerr << "setsockopt(SO_REUSEADDR) failure: " << strerror(errno) << endl;

  // bind socket
  if ((bind(sd, (const sockaddr *)&addr, sizeof(sockaddr_in))) < 0) {
    cerr << "bind() failure: " << strerror(errno) << endl;
    exit(1);
  }

  if (::listen(sd, 64) < 0) {
    cerr << "listen() failure: " << strerror(errno) << endl;
    exit(1);
  }

  handlerPtr = acceptHandler = new IOHandlerAccept(sd, addr, defaultHandlerPtr, m_handler_map, chfPtr);
  m_handler_map.insert_handler(acceptHandler);
  acceptHandler->start_polling();

  return Error::OK;
}



int Comm::send_request(struct sockaddr_in &addr, time_t timeout, CommBufPtr &cbufPtr, DispatchHandler *responseHandler) {
  boost::mutex::scoped_lock lock(m_mutex);
  IOHandlerDataPtr dataHandlerPtr;
  Header::HeaderT *mheader = (Header::HeaderT *)cbufPtr->data;
  int error = Error::OK;

  cbufPtr->reset_data_pointers();

  if (!m_handler_map.lookup_data_handler(addr, dataHandlerPtr)) {
    LOG_VA_ERROR("No connection for %s:%d", inet_ntoa(addr.sin_addr), ntohs(addr.sin_port));
    return Error::COMM_NOT_CONNECTED;
  }

  mheader->flags |= Header::FLAGS_MASK_REQUEST;

  if ((error = dataHandlerPtr->send_message(cbufPtr, timeout, responseHandler)) != Error::OK)
    dataHandlerPtr->shutdown();

  return error;
}


int Comm::send_response(struct sockaddr_in &addr, CommBufPtr &cbufPtr) {
  boost::mutex::scoped_lock lock(m_mutex);
  IOHandlerDataPtr dataHandlerPtr;
  Header::HeaderT *mheader = (Header::HeaderT *)cbufPtr->data;
  int error = Error::OK;

  cbufPtr->reset_data_pointers();

  if (!m_handler_map.lookup_data_handler(addr, dataHandlerPtr)) {
    LOG_VA_ERROR("No connection for %s:%d", inet_ntoa(addr.sin_addr), ntohs(addr.sin_port));
    return Error::COMM_NOT_CONNECTED;
  }

  mheader->flags &= Header::FLAGS_MASK_RESPONSE;

  if ((error = dataHandlerPtr->send_message(cbufPtr)) != Error::OK)
    dataHandlerPtr->shutdown();

  return error;
}



/**
 * 
 */
int Comm::create_datagram_receive_socket(struct sockaddr_in *addr, DispatchHandlerPtr &dispatchHandlerPtr) {
  IOHandlerPtr handlerPtr;
  IOHandlerDatagram *datagramHandler;
  int one = 1;
  int sd;

  if ((sd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    LOG_VA_ERROR("socket() failure: %s", strerror(errno));
    exit(1);
  }

  // Set to non-blocking
  FileUtils::set_flags(sd, O_NONBLOCK);

  int bufsize = 4*32768;
  if (setsockopt(sd, SOL_SOCKET, SO_SNDBUF, (char *)&bufsize, sizeof(bufsize)) < 0) {
    LOG_VA_ERROR("setsockopt(SO_SNDBUF) failed - %s", strerror(errno));
  }
  if (setsockopt(sd, SOL_SOCKET, SO_RCVBUF, (char *)&bufsize, sizeof(bufsize)) < 0) {
    LOG_VA_ERROR("setsockopt(SO_RCVBUF) failed - %s", strerror(errno));
  }

  if (setsockopt(sd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0) {
    LOG_VA_WARN("setsockopt(SO_REUSEADDR) failure: %s", strerror(errno));
  }

  // bind socket
  if ((bind(sd, (const sockaddr *)addr, sizeof(sockaddr_in))) < 0) {
    LOG_VA_ERROR("bind() failure: %s", strerror(errno));
    exit(1);
  }

  handlerPtr = datagramHandler = new IOHandlerDatagram(sd, *addr, dispatchHandlerPtr, m_handler_map);

  handlerPtr->get_local_address(addr);

  m_handler_map.insert_datagram_handler(datagramHandler);
  datagramHandler->start_polling();

  return Error::OK;
}


/**
 * 
 */
int Comm::send_datagram(struct sockaddr_in &addr, struct sockaddr_in &sendAddr, CommBufPtr &cbufPtr) {
  boost::mutex::scoped_lock lock(m_mutex);
  IOHandlerDatagramPtr datagramHandlerPtr;
  Header::HeaderT *mheader = (Header::HeaderT *)cbufPtr->data;
  int error = Error::OK;

  cbufPtr->reset_data_pointers();

  if (!m_handler_map.lookup_datagram_handler(sendAddr, datagramHandlerPtr)) {
    std::string str;
    LOG_VA_ERROR("Datagram send address %s not registered", InetAddr::string_format(str, sendAddr));
    DUMP_CORE;
  }

  mheader->flags &= Header::FLAGS_MASK_REQUEST;

  if ((error = datagramHandlerPtr->send_message(addr, cbufPtr)) != Error::OK)
    datagramHandlerPtr->shutdown();

  return error;
}



/**
 *
 */
int Comm::set_timer(uint64_t durationMillis, DispatchHandler *handler) {
  struct TimerT timer;
  boost::xtime_get(&timer.expireTime, boost::TIME_UTC);
  timer.expireTime.sec += durationMillis / 1000LL;
  timer.expireTime.nsec += (durationMillis % 1000LL) * 1000000LL;
  timer.handler = handler;
  m_timer_reactor->add_timer(timer);
  return Error::OK;
}



/**
 *
 */
int Comm::set_timer_absolute(boost::xtime expireTime, DispatchHandler *handler) {
  struct TimerT timer;  
  memcpy(&timer.expireTime, &expireTime, sizeof(boost::xtime));
  timer.handler = handler;
  m_timer_reactor->add_timer(timer);
  return Error::OK;
}

/**
 *
 */
int Comm::get_local_address(struct sockaddr_in addr, struct sockaddr_in *localAddr) {
  boost::mutex::scoped_lock lock(m_mutex);
  IOHandlerDataPtr dataHandlerPtr;

  if (!m_handler_map.lookup_data_handler(addr, dataHandlerPtr)) {
    LOG_VA_ERROR("No connection for %s:%d", inet_ntoa(addr.sin_addr), ntohs(addr.sin_port));
    return Error::COMM_NOT_CONNECTED;
  }

  dataHandlerPtr->get_local_address(localAddr);

  return Error::OK;
}


/**
 * 
 */
int Comm::close_socket(struct sockaddr_in &addr) {
  IOHandlerPtr handlerPtr;

  if (!m_handler_map.decomission_handler(addr, handlerPtr))
    return Error::COMM_NOT_CONNECTED;

  handlerPtr->shutdown();

  return Error::OK;
}


/**
 *  ----- Private methods -----
 */


/**
 *
 */
int Comm::connect_socket(int sd, struct sockaddr_in &addr, DispatchHandlerPtr &defaultHandlerPtr) {
  IOHandlerPtr handlerPtr;
  IOHandlerData *dataHandler;
  int one = 1;

  // Set to non-blocking
  FileUtils::set_flags(sd, O_NONBLOCK);

#if defined(__linux__)
  if (setsockopt(sd, SOL_TCP, TCP_NODELAY, &one, sizeof(one)) < 0)
    cerr << "setsockopt(TCP_NODELAY) failure: " << strerror(errno) << endl;
#elif defined(__APPLE__)
  if (setsockopt(sd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one)) < 0)
    LOG_VA_WARN("setsockopt(SO_NOSIGPIPE) failure: %s", strerror(errno));
#endif

  handlerPtr = dataHandler = new IOHandlerData(sd, addr, defaultHandlerPtr, m_handler_map);
  m_handler_map.insert_handler(dataHandler);

  while (::connect(sd, (struct sockaddr *)&addr, sizeof(struct sockaddr_in)) < 0) {
    if (errno == EINTR) {
      poll(0, 0, 1000);
      continue;
    }
    else if (errno == EINPROGRESS) {
      dataHandler->start_polling();
      dataHandler->add_poll_interest(Reactor::READ_READY|Reactor::WRITE_READY);
      return Error::OK;
    }
    LOG_VA_ERROR("connect() failure : %s", strerror(errno));
    exit(1);
  }

  dataHandler->start_polling();

  return Error::OK;
}
