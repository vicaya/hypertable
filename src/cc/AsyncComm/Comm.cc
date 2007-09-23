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
  if (ReactorFactory::msReactors.size() == 0) {
    LOG_ERROR("ReactorFactory::Initialize must be called before creating AsyncComm::Comm object");
    DUMP_CORE;
  }
  mTimerReactor = ReactorFactory::GetReactor();
}



/**
 *
 */
Comm::~Comm() {

  set<IOHandler *> handlers;
  mHandlerMap.DecomissionAll(handlers);
  for (set<IOHandler *>::iterator iter = handlers.begin(); iter != handlers.end(); ++iter)
    (*iter)->Shutdown();

  // wait for all decomissioned handlers to get purged by Reactor
  mHandlerMap.WaitForEmpty();
}



/**
 */
int Comm::Connect(struct sockaddr_in &addr, time_t timeout, DispatchHandler *defaultHandler) {
  int sd;
  IOHandlerPtr handlerPtr;
  IOHandlerData *dataHandler;
  int one = 1;

  if (mHandlerMap.ContainsHandler(addr))
    return Error::COMM_ALREADY_CONNECTED;

  if ((sd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
    cerr << "socket() failure: " << strerror(errno) << endl;
    exit(1);
  }

  // Set to non-blocking
  FileUtils::SetFlags(sd, O_NONBLOCK);

#if defined(__linux__)
  if (setsockopt(sd, SOL_TCP, TCP_NODELAY, &one, sizeof(one)) < 0)
    cerr << "setsockopt(TCP_NODELAY) failure: " << strerror(errno) << endl;
#elif defined(__APPLE__)
  if (setsockopt(sd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one)) < 0)
    LOG_VA_WARN("setsockopt(SO_NOSIGPIPE) failure: %s", strerror(errno));
#endif

  handlerPtr = dataHandler = new IOHandlerData(sd, addr, defaultHandler, mHandlerMap);
  mHandlerMap.InsertHandler(dataHandler);
  dataHandler->SetTimeout(timeout);

  while (connect(sd, (struct sockaddr *)&addr, sizeof(struct sockaddr_in)) < 0) {
    if (errno == EINTR) {
      poll(0, 0, 1000);
      continue;
    }
    else if (errno == EINPROGRESS) {
      dataHandler->StartPolling();
      dataHandler->AddPollInterest(Reactor::READ_READY|Reactor::WRITE_READY);
      return Error::OK;
    }
    LOG_VA_ERROR("connect() failure : %s", strerror(errno));
    exit(1);
  }

  dataHandler->StartPolling();

  return Error::OK;
}



/**
 *
 */
int Comm::Listen(struct sockaddr_in &addr, ConnectionHandlerFactory *hfactory, DispatchHandler *defaultHandler) {
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

  if (listen(sd, 64) < 0) {
    cerr << "listen() failure: " << strerror(errno) << endl;
    exit(1);
  }

  handlerPtr = acceptHandler = new IOHandlerAccept(sd, addr, defaultHandler, mHandlerMap, hfactory);
  mHandlerMap.InsertHandler(acceptHandler);
  acceptHandler->StartPolling();

  return Error::OK;
}



int Comm::SendRequest(struct sockaddr_in &addr, CommBufPtr &cbufPtr, DispatchHandler *responseHandler) {
  boost::mutex::scoped_lock lock(mMutex);
  IOHandlerDataPtr dataHandlerPtr;
  Header::HeaderT *mheader = (Header::HeaderT *)cbufPtr->data;
  int error = Error::OK;

  cbufPtr->ResetDataPointers();

  if (!mHandlerMap.LookupDataHandler(addr, dataHandlerPtr)) {
    LOG_VA_ERROR("No connection for %s:%d", inet_ntoa(addr.sin_addr), ntohs(addr.sin_port));
    return Error::COMM_NOT_CONNECTED;
  }

  mheader->flags |= Header::FLAGS_MASK_REQUEST;

  if ((error = dataHandlerPtr->SendMessage(cbufPtr, responseHandler)) != Error::OK)
    dataHandlerPtr->Shutdown();

  return error;
}


int Comm::SendResponse(struct sockaddr_in &addr, CommBufPtr &cbufPtr) {
  boost::mutex::scoped_lock lock(mMutex);
  IOHandlerDataPtr dataHandlerPtr;
  Header::HeaderT *mheader = (Header::HeaderT *)cbufPtr->data;
  int error = Error::OK;

  cbufPtr->ResetDataPointers();

  if (!mHandlerMap.LookupDataHandler(addr, dataHandlerPtr)) {
    LOG_VA_ERROR("No connection for %s:%d", inet_ntoa(addr.sin_addr), ntohs(addr.sin_port));
    return Error::COMM_NOT_CONNECTED;
  }

  mheader->flags &= Header::FLAGS_MASK_RESPONSE;

  if ((error = dataHandlerPtr->SendMessage(cbufPtr)) != Error::OK)
    dataHandlerPtr->Shutdown();

  return error;
}



/**
 * 
 */
int Comm::CreateDatagramReceiveSocket(struct sockaddr_in &addr, DispatchHandler *handler) {
  IOHandlerPtr handlerPtr;
  IOHandlerDatagram *datagramHandler;
  int one = 1;
  int sd;

  if ((sd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    LOG_VA_ERROR("socket() failure: %s", strerror(errno));
    exit(1);
  }

  // Set to non-blocking
  FileUtils::SetFlags(sd, O_NONBLOCK);

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
  if ((bind(sd, (const sockaddr *)&addr, sizeof(sockaddr_in))) < 0) {
    LOG_VA_ERROR("bind() failure: %s", strerror(errno));
    exit(1);
  }

  handlerPtr = datagramHandler = new IOHandlerDatagram(sd, addr, handler, mHandlerMap);
  mHandlerMap.InsertDatagramHandler(datagramHandler);
  datagramHandler->StartPolling();

  return Error::OK;
}


/**
 * 
 */
int Comm::SendDatagram(struct sockaddr_in &addr, struct sockaddr_in &sendAddr, CommBufPtr &cbufPtr) {
  boost::mutex::scoped_lock lock(mMutex);
  IOHandlerDatagramPtr datagramHandlerPtr;
  Header::HeaderT *mheader = (Header::HeaderT *)cbufPtr->data;
  int error = Error::OK;

  cbufPtr->ResetDataPointers();

  if (!mHandlerMap.LookupDatagramHandler(sendAddr, datagramHandlerPtr)) {
    std::string str;
    LOG_VA_ERROR("Datagram send address %s not registered", InetAddr::StringFormat(str, sendAddr));
    DUMP_CORE;
  }

  mheader->flags &= Header::FLAGS_MASK_REQUEST;

  if ((error = datagramHandlerPtr->SendMessage(addr, cbufPtr)) != Error::OK)
    datagramHandlerPtr->Shutdown();

  return error;
}



/**
 *
 */
int Comm::SetTimer(uint64_t durationMillis, DispatchHandler *handler) {
  struct TimerT timer;
  boost::xtime_get(&timer.expireTime, boost::TIME_UTC);
  timer.expireTime.sec += durationMillis / 1000LL;
  timer.expireTime.nsec += (durationMillis % 1000LL) * 1000000LL;
  timer.handler = handler;
  mTimerReactor->AddTimer(timer);
  return Error::OK;
}



/**
 *
 */
int Comm::SetTimerAbsolute(boost::xtime expireTime, DispatchHandler *handler) {
  struct TimerT timer;  
  memcpy(&timer.expireTime, &expireTime, sizeof(boost::xtime));
  timer.handler = handler;
  mTimerReactor->AddTimer(timer);
  return Error::OK;
}

/**
 *
 */
int Comm::GetLocalAddress(struct sockaddr_in addr, struct sockaddr_in *localAddr) {
  boost::mutex::scoped_lock lock(mMutex);
  IOHandlerDataPtr dataHandlerPtr;

  if (!mHandlerMap.LookupDataHandler(addr, dataHandlerPtr)) {
    LOG_VA_ERROR("No connection for %s:%d", inet_ntoa(addr.sin_addr), ntohs(addr.sin_port));
    return Error::COMM_NOT_CONNECTED;
  }

  dataHandlerPtr->GetLocalAddress(localAddr);

  return Error::OK;
}


/**
 * 
 */
int Comm::CloseSocket(struct sockaddr_in &addr) {
  IOHandlerPtr handlerPtr;

  if (!mHandlerMap.DecomissionHandler(addr, handlerPtr))
    return Error::COMM_NOT_CONNECTED;

  handlerPtr->Shutdown();

  return Error::OK;
}


