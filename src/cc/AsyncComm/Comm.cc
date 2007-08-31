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
}



/**
 *
 */
Comm::~Comm() {
  mHandlerMap.UnregisterAll();
}



/**
 */
int Comm::Connect(struct sockaddr_in &addr, time_t timeout, DispatchHandler *defaultHandler) {
  int sd;
  IOHandlerDataPtr dataHandlerPtr;

  if (mHandlerMap.LookupDataHandler(addr, dataHandlerPtr))
    return Error::COMM_ALREADY_CONNECTED;

  if ((sd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
    cerr << "socket() failure: " << strerror(errno) << endl;
    exit(1);
  }

  // Set to non-blocking
  FileUtils::SetFlags(sd, O_NONBLOCK);

#if defined(__APPLE__)
  int one = 1;
  if (setsockopt(sd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one)) < 0)
    LOG_VA_WARN("setsockopt(SO_NOSIGPIPE) failure: %s", strerror(errno));
#endif

  dataHandlerPtr.reset( new IOHandlerData(sd, addr, defaultHandler, mHandlerMap) );
  mHandlerMap.InsertDataHandler(dataHandlerPtr);
  dataHandlerPtr->SetTimeout(timeout);

  while (connect(sd, (struct sockaddr *)&addr, sizeof(struct sockaddr_in)) < 0) {
    if (errno == EINTR) {
      poll(0, 0, 1000);
      continue;
    }
    else if (errno == EINPROGRESS) {
      dataHandlerPtr->StartPolling();
      dataHandlerPtr->AddPollInterest(Reactor::READ_READY|Reactor::WRITE_READY);
      return Error::OK;
    }
    LOG_VA_ERROR("connect() failure : %s", strerror(errno));
    exit(1);
  }

  dataHandlerPtr->StartPolling();

  return Error::OK;
}


/**
 *
 */
int Comm::Listen(uint16_t port, ConnectionHandlerFactory *hfactory, DispatchHandler *defaultHandler) {
  struct sockaddr_in addr;
  int one = 1;
  int sd;

  port = htons(port);

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

  // create address structure to bind to - any available port - any address
  memset(&addr, 0 , sizeof(sockaddr_in));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = port;

  // bind socket
  if ((bind(sd, (const sockaddr *)&addr, sizeof(sockaddr_in))) < 0) {
    cerr << "bind() failure: " << strerror(errno) << endl;
    exit(1);
  }

  if (listen(sd, 64) < 0) {
    cerr << "listen() failure: " << strerror(errno) << endl;
    exit(1);
  }

  IOHandlerAcceptPtr acceptHandlerPtr( new IOHandlerAccept(sd, addr, defaultHandler, mHandlerMap, hfactory) );
  mHandlerMap.InsertAcceptHandler(acceptHandlerPtr);
  acceptHandlerPtr->StartPolling();

  return 0;
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
