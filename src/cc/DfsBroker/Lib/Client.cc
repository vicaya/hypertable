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

#include <cassert>

#include "AsyncComm/Comm.h"
#include "AsyncComm/HeaderBuilder.h"

#include "Common/Error.h"
#include "Common/Logger.h"

#include "Client.h"

using namespace hypertable;
using namespace hypertable::DfsBroker;


Client::Client(ConnectionManager *connManager, struct sockaddr_in &addr, time_t timeout) : mConnectionManager(connManager), mAddr(addr), mTimeout(timeout) {
  mComm = mConnectionManager->GetComm();
  mProtocol = new Protocol();
  mConnectionManager->Add(mAddr, mTimeout, "DFS Broker");
}



Client::Client(ConnectionManager *connManager, PropertiesPtr &propsPtr) : mConnectionManager(connManager) {
  const char *host;
  uint16_t port;

  mComm = mConnectionManager->GetComm();

  mProtocol = new Protocol();

  {
    if ((port = (uint16_t)propsPtr->getPropertyInt("DfsBroker.port", 0)) == 0) {
      LOG_ERROR(".port property not specified.");
      exit(1);
    }

    if ((host = propsPtr->getProperty("DfsBroker.host", (const char *)0)) == 0) {
      LOG_ERROR(".host property not specified.");
      exit(1);
    }

    mTimeout = propsPtr->getPropertyInt("DfsBroker.timeout", 30);

    memset(&mAddr, 0, sizeof(struct sockaddr_in));
    {
      struct hostent *he = gethostbyname(host);
      if (he == 0) {
	herror("gethostbyname()");
	exit(1);
      }
      memcpy(&mAddr.sin_addr.s_addr, he->h_addr_list[0], sizeof(uint32_t));
    }
    mAddr.sin_family = AF_INET;
    mAddr.sin_port = htons(port);
  }

  mConnectionManager->Add(mAddr, mTimeout, "DFS Broker");
  
}


int Client::Open(std::string &name, DispatchHandler *handler) {
  CommBufPtr cbufPtr( mProtocol->CreateOpenRequest(name, 0) );
  return SendMessage(cbufPtr, handler);
}



int Client::Open(std::string &name, int32_t *fdp) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  *fdp = -1;
  CommBufPtr cbufPtr( mProtocol->CreateOpenRequest(name, 0) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Dfs 'open' error, name=%s : %s", name.c_str(), mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
    else
      *fdp = ((Protocol::ResponseHeaderOpenT *)eventPtr->message)->handle;
  }
  return error;
}


int Client::Create(std::string &name, bool overwrite, int32_t bufferSize,
		   int32_t replication, int64_t blockSize, DispatchHandler *handler) {
  CommBufPtr cbufPtr( mProtocol->CreateCreateRequest(name, overwrite, bufferSize, replication, blockSize) );
  return SendMessage(cbufPtr, handler);
}


int Client::Create(std::string &name, bool overwrite, int32_t bufferSize,
		   int32_t replication, int64_t blockSize, int32_t *fdp) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  *fdp = -1;
  CommBufPtr cbufPtr( mProtocol->CreateCreateRequest(name, overwrite, bufferSize, replication, blockSize) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Dfs 'create' error, name=%s : %s", name.c_str(), mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
    else
      *fdp = ((Protocol::ResponseHeaderCreateT *)eventPtr->message)->handle;
  }
  return error;
}



int Client::Close(int32_t fd, DispatchHandler *handler) {
  CommBufPtr cbufPtr( mProtocol->CreateCloseRequest(fd) );
  return SendMessage(cbufPtr, handler);
}



int Client::Close(int32_t fd) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateCloseRequest(fd) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Dfs 'close' error, fd=%d : %s", fd, mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
  }
  return error;
}



int Client::Read(int32_t fd, uint32_t amount, DispatchHandler *handler) {
  CommBufPtr cbufPtr( mProtocol->CreateReadRequest(fd, amount) );
  return SendMessage(cbufPtr, handler);
}



int Client::Read(int32_t fd, uint32_t amount, uint8_t *dst, uint32_t *nreadp) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateReadRequest(fd, amount) );
  int error = SendMessage(cbufPtr, &syncHandler);
  *nreadp = 0;
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Dfs 'read' error (amount=%d, fd=%d) : %s",
		   amount, fd, mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
    else {
      Protocol::ResponseHeaderReadT *readHeader = (Protocol::ResponseHeaderReadT *)eventPtr->message;
      *nreadp = readHeader->amount;
      memcpy(dst, &readHeader[1], readHeader->amount);
    }
  }
  return error;
}



int Client::Append(int32_t fd, const void *buf, uint32_t amount, DispatchHandler *handler) {
  CommBufPtr cbufPtr( mProtocol->CreateAppendRequest(fd, buf, amount) );
  return SendMessage(cbufPtr, handler);
}



int Client::Append(int32_t fd, const void *buf, uint32_t amount) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateAppendRequest(fd, buf, amount) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Dfs 'append' error, fd=%d, amount=%d : %s", fd, amount, mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
    else if (((Protocol::ResponseHeaderAppendT *)eventPtr->message)->amount != (int32_t)amount) {
      LOG_VA_ERROR("Short DFS file append fd=%d : tried to append %d but only got %d", fd, amount,
		   ((Protocol::ResponseHeaderAppendT *)eventPtr->message)->amount);
      error = Error::DFSBROKER_IO_ERROR;
    }
  }
  return error;
}


int Client::Seek(int32_t fd, uint64_t offset, DispatchHandler *handler) {
  CommBufPtr cbufPtr( mProtocol->CreateSeekRequest(fd, offset) );
  return SendMessage(cbufPtr, handler);
}


int Client::Seek(int32_t fd, uint64_t offset) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateSeekRequest(fd, offset) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Dfs 'seek' error, fd=%d, offset=%lld : %s", fd, offset, mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
  }
  return error;
}


int Client::Remove(std::string &name, DispatchHandler *handler) {
  CommBufPtr cbufPtr( mProtocol->CreateRemoveRequest(name) );
  return SendMessage(cbufPtr, handler);
}


int Client::Remove(std::string &name) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateRemoveRequest(name) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Dfs 'remove' error, name=%s : %s", name.c_str(), mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
  }
  return error;
}



int Client::Shutdown(uint16_t flags, DispatchHandler *handler) {
  CommBufPtr cbufPtr( mProtocol->CreateShutdownRequest(flags) );
  return SendMessage(cbufPtr, handler);
}


int Client::Status() {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateStatusRequest() );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR(" 'status' error : %s", mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
  }
  return error;
}


int Client::Length(std::string &name, DispatchHandler *handler) {
  CommBufPtr cbufPtr( mProtocol->CreateLengthRequest(name) );
  return SendMessage(cbufPtr, handler);
}



int Client::Length(std::string &name, int64_t *lenp) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateLengthRequest(name) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Dfs 'length' error, name=%s : %s", name.c_str(), mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
    else
      *lenp = ((Protocol::ResponseHeaderLengthT *)eventPtr->message)->length;
  }
  return error;
}



int Client::Pread(int32_t fd, uint64_t offset, uint32_t amount, DispatchHandler *handler) {
  CommBufPtr cbufPtr( mProtocol->CreatePositionReadRequest(fd, offset, amount) );
  return SendMessage(cbufPtr, handler);
}



int Client::Pread(int32_t fd, uint64_t offset, uint32_t amount, uint8_t *dst, uint32_t *nreadp) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreatePositionReadRequest(fd, offset, amount) );
  int error = SendMessage(cbufPtr, &syncHandler);
  *nreadp = 0;
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Dfs 'pread' error (offset=%lld, amount=%d, fd=%d) : %s",
		   offset, amount, fd, mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
    else {
      Protocol::ResponseHeaderReadT *readHeader = (Protocol::ResponseHeaderReadT *)eventPtr->message;
      *nreadp = readHeader->amount;
      memcpy(dst, &readHeader[1], readHeader->amount);
    }
  }
  return error;
}


int Client::Mkdirs(std::string &name, DispatchHandler *handler) {
  CommBufPtr cbufPtr( mProtocol->CreateMkdirsRequest(name) );
  return SendMessage(cbufPtr, handler);
}


int Client::Mkdirs(std::string &name) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateMkdirsRequest(name) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Dfs 'mkdirs' error, name=%s : %s", name.c_str(), mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
  }
  return error;
}


int Client::Flush(int32_t fd, DispatchHandler *handler) {
  CommBufPtr cbufPtr( mProtocol->CreateFlushRequest(fd) );
  return SendMessage(cbufPtr, handler);
}


int Client::Flush(int32_t fd) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateFlushRequest(fd) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Dfs 'flush' error, fd=%d : %s", fd, mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
  }
  return error;
}


int Client::Rmdir(std::string &name, DispatchHandler *handler) {
  CommBufPtr cbufPtr( mProtocol->CreateRmdirRequest(name) );
  return SendMessage(cbufPtr, handler);
}


int Client::Rmdir(std::string &name) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateRmdirRequest(name) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Dfs 'rmdir' error, name=%s : %s", name.c_str(), mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
  }
  return error;
}


int Client::SendMessage(CommBufPtr &cbufPtr, DispatchHandler *handler) {
  int error;

  if ((error = mComm->SendRequest(mAddr, mTimeout, cbufPtr, handler)) != Error::OK) {
    LOG_VA_WARN("Comm::SendRequest to %s:%d failed - %s",
		inet_ntoa(mAddr.sin_addr), ntohs(mAddr.sin_port), Error::GetText(error));
  }
  return error;
}
