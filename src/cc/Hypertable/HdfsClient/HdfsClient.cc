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

#include <boost/thread/thread.hpp>

#include "AsyncComm/Comm.h"
#include "AsyncComm/HeaderBuilder.h"

#include "Common/Error.h"
#include "Common/Logger.h"

#include "HdfsClient.h"

using namespace hypertable;


HdfsClient::HdfsClient(Comm *comm, struct sockaddr_in &addr, time_t timeout) :
  mComm(comm), mAddr(addr), mTimeout(timeout), mConnectionManager("Waiting for HdfsBroker") {
  mProtocol = new HdfsProtocol();
  mConnectionManager.Initiate(comm, addr, timeout);
}


int HdfsClient::Open(const char *name, CallbackHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateOpenRequest(name, 0) );
  return SendMessage(cbufPtr, handler, msgIdp);
}



int HdfsClient::Open(const char *name, int32_t *fdp) {
  CallbackHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  *fdp = -1;
  CommBufPtr cbufPtr( mProtocol->CreateOpenRequest(name, 0) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Hdfs 'open' error, name=%s : %s", name, mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
    else
      *fdp = ((HdfsProtocol::ResponseHeaderOpenT *)eventPtr->message)->handle;
  }
  return error;
}


int HdfsClient::Create(const char *name, bool overwrite, int32_t bufferSize,
		   int32_t replication, int64_t blockSize, CallbackHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateCreateRequest(name, overwrite, bufferSize, replication, blockSize) );
  return SendMessage(cbufPtr, handler, msgIdp);
}


int HdfsClient::Create(const char *name, bool overwrite, int32_t bufferSize,
		   int32_t replication, int64_t blockSize, int32_t *fdp) {
  CallbackHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  *fdp = -1;
  CommBufPtr cbufPtr( mProtocol->CreateCreateRequest(name, overwrite, bufferSize, replication, blockSize) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Hdfs 'create' error, name=%s : %s", name, mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
    else
      *fdp = ((HdfsProtocol::ResponseHeaderCreateT *)eventPtr->message)->handle;
  }
  return error;
}



int HdfsClient::Close(int32_t fd, CallbackHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateCloseRequest(fd) );
  return SendMessage(cbufPtr, handler, msgIdp);
}



int HdfsClient::Close(int32_t fd) {
  CallbackHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateCloseRequest(fd) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Hdfs 'close' error, fd=%d : %s", fd, mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
  }
  return error;
}



int HdfsClient::Read(int32_t fd, uint32_t amount, CallbackHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateReadRequest(fd, amount) );
  return SendMessage(cbufPtr, handler, msgIdp);
}



int HdfsClient::Read(int32_t fd, uint32_t amount, uint8_t *dst, uint32_t *nreadp) {
  CallbackHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateReadRequest(fd, amount) );
  int error = SendMessage(cbufPtr, &syncHandler);
  *nreadp = 0;
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Hdfs 'read' error (amount=%d, fd=%d) : %s",
		   amount, fd, mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
    else {
      HdfsProtocol::ResponseHeaderReadT *readHeader = (HdfsProtocol::ResponseHeaderReadT *)eventPtr->message;
      *nreadp = readHeader->amount;
      memcpy(dst, &readHeader[1], readHeader->amount);
    }
  }
  return error;
}



int HdfsClient::Write(int32_t fd, uint8_t *buf, uint32_t amount, CallbackHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateWriteRequest(fd, buf, amount) );
  return SendMessage(cbufPtr, handler, msgIdp);
}



int HdfsClient::Write(int32_t fd, uint8_t *buf, uint32_t amount) {
  CallbackHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateWriteRequest(fd, buf, amount) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Hdfs 'write' error, fd=%d, amount=%d : %s", fd, amount, mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
    else if (((HdfsProtocol::ResponseHeaderWriteT *)eventPtr->message)->amount != (int32_t)amount) {
      LOG_VA_ERROR("Short HDFS file write fd=%d : tried to write %d but only got %d", fd, amount,
		   ((HdfsProtocol::ResponseHeaderWriteT *)eventPtr->message)->amount);
      error = Error::HDFSBROKER_IO_ERROR;
    }
  }
  return error;
}


int HdfsClient::Seek(int32_t fd, uint64_t offset, CallbackHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateSeekRequest(fd, offset) );
  return SendMessage(cbufPtr, handler, msgIdp);
}


int HdfsClient::Seek(int32_t fd, uint64_t offset) {
  CallbackHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateSeekRequest(fd, offset) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Hdfs 'seek' error, fd=%d, offset=%lld : %s", fd, offset, mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
  }
  return error;
}


int HdfsClient::Remove(const char *name, CallbackHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateRemoveRequest(name) );
  return SendMessage(cbufPtr, handler, msgIdp);
}


int HdfsClient::Remove(const char *name) {
  CallbackHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateRemoveRequest(name) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Hdfs 'remove' error, name=%s : %s", name, mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
  }
  return error;
}



int HdfsClient::Shutdown(uint16_t flags, CallbackHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateShutdownRequest(flags) );
  return SendMessage(cbufPtr, handler, msgIdp);
}



int HdfsClient::Length(const char *name, CallbackHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateLengthRequest(name) );
  return SendMessage(cbufPtr, handler, msgIdp);
}



int HdfsClient::Length(const char *name, int64_t *lenp) {
  CallbackHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateLengthRequest(name) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Hdfs 'length' error, name=%s : %s", name, mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
    else
      *lenp = ((HdfsProtocol::ResponseHeaderLengthT *)eventPtr->message)->length;
  }
  return error;
}



int HdfsClient::Pread(int32_t fd, uint64_t offset, uint32_t amount, CallbackHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreatePositionReadRequest(fd, offset, amount) );
  return SendMessage(cbufPtr, handler, msgIdp);
}



int HdfsClient::Pread(int32_t fd, uint64_t offset, uint32_t amount, uint8_t *dst, uint32_t *nreadp) {
  CallbackHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreatePositionReadRequest(fd, offset, amount) );
  int error = SendMessage(cbufPtr, &syncHandler);
  *nreadp = 0;
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Hdfs 'pread' error (offset=%lld, amount=%d, fd=%d) : %s",
		   offset, amount, fd, mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
    else {
      HdfsProtocol::ResponseHeaderReadT *readHeader = (HdfsProtocol::ResponseHeaderReadT *)eventPtr->message;
      *nreadp = readHeader->amount;
      memcpy(dst, &readHeader[1], readHeader->amount);
    }
  }
  return error;
}


int HdfsClient::Mkdirs(const char *name, CallbackHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateMkdirsRequest(name) );
  return SendMessage(cbufPtr, handler, msgIdp);
}


int HdfsClient::Mkdirs(const char *name) {
  CallbackHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateMkdirsRequest(name) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Hdfs 'mkdirs' error, name=%s : %s", name, mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
  }
  return error;
}



int HdfsClient::SendMessage(CommBufPtr &cbufPtr, CallbackHandler *handler, uint32_t *msgIdp) {
  int error;

  if (msgIdp)
    *msgIdp = ((Header::HeaderT *)cbufPtr->data)->id;

  if ((error = mComm->SendRequest(mAddr, cbufPtr, handler)) != Error::OK) {
    LOG_VA_WARN("Comm::SendRequest to %s:%d failed - %s",
		inet_ntoa(mAddr.sin_addr), ntohs(mAddr.sin_port), Error::GetText(error));
  }
  return error;
}
