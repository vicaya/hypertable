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

extern "C" {
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
}

#include "AsyncComm/CallbackHandlerSynchronizer.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"

#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/Properties.h"

#include "HyperspaceClient.h"
#include "HyperspaceProtocol.h"

using namespace hypertable;


namespace {
  const char *DEFAULT_HOST    = "localhost";
  const int   DEFAULT_PORT    = 38547;
  const int   DEFAULT_TIMEOUT = 30;
}



HyperspaceClient::HyperspaceClient(Comm *comm, struct sockaddr_in &addr, time_t timeout) :
  mComm(comm), mAddr(addr), mTimeout(timeout), mConnManager("Waiting for Placerfs server") {
  mConnManager.Initiate(comm, addr, timeout);
  mProtocol = new HyperspaceProtocol();
}



HyperspaceClient::HyperspaceClient(Comm *comm, Properties *props) : mComm(comm), mConnManager("Waiting for Placerfs server") {

  assert(comm);
  assert(props);

  const char *host = props->getProperty("Placerfs.Server.host", DEFAULT_HOST);

  int port = props->getPropertyInt("Placerfs.Server.port", DEFAULT_PORT);

  mTimeout = (time_t)props->getPropertyInt("Placerfs.Client.timeout", DEFAULT_TIMEOUT);

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

  mConnManager.Initiate(mComm, mAddr, mTimeout);

  mProtocol = new HyperspaceProtocol();
}



bool HyperspaceClient::WaitForConnection() {
  if (!mConnManager.WaitForConnection(mTimeout)) {
    LOG_VA_WARN("Timed out waiting for connection to master at %s:%d", inet_ntoa(mAddr.sin_addr), ntohs(mAddr.sin_port));
    return false;
  }
  return true;
}


/**
 * Submit asynchronous 'mkdirs' request
 */
int HyperspaceClient::Mkdirs(const char *name, CallbackHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateMkdirsRequest(name) );
  return SendMessage(cbufPtr, handler, msgIdp);
}



/**
 * Blocking 'mkdirs' request
 */
int HyperspaceClient::Mkdirs(const char *name) {
  CallbackHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateMkdirsRequest(name) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Placerfs 'mkdirs' error, name=%s : %s", name, mProtocol->StringFormatMessage(eventPtr.get()).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr.get());
    }
  }
  return error;
}



/**
 * Submit asynchronous 'create' request
 */
int HyperspaceClient::Create(const char *name, CallbackHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateCreateRequest(name) );
  return SendMessage(cbufPtr, handler, msgIdp);
}



/**
 * Blocking 'create' method
 */
int HyperspaceClient::Create(const char *name) {
  CallbackHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateCreateRequest(name) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Placerfs 'create' error, name=%s : %s", name, mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
  }
  return error;
}


/**
 * Submit asynchronous 'attrset' request
 */
int HyperspaceClient::AttrSet(const char *fname, const char *aname, const char *avalue, CallbackHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateAttrSetRequest(fname, aname, avalue) );
  return SendMessage(cbufPtr, handler, msgIdp);
}


/**
 * Blocking 'attrset' method
 */
int HyperspaceClient::AttrSet(const char *fname, const char *aname, const char *avalue) {
  CallbackHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateAttrSetRequest(fname, aname, avalue) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Placerfs 'attrset' error, fname=%s aname=%s : %s", fname, aname, mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
  }
  return error;
}


/**
 * Submit asynchronous 'attrget' request
 */
int HyperspaceClient::AttrGet(const char *fname, const char *aname, CallbackHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateAttrGetRequest(fname, aname) );
  return SendMessage(cbufPtr, handler, msgIdp);
}



/**
 * Blocking 'attrget' method
 */
int HyperspaceClient::AttrGet(const char *fname, const char *aname, DynamicBuffer &out) {
  CallbackHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateAttrGetRequest(fname, aname) );
  out.clear();
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_WARN("Placerfs 'attrget' error, fname=%s aname=%s : %s", fname, aname, mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
    else {
      if (eventPtr->messageLen < 9) {
	LOG_VA_ERROR("Placerfs 'attrget' error, fname=%s aname=%s : short response", fname, aname);
	error = Error::PROTOCOL_ERROR;
      }
      else {
	const char *avalue;
	const uint8_t *ptr = eventPtr->message;
	CommBuf::DecodeString(&ptr[6], eventPtr->messageLen-6, &avalue);
	if (avalue != 0) {
	  out.reserve(strlen(avalue)+1);
	  out.addNoCheck(avalue, strlen(avalue));
	  *out.ptr = 0;
	}
      }
    }
  }
  return error;
}


/**
 * Submit asynchronous 'attrdel' request
 */
int HyperspaceClient::AttrDel(const char *fname, const char *aname, CallbackHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateAttrDelRequest(fname, aname) );
  return SendMessage(cbufPtr, handler, msgIdp);
}


/**
 * Blocking 'attrdel' method
 */
int HyperspaceClient::AttrDel(const char *fname, const char *aname) {
  CallbackHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateAttrDelRequest(fname, aname) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Placerfs 'attrdel' error, fname=%s aname=%s : %s", fname, aname, mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
  }
  return error;
}


/**
 * Submit asynchronous 'exists' request
 */
int HyperspaceClient::Exists(const char *name, CallbackHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateExistsRequest(name) );
  return SendMessage(cbufPtr, handler, msgIdp);
}


/**
 * Blocking 'exists' method
 */
int HyperspaceClient::Exists(const char *name) {
  CallbackHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateExistsRequest(name) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      error = (int)mProtocol->ResponseCode(eventPtr);
      if (error != Error::HYPERTABLEFS_FILE_NOT_FOUND) {
	LOG_VA_ERROR("Placerfs 'attrdel' error, fname=%s : %s", name, mProtocol->StringFormatMessage(eventPtr).c_str());
      }
    }
  }
  return error;
}


int HyperspaceClient::SendMessage(CommBufPtr &cbufPtr, CallbackHandler *handler, uint32_t *msgIdp) {
  int error;

  if (msgIdp)
    *msgIdp = ((Message::HeaderT *)cbufPtr->data)->id;

  if ((error = mComm->SendRequest(mAddr, cbufPtr, handler)) != Error::OK) {
    LOG_VA_WARN("Comm::SendRequest to %s:%d failed - %s",
		inet_ntoa(mAddr.sin_addr), ntohs(mAddr.sin_port), Error::GetText(error));
  }
  return error;
}
