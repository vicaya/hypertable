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

extern "C" {
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
}

#include "AsyncComm/DispatchHandlerSynchronizer.h"
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



HyperspaceClient::HyperspaceClient(ConnectionManager *connManager, struct sockaddr_in &addr, time_t timeout) : mConnManager(connManager), mAddr(addr), mTimeout(timeout) {

  mComm = mConnManager->GetComm();

  mConnManager->Add(mAddr, mTimeout, "Hyperspace");

  mProtocol = new HyperspaceProtocol();
}



HyperspaceClient::HyperspaceClient(ConnectionManager *connManager, Properties *props) : mConnManager(connManager) {

  mComm = mConnManager->GetComm();

  const char *host = props->getProperty("Hyperspace.host", DEFAULT_HOST);

  int port = props->getPropertyInt("Hyperspace.port", DEFAULT_PORT);

  mTimeout = (time_t)props->getPropertyInt("Hyperspace.Client.timeout", DEFAULT_TIMEOUT);

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

  mConnManager->Add(mAddr, 15, "Hyperspace");

  mProtocol = new HyperspaceProtocol();
}



bool HyperspaceClient::WaitForConnection(long maxWaitSecs) {
  return mConnManager->WaitForConnection(mAddr, maxWaitSecs);
}


/**
 * Submit asynchronous 'mkdirs' request
 */
int HyperspaceClient::Mkdirs(const char *name, DispatchHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateMkdirsRequest(name) );
  return SendMessage(cbufPtr, handler, msgIdp);
}



/**
 * Blocking 'mkdirs' request
 */
int HyperspaceClient::Mkdirs(const char *name) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateMkdirsRequest(name) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Hyperspace 'mkdirs' error, name=%s : %s", name, mProtocol->StringFormatMessage(eventPtr.get()).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr.get());
    }
  }
  return error;
}



/**
 * Submit asynchronous 'create' request
 */
int HyperspaceClient::Create(const char *name, DispatchHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateCreateRequest(name) );
  return SendMessage(cbufPtr, handler, msgIdp);
}



/**
 * Blocking 'create' method
 */
int HyperspaceClient::Create(const char *name) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateCreateRequest(name) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Hyperspace 'create' error, name=%s : %s", name, mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
  }
  return error;
}


/**
 * Submit asynchronous 'attrset' request
 */
int HyperspaceClient::AttrSet(const char *fname, const char *aname, const char *avalue, DispatchHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateAttrSetRequest(fname, aname, avalue) );
  return SendMessage(cbufPtr, handler, msgIdp);
}


/**
 * Blocking 'attrset' method
 */
int HyperspaceClient::AttrSet(const char *fname, const char *aname, const char *avalue) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateAttrSetRequest(fname, aname, avalue) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Hyperspace 'attrset' error, fname=%s aname=%s : %s", fname, aname, mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
  }
  return error;
}


/**
 * Submit asynchronous 'attrget' request
 */
int HyperspaceClient::AttrGet(const char *fname, const char *aname, DispatchHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateAttrGetRequest(fname, aname) );
  return SendMessage(cbufPtr, handler, msgIdp);
}



/**
 * Blocking 'attrget' method
 */
int HyperspaceClient::AttrGet(const char *fname, const char *aname, DynamicBuffer &out) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateAttrGetRequest(fname, aname) );
  out.clear();
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_WARN("Hyperspace 'attrget' error, fname=%s aname=%s : %s", fname, aname, mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
    else {
      if (eventPtr->messageLen < 7) {
	LOG_VA_ERROR("Hyperspace 'attrget' error, fname=%s aname=%s : short response", fname, aname);
	error = Error::PROTOCOL_ERROR;
      }
      else {
	const char *avalue;
	const uint8_t *ptr = eventPtr->message;
	CommBuf::DecodeString(&ptr[4], eventPtr->messageLen-4, &avalue);
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
int HyperspaceClient::AttrDel(const char *fname, const char *aname, DispatchHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateAttrDelRequest(fname, aname) );
  return SendMessage(cbufPtr, handler, msgIdp);
}


/**
 * Blocking 'attrdel' method
 */
int HyperspaceClient::AttrDel(const char *fname, const char *aname) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateAttrDelRequest(fname, aname) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Hyperspace 'attrdel' error, fname=%s aname=%s : %s", fname, aname, mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
  }
  return error;
}


/**
 * Submit asynchronous 'exists' request
 */
int HyperspaceClient::Exists(const char *name, DispatchHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateExistsRequest(name) );
  return SendMessage(cbufPtr, handler, msgIdp);
}


/**
 * Blocking 'exists' method
 */
int HyperspaceClient::Exists(const char *name) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateExistsRequest(name) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      error = (int)mProtocol->ResponseCode(eventPtr);
      if (error != Error::HYPERSPACE_FILE_NOT_FOUND) {
	LOG_VA_ERROR("Hyperspace 'attrdel' error, fname=%s : %s", name, mProtocol->StringFormatMessage(eventPtr).c_str());
      }
    }
  }
  return error;
}


/**
 * Submit asynchronous 'delete' request
 */
int HyperspaceClient::Delete(const char *name, DispatchHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateDeleteRequest(name) );
  return SendMessage(cbufPtr, handler, msgIdp);
}


/**
 * Blocking 'delete' method
 */
int HyperspaceClient::Delete(const char *name) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateDeleteRequest(name) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      error = (int)mProtocol->ResponseCode(eventPtr);
      if (error != Error::HYPERSPACE_FILE_NOT_FOUND) {
	LOG_VA_ERROR("Hyperspace 'attrdel' error, fname=%s : %s", name, mProtocol->StringFormatMessage(eventPtr).c_str());
      }
    }
  }
  return error;
}


/**
 * Blocking 'status' method
 */
int HyperspaceClient::Status() {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateStatusRequest() );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      error = (int)mProtocol->ResponseCode(eventPtr);
      LOG_VA_ERROR("Hyperspace 'status' error : %s", mProtocol->StringFormatMessage(eventPtr).c_str());
    }
  }
  return error;
}


int HyperspaceClient::SendMessage(CommBufPtr &cbufPtr, DispatchHandler *handler, uint32_t *msgIdp) {
  int error;

  if (msgIdp)
    *msgIdp = ((Header::HeaderT *)cbufPtr->data)->id;

  if ((error = mComm->SendRequest(mAddr, cbufPtr, handler)) != Error::OK) {
    LOG_VA_WARN("Comm::SendRequest to %s:%d failed - %s",
		inet_ntoa(mAddr.sin_addr), ntohs(mAddr.sin_port), Error::GetText(error));
  }
  return error;
}
