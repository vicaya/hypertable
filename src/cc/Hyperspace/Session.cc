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
#include "AsyncComm/Serialization.h"

#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/Properties.h"

#include "ClientSessionHandler.h"
#include "Session.h"
#include "Master.h"

using namespace hypertable;
using namespace Hyperspace;

const uint32_t Session::DEFAULT_CLIENT_PORT;

/**
 * 
 */
Session::Session(Comm *comm, PropertiesPtr &propsPtr, SessionCallback *callback) : mComm(comm), mVerbose(false), mSessionCallback(callback) {
  mSessionStatePtr = new ClientSessionState();
  mVerbose = propsPtr->getPropertyBool("verbose", false);
  mKeepaliveHandler = new ClientKeepaliveHandler(comm, propsPtr, callback, mSessionStatePtr);
}



bool Session::WaitForConnection(long maxWaitSecs) {
  boost::mutex::scoped_lock lock(mMutex);
  boost::xtime dropTime, now;

  boost::xtime_get(&dropTime, boost::TIME_UTC);
  dropTime.sec += maxWaitSecs;

  while (mSessionStatePtr->Get() != ClientSessionState::SAFE) {
    mCond.timed_wait(lock, dropTime);
    boost::xtime_get(&now, boost::TIME_UTC);
    if (xtime_cmp(now, dropTime) >= 0)
      return false;
  }
  return true;
}




#if 0

/**
 * Submit asynchronous 'mkdirs' request
 */
int Session::Mkdirs(const char *name, DispatchHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateMkdirsRequest(name) );
  return SendMessage(cbufPtr, handler, msgIdp);
}



/**
 * Blocking 'mkdirs' request
 */
int Session::Mkdirs(const char *name) {
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
int Session::Create(const char *name, DispatchHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateCreateRequest(name) );
  return SendMessage(cbufPtr, handler, msgIdp);
}



/**
 * Blocking 'create' method
 */
int Session::Create(const char *name) {
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
int Session::AttrSet(const char *fname, const char *aname, const char *avalue, DispatchHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateAttrSetRequest(fname, aname, avalue) );
  return SendMessage(cbufPtr, handler, msgIdp);
}


/**
 * Blocking 'attrset' method
 */
int Session::AttrSet(const char *fname, const char *aname, const char *avalue) {
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
int Session::AttrGet(const char *fname, const char *aname, DispatchHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateAttrGetRequest(fname, aname) );
  return SendMessage(cbufPtr, handler, msgIdp);
}



/**
 * Blocking 'attrget' method
 */
int Session::AttrGet(const char *fname, const char *aname, DynamicBuffer &out) {
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
	uint8_t *ptr = eventPtr->message + 4;
	size_t remaining = eventPtr->messageLen - 4;
	if (!Serialization::DecodeString(&ptr, &remaining, &avalue))
	  assert(!"problem decoding return packet");
	if (*avalue != 0) {
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
int Session::AttrDel(const char *fname, const char *aname, DispatchHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateAttrDelRequest(fname, aname) );
  return SendMessage(cbufPtr, handler, msgIdp);
}


/**
 * Blocking 'attrdel' method
 */
int Session::AttrDel(const char *fname, const char *aname) {
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
int Session::Exists(const char *name, DispatchHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateExistsRequest(name) );
  return SendMessage(cbufPtr, handler, msgIdp);
}


/**
 * Blocking 'exists' method
 */
int Session::Exists(const char *name) {
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
int Session::Delete(const char *name, DispatchHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateDeleteRequest(name) );
  return SendMessage(cbufPtr, handler, msgIdp);
}


/**
 * Blocking 'delete' method
 */
int Session::Delete(const char *name) {
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
int Session::Status() {
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


int Session::SendMessage(CommBufPtr &cbufPtr, DispatchHandler *handler, uint32_t *msgIdp) {
  int error;

  if (msgIdp)
    *msgIdp = ((Header::HeaderT *)cbufPtr->data)->id;

  if ((error = mComm->SendRequest(mAddr, cbufPtr, handler)) != Error::OK) {
    LOG_VA_WARN("Comm::SendRequest to %s:%d failed - %s",
		inet_ntoa(mAddr.sin_addr), ntohs(mAddr.sin_port), Error::GetText(error));
  }
  return error;
}


#endif
