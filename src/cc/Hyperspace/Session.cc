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
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/Properties.h"

#include "Master.h"
#include "Protocol.h"
#include "Session.h"

using namespace hypertable;
using namespace Hyperspace;

const uint32_t Session::DEFAULT_CLIENT_PORT;

/**
 * 
 */
Session::Session(Comm *comm, PropertiesPtr &propsPtr, SessionCallback *callback) : mComm(comm), mVerbose(false), mSessionCallback(callback) {
  uint16_t masterPort;
  const char *masterHost;

  masterHost = propsPtr->getProperty("Hyperspace.Master.host", "localhost");
  masterPort = (uint16_t)propsPtr->getPropertyInt("Hyperspace.Master.port", Master::DEFAULT_MASTER_PORT);

  if (!InetAddr::Initialize(&mMasterAddr, masterHost, masterPort))
    exit(1);

  mSessionStatePtr = new ClientSessionState();
  mVerbose = propsPtr->getPropertyBool("verbose", false);
  mKeepaliveHandler = new ClientKeepaliveHandler(comm, propsPtr, callback, mSessionStatePtr);
}


bool Session::WaitForConnection(long maxWaitSecs) {
  boost::mutex::scoped_lock lock(mMutex);
  return mSessionStatePtr->WaitForSafe(maxWaitSecs);
}

/**
 * Submit 'mkdir' request
 */
int Session::Mkdir(std::string name) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( Protocol::CreateMkdirRequest(name) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Hyperspace 'mkdir' error, name=%s : %s", name.c_str(), Protocol::StringFormatMessage(eventPtr.get()).c_str());
      error = (int)Protocol::ResponseCode(eventPtr.get());
    }
  }
  return error;
}


int Session::SendMessage(CommBufPtr &cbufPtr, DispatchHandler *handler) {
  int error;

  if ((error = mComm->SendRequest(mMasterAddr, cbufPtr, handler)) != Error::OK) {
    std::string str;
    LOG_VA_WARN("Comm::SendRequest to Hypertable.Master at %s failed - %s",
		InetAddr::StringFormat(str, mMasterAddr), Error::GetText(error));
  }
  return error;
}




#if 0

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

#endif
