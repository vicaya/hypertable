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

extern "C" {
#include <poll.h>
}

#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/Exception.h"
#include "Common/StringExt.h"

#include "ClientKeepaliveHandler.h"
#include "Master.h"
#include "Protocol.h"
#include "Session.h"

using namespace hypertable;
using namespace Hyperspace;

ClientKeepaliveHandler::ClientKeepaliveHandler(Comm *comm, PropertiesPtr &propsPtr, Session *session) : mComm(comm), mSession(session), mSessionId(0), mConnHandler(0), mLastKnownEvent(0) {
  int error;
  uint16_t masterPort;
  const char *masterHost;

  mVerbose = propsPtr->getPropertyBool("verbose", false);
  masterHost = propsPtr->getProperty("Hyperspace.Master.host", "localhost");
  masterPort = (uint16_t)propsPtr->getPropertyInt("Hyperspace.Master.port", Master::DEFAULT_MASTER_PORT);
  mLeaseInterval = (uint32_t)propsPtr->getPropertyInt("Hyperspace.Lease.Interval", Master::DEFAULT_LEASE_INTERVAL);
  mKeepAliveInterval = (uint32_t)propsPtr->getPropertyInt("Hyperspace.KeepAlive.Interval", Master::DEFAULT_KEEPALIVE_INTERVAL);
  
  if (!InetAddr::Initialize(&mMasterAddr, masterHost, masterPort))
    exit(1);

  if (mVerbose) {
    cout << "Hyperspace.KeepAlive.Interval=" << mKeepAliveInterval << endl;
    cout << "Hyperspace.Lease.Interval=" << mLeaseInterval << endl;
    cout << "Hyperspace.Master.host=" << masterHost << endl;
    cout << "Hyperspace.Master.port=" << masterPort << endl;
  }

  boost::xtime_get(&mLastKeepAliveSendTime, boost::TIME_UTC);
  boost::xtime_get(&mJeopardyTime, boost::TIME_UTC);
  mJeopardyTime.sec += mLeaseInterval;

  InetAddr::Initialize(&mLocalAddr, INADDR_ANY, 0);

  DispatchHandlerPtr dhp(this);
  if ((error = mComm->CreateDatagramReceiveSocket(&mLocalAddr, dhp)) != Error::OK) {
    std::string str;
    LOG_VA_ERROR("Unable to create datagram receive socket %s - %s", InetAddr::StringFormat(str, mLocalAddr), Error::GetText(error));
    exit(1);
  }

  CommBufPtr commBufPtr( Hyperspace::Protocol::CreateClientKeepaliveRequest(mSessionId, mLastKnownEvent) );

  if ((error = mComm->SendDatagram(mMasterAddr, mLocalAddr, commBufPtr) != Error::OK)) {
    LOG_VA_ERROR("Unable to send datagram - %s", Error::GetText(error));
    exit(1);
  }

  if ((error = mComm->SetTimer(mKeepAliveInterval*1000, this)) != Error::OK) {
    LOG_VA_ERROR("Problem setting timer - %s", Error::GetText(error));
    exit(1);
  }

  return;
}

/**
 *
 */
ClientKeepaliveHandler::~ClientKeepaliveHandler() {
  mComm->CloseSocket(mLocalAddr);
  delete mConnHandler;
}



/**
 *
 */
void ClientKeepaliveHandler::handle(hypertable::EventPtr &eventPtr) {
  boost::mutex::scoped_lock lock(mMutex);
  int error;
  uint16_t command = (uint16_t)-1;

  /**
  if (mVerbose) {
    LOG_VA_INFO("%s", eventPtr->toString().c_str());
  }
  **/

  if (eventPtr->type == hypertable::Event::MESSAGE) {
    uint8_t *msgPtr = eventPtr->message;
    size_t remaining = eventPtr->messageLen;

    try {

      if (!Serialization::DecodeShort(&msgPtr, &remaining, &command))
	throw ProtocolException("Truncated Request");

      // sanity check command code
      if (command >= Protocol::COMMAND_MAX)
	throw ProtocolException((std::string)"Invalid command (" + command + ")");

      switch (command) {
      case Protocol::COMMAND_KEEPALIVE:
	{
	  uint64_t sessionId;
	  int state;
	  uint32_t notificationCount;
	  uint64_t handle, eventId;
	  uint32_t eventMask;
	  const char *name;

	  if (mSession->GetState() == Session::STATE_EXPIRED)
	    return;

	  // update jeopardy time
	  memcpy(&mJeopardyTime, &mLastKeepAliveSendTime, sizeof(boost::xtime));
	  mJeopardyTime.sec += mLeaseInterval;

	  if (!Serialization::DecodeLong(&msgPtr, &remaining, &sessionId))
	    throw ProtocolException("Truncated Request");

	  if (!Serialization::DecodeInt(&msgPtr, &remaining, (uint32_t *)&error))
	    throw ProtocolException("Truncated Request");

	  if (error != Error::OK) {
	    if (error != Error::HYPERSPACE_EXPIRED_SESSION) {
	      LOG_VA_ERROR("Master session error - %s", Error::GetText(error));
	    }
	    ExpireSession();
	    return;
	  }

	  if (mSessionId == 0) {
	    mSessionId = sessionId;
	    if (mConnHandler == 0) {
	      mConnHandler = new ClientConnectionHandler(mComm, mSession, mLeaseInterval);
	      mConnHandler->SetVerboseMode(mVerbose);
	      mConnHandler->SetSessionId(mSessionId);
	    }
	  }

	  if (!Serialization::DecodeInt(&msgPtr, &remaining, &notificationCount)) {
	    throw ProtocolException("Truncated Request");
	  }

	  for (uint32_t i=0; i<notificationCount; i++) {

	    if (!Serialization::DecodeLong(&msgPtr, &remaining, &handle) ||
		!Serialization::DecodeLong(&msgPtr, &remaining, &eventId) ||
		!Serialization::DecodeInt(&msgPtr, &remaining, &eventMask))
	      throw ProtocolException("Truncated Request");

	    HandleMapT::iterator iter = mHandleMap.find(handle);
	    //LOG_VA_INFO("LastKnownEvent=%lld, eventId=%lld eventMask=%d", mLastKnownEvent, eventId, eventMask);
	    //LOG_VA_INFO("handle=%lldm, eventId=%lld, eventMask=%d", handle, eventId, eventMask);
	    assert (iter != mHandleMap.end());
	    ClientHandleStatePtr handleStatePtr = (*iter).second;

	    if (eventMask == EVENT_MASK_ATTR_SET || eventMask == EVENT_MASK_ATTR_DEL ||
		eventMask == EVENT_MASK_CHILD_NODE_ADDED || eventMask == EVENT_MASK_CHILD_NODE_REMOVED) {
	      if (!Serialization::DecodeString(&msgPtr, &remaining, &name))
		throw ProtocolException("Truncated Request");
	      if (eventId <= mLastKnownEvent)
		continue;
	      if (handleStatePtr->callbackPtr) {
		if (eventMask == EVENT_MASK_ATTR_SET)
		  handleStatePtr->callbackPtr->AttrSet(name);
		else if (eventMask == EVENT_MASK_ATTR_DEL)
		  handleStatePtr->callbackPtr->AttrDel(name);
		else if (eventMask == EVENT_MASK_CHILD_NODE_ADDED)
		  handleStatePtr->callbackPtr->ChildNodeAdded(name);
		else
		  handleStatePtr->callbackPtr->ChildNodeRemoved(name);
	      }
	    }
	    else if (eventMask == EVENT_MASK_LOCK_ACQUIRED) {
	      uint32_t mode;
	      if (!Serialization::DecodeInt(&msgPtr, &remaining, &mode))
		throw ProtocolException("Truncated Request");
	      if (eventId <= mLastKnownEvent)
		continue;
	      if (handleStatePtr->callbackPtr)
		handleStatePtr->callbackPtr->LockAcquired(mode);
	    }
	    else if (eventMask == EVENT_MASK_LOCK_RELEASED) {
	      if (eventId <= mLastKnownEvent)
		continue;
	      if (handleStatePtr->callbackPtr)
		handleStatePtr->callbackPtr->LockReleased();
	    }
	    else if (eventMask == EVENT_MASK_LOCK_GRANTED) {
	      uint32_t mode;
	      if (!Serialization::DecodeInt(&msgPtr, &remaining, &mode) ||
		  !Serialization::DecodeLong(&msgPtr, &remaining, &handleStatePtr->lockGeneration))
		throw ProtocolException("Truncated Request");
	      if (eventId <= mLastKnownEvent)
		continue;
	      handleStatePtr->lockStatus = LOCK_STATUS_GRANTED;
	      handleStatePtr->sequencer->generation = handleStatePtr->lockGeneration;
	      handleStatePtr->sequencer->mode = mode;
	      handleStatePtr->cond.notify_all();
	    }

	    mLastKnownEvent = eventId;
	  }
	  
	  /**
	  if (mVerbose) {
	    LOG_VA_INFO("sessionId = %lld", mSessionId);
	  }
	  **/

	  if (mConnHandler->Disconnected())
	    mConnHandler->InitiateConnection(mMasterAddr);
	  else
	    state = mSession->StateTransition(Session::STATE_SAFE);

	  if (notificationCount > 0) {
	    CommBufPtr commBufPtr( Hyperspace::Protocol::CreateClientKeepaliveRequest(mSessionId, mLastKnownEvent) );
	    boost::xtime_get(&mLastKeepAliveSendTime, boost::TIME_UTC);
	    if ((error = mComm->SendDatagram(mMasterAddr, mLocalAddr, commBufPtr) != Error::OK)) {
	      LOG_VA_ERROR("Unable to send datagram - %s", Error::GetText(error));
	      exit(1);
	    }
	  }

	  assert(mSessionId == sessionId);
	}
	break;
      default:
	throw ProtocolException((string)"Command code " + command + " not implemented");
      }
    }
    catch (ProtocolException &e) {
      std::string errMsg = e.what();
      LOG_VA_ERROR("Protocol error '%s'", e.what());
    }
  }
  else if (eventPtr->type == hypertable::Event::TIMER) {
    boost::xtime now;
    int state;
    
    // !!! fix - what about re-ordered packets?

    if ((state = mSession->GetState()) == Session::STATE_EXPIRED)
      return;

    boost::xtime_get(&now, boost::TIME_UTC);

    if (state == Session::STATE_SAFE) {
      if (xtime_cmp(mJeopardyTime, now) < 0) {
	mSession->StateTransition(Session::STATE_JEOPARDY);
      }
    }
    else if (mSession->Expired()) {
      ExpireSession();
      return;
    }

    CommBufPtr commBufPtr( Hyperspace::Protocol::CreateClientKeepaliveRequest(mSessionId, mLastKnownEvent) );

    boost::xtime_get(&mLastKeepAliveSendTime, boost::TIME_UTC);
    
    if ((error = mComm->SendDatagram(mMasterAddr, mLocalAddr, commBufPtr) != Error::OK)) {
      LOG_VA_ERROR("Unable to send datagram - %s", Error::GetText(error));
      exit(1);
    }

    if ((error = mComm->SetTimer(mKeepAliveInterval*1000, this)) != Error::OK) {
      LOG_VA_ERROR("Problem setting timer - %s", Error::GetText(error));
      exit(1);
    }
  }
  else {
    LOG_VA_INFO("%s", eventPtr->toString().c_str());
  }
}



void ClientKeepaliveHandler::ExpireSession() {
  mSession->StateTransition(Session::STATE_EXPIRED);
  if (mConnHandler)
    mConnHandler->Close();
  poll(0,0,2000);
  delete mConnHandler;
  mConnHandler = 0;
  return;
}
