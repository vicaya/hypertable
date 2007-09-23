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

#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/Exception.h"
#include "Common/StringExt.h"

#include "ClientKeepaliveHandler.h"
#include "ClientSessionState.h"
#include "Master.h"
#include "Protocol.h"
#include "Session.h"

using namespace hypertable;
using namespace Hyperspace;

ClientKeepaliveHandler::ClientKeepaliveHandler(Comm *comm, PropertiesPtr &propsPtr, Hyperspace::SessionCallback *sessionCallback, ClientSessionStatePtr &sessionStatePtr) : mComm(comm), mSessionCallback(sessionCallback), mSessionStatePtr(sessionStatePtr), mSessionId(0), mConnHandler(0), mLastKnownEvent(0) {
  int error;
  uint16_t sendPort, masterPort;
  const char *masterHost;

  mVerbose = propsPtr->getPropertyBool("verbose", false);
  masterHost = propsPtr->getProperty("Hyperspace.Master.host", "localhost");
  masterPort = (uint16_t)propsPtr->getPropertyInt("Hyperspace.Master.port", Master::DEFAULT_MASTER_PORT);
  mLeaseInterval = (uint32_t)propsPtr->getPropertyInt("Hyperspace.Lease.Interval", Master::DEFAULT_LEASE_INTERVAL);
  mKeepAliveInterval = (uint32_t)propsPtr->getPropertyInt("Hyperspace.KeepAlive.Interval", Master::DEFAULT_KEEPALIVE_INTERVAL);
  mGracePeriod = (uint32_t)propsPtr->getPropertyInt("Hyperspace.GracePeriod", Master::DEFAULT_GRACEPERIOD);
  sendPort = (uint16_t)propsPtr->getPropertyInt("Hyperspace.Client.port", Session::DEFAULT_CLIENT_PORT);
  
  if (!InetAddr::Initialize(&mMasterAddr, masterHost, masterPort))
    exit(1);

  if (mVerbose) {
    cout << "Hyperspace.Client.port=" << sendPort << endl;
    cout << "Hyperspace.GracePeriod=" << mGracePeriod << endl;
    cout << "Hyperspace.KeepAlive.Interval=" << mKeepAliveInterval << endl;
    cout << "Hyperspace.Lease.Interval=" << mLeaseInterval << endl;
    cout << "Hyperspace.Master.host=" << masterHost << endl;
    cout << "Hyperspace.Master.port=" << masterPort << endl;
  }

  boost::xtime_get(&mLastKeepAliveSendTime, boost::TIME_UTC);
  boost::xtime_get(&mJeopardyTime, boost::TIME_UTC);
  mJeopardyTime.sec += mGracePeriod;

  InetAddr::Initialize(&mLocalAddr, INADDR_ANY, sendPort);

  if ((error = mComm->CreateDatagramReceiveSocket(mLocalAddr, this)) != Error::OK) {
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
void ClientKeepaliveHandler::handle(EventPtr &eventPtr) {
  boost::mutex::scoped_lock lock(mMutex);
  int error;
  uint16_t command = (uint16_t)-1;

  if (mVerbose) {
    LOG_VA_INFO("%s", eventPtr->toString().c_str());
  }

  if (eventPtr->type == Event::MESSAGE) {
    uint8_t *msgPtr = eventPtr->message;
    size_t remaining = eventPtr->messageLen;

    try {

      if (!Serialization::DecodeShort(&msgPtr, &remaining, &command)) {
	std::string message = "Truncated Request";
	throw ProtocolException(message);
      }

      // sanity check command code
      if (command >= Protocol::COMMAND_MAX) {
	std::string message = (std::string)"Invalid command (" + command + ")";
	throw ProtocolException(message);
      }

      switch (command) {
      case Protocol::COMMAND_KEEPALIVE:
	{
	  uint64_t sessionId;
	  int state;

	  if (mSessionStatePtr->Get() == ClientSessionState::EXPIRED)
	    return;

	  state = mSessionStatePtr->Transition(ClientSessionState::SAFE);
	  if (state == ClientSessionState::JEOPARDY)
	    mSessionCallback->Safe();

	  // update jeopardy time
	  memcpy(&mJeopardyTime, &mLastKeepAliveSendTime, sizeof(boost::xtime));
	  mJeopardyTime.sec += mLeaseInterval;

	  if (!Serialization::DecodeLong(&msgPtr, &remaining, &sessionId)) {
	    std::string message = "Truncated Request";
	    cerr << "Message len = " << eventPtr->messageLen << endl;
	    throw ProtocolException(message);
	  }

	  if (mSessionId == 0) {
	    mSessionId = sessionId;
	    if (mConnHandler == 0) {
	      mConnHandler = new ClientConnectionHandler(mComm, mSessionCallback, mSessionStatePtr);
	      mConnHandler->SetVerboseMode(mVerbose);
	      mConnHandler->SetSessionId(mSessionId);
	    }
	  }

	  if (mVerbose) {
	    LOG_VA_INFO("sessionId = %lld", mSessionId);
	  }

	  if (mConnHandler->Disconnected())
	    mConnHandler->InitiateConnection(mMasterAddr, mLeaseInterval);

	  assert(mSessionId == sessionId);
	  
	}
	break;
      default:
	std::string message = (string)"Command code " + command + " not implemented";
	throw ProtocolException(message);
      }
    }
    catch (ProtocolException &e) {
      std::string errMsg = e.what();
      LOG_VA_ERROR("Protocol error '%s'", e.what());
    }
  }
  else if (eventPtr->type == Event::TIMER) {
    boost::xtime now;
    int state;
    
    // !!! fix - what about re-ordered packets?

    state = mSessionStatePtr->Get();

    if (state == ClientSessionState::EXPIRED)
      return;

    boost::xtime_get(&now, boost::TIME_UTC);

    if (state == ClientSessionState::SAFE) {
      if (xtime_cmp(mJeopardyTime, now) < 0) {
	mSessionStatePtr->Transition(ClientSessionState::JEOPARDY);
	mSessionCallback->Jeopardy();
	memcpy(&mExpireTime, &now, sizeof(boost::xtime));	  
	mExpireTime.sec += mGracePeriod;
      }
    }
    else {
      if (xtime_cmp(mExpireTime, now) < 0) {
	mSessionStatePtr->Transition(ClientSessionState::EXPIRED);
	mSessionCallback->Expired();
	return;
      }
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

