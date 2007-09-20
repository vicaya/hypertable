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
#include "Common/Exception.h"
#include "Common/InetAddr.h"

#include "ClientSessionHandler.h"
#include "Master.h"
#include "Protocol.h"
#include "Session.h"

using namespace Hyperspace;
using namespace hypertable;


/**
 * 
 */    
ClientSessionHandler::ClientSessionHandler(Comm *comm, PropertiesPtr &propsPtr, Hyperspace::Session *session) : mComm(comm), mSession(session), mConnected(false), mState(STATE_CANCELLED), mSessionId(0) {
  int error;
  uint16_t sendPort, masterPort;

  mVerbose = propsPtr->getPropertyBool("verbose", false);

  mLeaseInterval = (uint32_t)propsPtr->getPropertyInt("Hyperspace.Lease.Interval", Master::DEFAULT_LEASE_INTERVAL);

  mKeepAliveInterval = (uint32_t)propsPtr->getPropertyInt("Hyperspace.KeepAlive.Interval", Master::DEFAULT_KEEPALIVE_INTERVAL);

  sendPort = (uint16_t)propsPtr->getPropertyInt("Hyperspace.Client.port", Session::DEFAULT_CLIENT_PORT);
  InetAddr::Initialize(&mDatagramSendAddr, INADDR_ANY, sendPort);

  const char *host = propsPtr->getProperty("Hyperspace.Master.host", "localhost");
  masterPort = (uint16_t)propsPtr->getPropertyInt("Hyperspace.Master.port", Master::DEFAULT_MASTER_PORT);
  if (!InetAddr::Initialize(&mMasterAddr, host, masterPort))
    exit(1);

  if (mVerbose) {
    cout << "Hyperspace.Client.port=" << sendPort << endl;
    cout << "Hyperspace.KeepAlive.Interval=" << mKeepAliveInterval << endl;
    cout << "Hyperspace.Lease.Interval=" << mLeaseInterval << endl;
    cout << "Hyperspace.Master.host=" << host << endl;
    cout << "Hyperspace.Master.port=" << masterPort << endl;
  }

  boost::xtime_get(&mLastKeepAliveSendTime, boost::TIME_UTC);

  // !!! fix me (should pass addr in) !!!
  if ((error = mComm->CreateDatagramReceiveSocket(mDatagramSendAddr, this)) != Error::OK) {
    std::string str;
    LOG_VA_ERROR("Unable to create datagram receive socket %s - %s", InetAddr::StringFormat(str, mDatagramSendAddr), Error::GetText(error));
    exit(1);
  }

  CommBufPtr commBufPtr( Hyperspace::Protocol::CreateKeepAliveRequest(0) );

  if ((error = mComm->SendDatagram(mMasterAddr, mDatagramSendAddr, commBufPtr) != Error::OK)) {
    LOG_VA_ERROR("Unable to send datagram - %s", Error::GetText(error));
    exit(1);
  }

  if ((error = mComm->SetTimer(mKeepAliveInterval*1000, this)) != Error::OK) {
    LOG_VA_ERROR("Problem setting timer - %s", Error::GetText(error));
    exit(1);
  }

  return;
}


void ClientSessionHandler::handle(EventPtr &eventPtr) {
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
	  uint32_t sessionId;

	  if (!Serialization::DecodeInt(&msgPtr, &remaining, &sessionId)) {
	    std::string message = "Truncated Request";
	    cerr << "Message len = " << eventPtr->messageLen << endl;
	    throw ProtocolException(message);
	  }

	  if (mState == STATE_CANCELLED) {
	    mState = STATE_CONNECTING;
	    if ((error = mComm->Connect(mMasterAddr, mLeaseInterval, this)) != Error::OK) {
	      std::string str;
	      LOG_VA_ERROR("Problem establishing TCP connection with Hyperspace.Master at %s", InetAddr::StringFormat(str, mMasterAddr));
	      exit(1);
	    }
	  }

	  if (mSessionId == 0)
	    mSessionId = sessionId;
	  
	  assert(mSessionId == sessionId);

	}
	break;
      case Protocol::COMMAND_HANDSHAKE:
	mState = STATE_READY;
	mSession->SetState(Session::STATE_SAFE);
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
  else if (eventPtr->type == Event::DISCONNECT) {
    LOG_VA_WARN("%s", eventPtr->toString().c_str());
    mConnected = false;
    mState = STATE_CANCELLED;
    mSession->SetState(Session::STATE_JEOPARDY);
  }
  else if (eventPtr->type == Event::CONNECTION_ESTABLISHED) {

    mConnected = true;
    mState = STATE_HANDSHAKING;

    CommBufPtr commBufPtr( Hyperspace::Protocol::CreateHandshakeRequest(mSessionId) );

    if ((error = mComm->SendRequest(eventPtr->addr, commBufPtr, this)) != Error::OK) {
      LOG_VA_ERROR("Unable to send handshake request - %s", Error::GetText(error));
      // we should probably just change states here ...
      exit(1);
    }

  }
  else if (eventPtr->type == Event::TIMER) {

    CommBufPtr commBufPtr( Hyperspace::Protocol::CreateKeepAliveRequest(mSessionId) );
    
    if ((error = mComm->SendDatagram(mMasterAddr, mDatagramSendAddr, commBufPtr) != Error::OK)) {
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
