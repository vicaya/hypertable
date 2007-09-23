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

#include "ClientConnectionHandler.h"
#include "Master.h"
#include "Protocol.h"
#include "Session.h"

using namespace Hyperspace;
using namespace hypertable;


/**
 * 
 */    
ClientConnectionHandler::ClientConnectionHandler(Comm *comm, Session *session) : mComm(comm), mSession(session), mState(DISCONNECTED), mSessionId(0), mVerbose(false) {
  return;
}


void ClientConnectionHandler::handle(EventPtr &eventPtr) {
  boost::mutex::scoped_lock lock(mMutex);
  int error;

  if (mVerbose) {
    LOG_VA_INFO("%s", eventPtr->toString().c_str());
  }

  if (eventPtr->type == Event::MESSAGE) {

    if (Protocol::ResponseCode(eventPtr.get()) != Error::OK) {
      LOG_VA_ERROR("Connection handshake error: %s", Protocol::StringFormatMessage(eventPtr.get()).c_str());
      mComm->CloseSocket(eventPtr->addr);
      mState = DISCONNECTED;
      return;
    }

    mSession->StateTransition(Session::STATE_SAFE);

    mState = CONNECTED;
  }
  else if (eventPtr->type == Event::DISCONNECT) {

    if (mVerbose) {
      LOG_VA_WARN("%s", eventPtr->toString().c_str());
    }

    mSession->StateTransition(Session::STATE_JEOPARDY);

    mState = DISCONNECTED;
  }
  else if (eventPtr->type == Event::CONNECTION_ESTABLISHED) {

    mState = HANDSHAKING;

    CommBufPtr commBufPtr( Hyperspace::Protocol::CreateHandshakeRequest(mSessionId) );

    if ((error = mComm->SendRequest(eventPtr->addr, commBufPtr, this)) != Error::OK) {
      LOG_VA_ERROR("Problem sending handshake request - %s", Error::GetText(error));
      mComm->CloseSocket(eventPtr->addr);
      mState = DISCONNECTED;
    }
  }
  else {
    LOG_VA_INFO("%s", eventPtr->toString().c_str());
  }

}
