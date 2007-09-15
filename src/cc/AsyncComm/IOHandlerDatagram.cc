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
#include <iostream>
using namespace std;

extern "C" {
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#if defined(__APPLE__)
#include <sys/event.h>
#endif
}

#define DISABLE_LOG_DEBUG 1

#include "Common/Error.h"
#include "Common/FileUtils.h"

#include "IOHandlerDatagram.h"
#include "HandlerMap.h"
using namespace hypertable;

#if defined(__linux__)

bool IOHandlerDatagram::HandleEvent(struct epoll_event *event) {

  //DisplayEvent(event);

  if (mShutdown) {
    DeliverEvent( new Event(Event::DISCONNECT, mId, mAddr, Error::OK) );
    return true;
  }

  if (event->events & EPOLLOUT) {
    if (HandleWriteReadiness()) {
      DeliverEvent( new Event(Event::DISCONNECT, mId, mAddr, Error::OK) );
      return true;
    }
  }

  if (event->events & EPOLLIN) {
    size_t nread, totalRead = 0;
    while (true) {
      if (!mGotHeader) {
	uint8_t *ptr = ((uint8_t *)&mMessageHeader) + (sizeof(Header::HeaderT) - mMessageHeaderRemaining);
	nread = FileUtils::Read(mSd, ptr, mMessageHeaderRemaining);
	if (nread == (size_t)-1) {
	  if (errno != ECONNREFUSED) {
	    LOG_VA_ERROR("FileUtils::Read(%d, len=%d) failure : %s", mSd, mMessageHeaderRemaining, strerror(errno));
	  }
	  int error = (errno == ECONNREFUSED) ? Error::COMM_CONNECT_ERROR : Error::OK;
	  DeliverEvent( new Event(Event::DISCONNECT, mId, mAddr, error ) );
	  return true;
	}
	else if (nread == 0 && totalRead == 0) {
	  // eof
	  DeliverEvent( new Event(Event::DISCONNECT, mId, mAddr, Error::OK) );
	  return true;
	}
	else if (nread < mMessageHeaderRemaining) {
	  mMessageHeaderRemaining -= nread;
	  return false;
	}
	else {
	  mGotHeader = true;
	  mMessageHeaderRemaining = 0;
	  mMessage = new uint8_t [ mMessageHeader.totalLen ];
	  memcpy(mMessage, &mMessageHeader, sizeof(Header::HeaderT));
	  mMessagePtr = mMessage + sizeof(Header::HeaderT);
	  mMessageRemaining = (mMessageHeader.totalLen) - sizeof(Header::HeaderT);
	  totalRead += nread;
	}
      }
      if (mGotHeader) {
	nread = FileUtils::Read(mSd, mMessagePtr, mMessageRemaining);
	if (nread < 0) {
	  LOG_VA_ERROR("FileUtils::Read(%d, len=%d) failure : %s", mSd, mMessageHeaderRemaining, strerror(errno));
	  DeliverEvent( new Event(Event::DISCONNECT, mId, mAddr, Error::OK) );
	  return true;
	}
	else if (nread == 0 && totalRead == 0) {
	  // eof
	  DeliverEvent( new Event(Event::DISCONNECT, mId, mAddr, Error::OK) );
	  return true;
	}
	else if (nread < mMessageRemaining) {
	  mMessagePtr += nread;
	  mMessageRemaining -= nread;
	}
	else {
	  DispatchHandler *dh = 0;
	  uint32_t id = ((Header::HeaderT *)mMessage)->id;
	  if ((((Header::HeaderT *)mMessage)->flags & Header::FLAGS_MASK_REQUEST) == 0 &&
	      (dh = mReactor->RemoveRequest(id)) == 0) {
	    LOG_VA_WARN("Received response for non-pending event (id=%d,version=%d,totalLen=%d)",
			id, ((Header::HeaderT *)mMessage)->version, ((Header::HeaderT *)mMessage)->totalLen);
	  }
	  else
	    DeliverEvent( new Event(Event::MESSAGE, mId, mAddr, Error::OK, (Header::HeaderT *)mMessage), dh );
	  ResetIncomingMessageState();
	}
	totalRead += nread;
      }
    }
  }

  if (event->events & EPOLLERR) {
    LOG_VA_WARN("Received EPOLLERR on descriptor %d (%s:%d)", mSd, inet_ntoa(mAddr.sin_addr), ntohs(mAddr.sin_port));
    DeliverEvent( new Event(Event::DISCONNECT, mId, mAddr, Error::OK) );
    return true;
  }

  if (event->events & EPOLLHUP) {
    LOG_VA_WARN("Received EPOLLHUP on descriptor %d (%s:%d)", mSd, inet_ntoa(mAddr.sin_addr), ntohs(mAddr.sin_port));
    DeliverEvent( new Event(Event::DISCONNECT, mId, mAddr, Error::OK) );
    return true;
  }

  return false;
}

#elif defined(__APPLE__)

/**
 *
 */
bool IOHandlerDatagram::HandleEvent(struct kevent *event) {

  //DisplayEvent(event);

  if (mShutdown) {
    //DeliverEvent( new Event(Event::DISCONNECT, mId, mAddr, Error::OK) );
    return true;
  }

  assert(mSd == (int)event->ident);

  assert((event->flags & EV_EOF) == 0);

  if (event->filter == EVFILT_WRITE) {
    if (HandleWriteReadiness()) {
      //DeliverEvent( new Event(Event::DISCONNECT, mId, mAddr, Error::OK) );
      return true;
    }
  }

  if (event->filter == EVFILT_READ) {
    size_t available = (size_t)event->data;
    ssize_t nread;
    uint8_t *rmsg;
    struct sockaddr_in addr;
    socklen_t fromlen = sizeof(struct sockaddr_in);

    if ((nread = FileUtils::Recvfrom(mSd, mMessage, 65536, (struct sockaddr *)&addr, &fromlen)) == (ssize_t)-1) {
      LOG_VA_ERROR("FileUtils::Recvfrom(%d, len=%d) failure : %s", mSd, available, strerror(errno));
      DeliverEvent( new Event(Event::ERROR, mSd, addr, Error::COMM_RECEIVE_ERROR) );
      return true;
    }

    //LOG_VA_INFO("Received message from  %s:%d", inet_ntoa(addr.sin_addr), ntohs(addr.sin_port));

    rmsg = new uint8_t [ nread ];
    memcpy(rmsg, mMessage, nread);

    DeliverEvent( new Event(Event::MESSAGE, 0, addr, Error::OK, (Header::HeaderT *)rmsg) );

    return false;
  }

}
#else
ImplementMe;
#endif


bool IOHandlerDatagram::HandleWriteReadiness() {
  boost::mutex::scoped_lock lock(mMutex);

  if (FlushSendQueue() != Error::OK)
    return true;

  // is this necessary?
  if (mSendQueue.empty())
    RemovePollInterest(Reactor::WRITE_READY);

  return false;
}



int IOHandlerDatagram::SendMessage(struct sockaddr_in &addr, CommBufPtr &cbufPtr) {
  boost::mutex::scoped_lock lock(mMutex);
  int error;
  bool initiallyEmpty = mSendQueue.empty() ? true : false;

  LOG_ENTER;

  //LOG_VA_INFO("Pushing message destined for %s:%d onto send queue", 
  //inet_ntoa(addr.sin_addr), ntohs(addr.sin_port));

  mSendQueue.push_back(SendRecT(addr, cbufPtr));

  if ((error = FlushSendQueue()) != Error::OK)
    return error;

  if (initiallyEmpty && !mSendQueue.empty()) {
    AddPollInterest(Reactor::WRITE_READY);
    //LOG_INFO("Adding Write interest");
  }
  else if (!initiallyEmpty && mSendQueue.empty()) {
    RemovePollInterest(Reactor::WRITE_READY);
    //LOG_INFO("Removing Write interest");
  }

  return Error::OK;
}



int IOHandlerDatagram::FlushSendQueue() {
  ssize_t nsent;

  while (!mSendQueue.empty()) {

    SendRecT &sendRec = mSendQueue.front();

    assert(sendRec.second->dataLen > 0);
    assert(sendRec.second->ext == 0);

    nsent = FileUtils::Sendto(mSd, sendRec.second->data, sendRec.second->dataLen,
			      (const sockaddr*)&sendRec.first, sizeof(struct sockaddr_in));

    if (nsent == (ssize_t)-1) {
      LOG_VA_WARN("FileUtils::Sendto(%d, len=%d, addr=%s:%d) failed : %s", mSd, sendRec.second->dataLen,
		  inet_ntoa(sendRec.first.sin_addr), ntohs(sendRec.first.sin_port), strerror(errno));
      return Error::COMM_SEND_ERROR;
    }
    else if (nsent < sendRec.second->dataLen) {
      LOG_VA_WARN("Only sent %d bytes", nsent);
      if (nsent == 0)
	break;
      sendRec.second->dataLen -= nsent;
      sendRec.second->data += nsent;
      break;
    }

    //LOG_VA_INFO("Successfully sent message to %s:%d", inet_ntoa(sendRec.first.sin_addr), ntohs(sendRec.first.sin_port));

    // buffer written successfully, now remove from queue (which will destroy buffer)
    mSendQueue.pop_front();
  }

  return Error::OK;
}
