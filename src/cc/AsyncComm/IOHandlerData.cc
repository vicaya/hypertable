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

#include "IOHandlerData.h"
#include "HandlerMap.h"
using namespace hypertable;

atomic_t IOHandlerData::msNextConnectionId = ATOMIC_INIT(1);

#if defined(__linux__)

bool IOHandlerData::HandleEvent(struct epoll_event *event) {

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
bool IOHandlerData::HandleEvent(struct kevent *event) {

  //DisplayEvent(event);

  if (mShutdown) {
    DeliverEvent( new Event(Event::DISCONNECT, mId, mAddr, Error::OK) );
    return true;
  }

  assert(mSd == (int)event->ident);

  if (event->flags & EV_EOF) {
    if (!mConnected)
      DeliverEvent( new Event(Event::DISCONNECT, mId, mAddr, Error::COMM_CONNECT_ERROR) );
    else
      DeliverEvent( new Event(Event::DISCONNECT, mId, mAddr, Error::OK) );
    return true;
  }

  if (event->filter == EVFILT_WRITE) {
    if (HandleWriteReadiness()) {
      DeliverEvent( new Event(Event::DISCONNECT, mId, mAddr, Error::OK) );
      return true;
    }
  }

  if (event->filter == EVFILT_READ) {
    size_t available = (size_t)event->data;
    size_t nread;
    while (available > 0) {
      if (!mGotHeader) {
	uint8_t *ptr = ((uint8_t *)&mMessageHeader) + (sizeof(Header::HeaderT) - mMessageHeaderRemaining);
	if (mMessageHeaderRemaining < available) {
	  nread = FileUtils::Read(mSd, ptr, mMessageHeaderRemaining);
	  if (nread < 0) {
	    LOG_VA_ERROR("FileUtils::Read(%d, len=%d) failure : %s", mSd, mMessageHeaderRemaining, strerror(errno));
	    DeliverEvent( new Event(Event::DISCONNECT, mId, mAddr, Error::OK) );
	    return true;
	  }
	  assert(nread == mMessageHeaderRemaining);
	  mGotHeader = true;
	  available -= nread;
	  mMessageHeaderRemaining = 0;
	  mMessage = new uint8_t [ mMessageHeader.totalLen ];
	  memcpy(mMessage, &mMessageHeader, sizeof(Header::HeaderT));
	  mMessagePtr = mMessage + sizeof(Header::HeaderT);
	  mMessageRemaining = (mMessageHeader.totalLen) - sizeof(Header::HeaderT);
	}
	else {
	  nread = FileUtils::Read(mSd, ptr, available);
	  if (nread < 0) {
	    LOG_VA_ERROR("FileUtils::Read(%d, len=%d) failure : %s", mSd, available, strerror(errno));
	    DeliverEvent( new Event(Event::DISCONNECT, mId, mAddr, Error::OK) );
	    return true;
	  }
	  assert(nread == available);
	  mMessageHeaderRemaining -= nread;
	  return false;
	}
      }
      if (mGotHeader) {
	if (mMessageRemaining <= available) {
	  nread = FileUtils::Read(mSd, mMessagePtr, mMessageRemaining);
	  if (nread < 0) {
	    LOG_VA_ERROR("FileUtils::Read(%d, len=%d) failure : %s", mSd, mMessageRemaining, strerror(errno));
	    DeliverEvent( new Event(Event::DISCONNECT, mId, mAddr, Error::OK) );
	    return true;
	  }
	  assert(nread == mMessageRemaining);
	  available -= nread;

	  DispatchHandler *dh = 0;
	  uint32_t id = ((Header::HeaderT *)mMessage)->id;
	  if ((((Header::HeaderT *)mMessage)->flags & Header::FLAGS_MASK_REQUEST) == 0 &&
	      (dh = mReactor->RemoveRequest(id)) == 0) {
	    LOG_VA_WARN("Received response for non-pending event (id=%d)", id);
	  }
	  else
	    DeliverEvent( new Event(Event::MESSAGE, mId, mAddr, Error::OK, (Header::HeaderT *)mMessage), dh );
	  ResetIncomingMessageState();
	}
	else {
	  nread = FileUtils::Read(mSd, mMessagePtr, available);
	  if (nread < 0) {
	    LOG_VA_ERROR("FileUtils::Read(%d, len=%d) failure : %s", mSd, available, strerror(errno));
	    DeliverEvent( new Event(Event::DISCONNECT, mId, mAddr, Error::OK) );
	    return true;
	  }
	  assert(nread == available);
	  mMessagePtr += nread;
	  mMessageRemaining -= nread;
	  available = 0;
	}
      }
    }
  }

  return false;
}
#else
  ImplementMe;
#endif


bool IOHandlerData::HandleWriteReadiness() {
  boost::mutex::scoped_lock lock(mMutex);

  if (mConnected == false) {
    int bufsize = 4*32768;
    if (setsockopt(mSd, SOL_SOCKET, SO_SNDBUF, (char *)&bufsize, sizeof(bufsize)) < 0) {
      LOG_VA_ERROR("setsockopt(SO_SNDBUF) failed - %s", strerror(errno));
    }
    if (setsockopt(mSd, SOL_SOCKET, SO_RCVBUF, (char *)&bufsize, sizeof(bufsize)) < 0) {
      LOG_VA_ERROR("setsockopt(SO_RCVBUF) failed - %s", strerror(errno));
    }
    //clog << "Connection established." << endl;
    mConnected = true;
    DeliverEvent( new Event(Event::CONNECTION_ESTABLISHED, mId, mAddr, Error::OK) );
  }
  else {
    if (FlushSendQueue() != Error::OK)
      return true;
  }
  if (mSendQueue.empty())
    RemovePollInterest(Reactor::WRITE_READY);	
  return false;
}



int IOHandlerData::SendMessage(CommBufPtr &cbufPtr, DispatchHandler *dispatchHandler) {
  boost::mutex::scoped_lock lock(mMutex);
  int error;
  struct timeval tv;
  bool initiallyEmpty = mSendQueue.empty() ? true : false;
  Header::HeaderT *mheader = (Header::HeaderT *)cbufPtr->data;

  LOG_ENTER;

  if (gettimeofday(&tv, 0) < 0) {
    LOG_VA_ERROR("gettimeofday() failed : %s", strerror(errno));
    exit(1);
  }

  // If request, Add message ID to request cache
  if (dispatchHandler != 0 && mheader->flags & Header::FLAGS_MASK_REQUEST)
    mReactor->AddRequest(mheader->id, this, dispatchHandler, tv.tv_sec + mTimeout);

  mSendQueue.push_back(cbufPtr);

  if ((error = FlushSendQueue()) != Error::OK)
    return error;

  if (initiallyEmpty && !mSendQueue.empty()) {
    AddPollInterest(Reactor::WRITE_READY);
    LOG_INFO("Adding Write interest");
  }
  else if (!initiallyEmpty && mSendQueue.empty()) {
    RemovePollInterest(Reactor::WRITE_READY);
    LOG_INFO("Removing Write interest");
  }

  return Error::OK;
}



int IOHandlerData::FlushSendQueue() {
  ssize_t nwritten, towrite;
  struct iovec vec[2];
  int count;

  while (!mSendQueue.empty()) {

    CommBufPtr &cbufPtr = mSendQueue.front();

    count = 0;
    towrite = 0;
    if (cbufPtr->dataLen > 0) {
      vec[0].iov_base = cbufPtr->data;
      vec[0].iov_len = cbufPtr->dataLen;
      towrite = cbufPtr->dataLen;
      ++count;
    }
    if (cbufPtr->ext != 0) {
      ssize_t remaining = cbufPtr->extLen - (cbufPtr->extPtr - cbufPtr->ext);
      if (remaining > 0) {
	vec[count].iov_base = cbufPtr->extPtr;
	vec[count].iov_len = remaining;
	towrite += remaining;
	++count;
      }
    }

    nwritten = FileUtils::Writev(mSd, vec, count);
    if (nwritten == (ssize_t)-1) {
      LOG_VA_WARN("FileUtils::Writev(%d, len=%d) failed : %s", mSd, towrite, strerror(errno));
      return Error::COMM_BROKEN_CONNECTION;
    }
    else if (nwritten < towrite) {
      if (nwritten == 0)
	break;
      if (cbufPtr->dataLen > 0) {
	if (nwritten < (ssize_t)cbufPtr->dataLen) {
	  cbufPtr->dataLen -= nwritten;
	  cbufPtr->data += nwritten;
	  break;
	}
	else {
	  nwritten -= cbufPtr->dataLen;
	  cbufPtr->dataLen = 0;
	}
      }
      if (cbufPtr->ext != 0) {
	cbufPtr->extPtr += nwritten;
	break;
      }
    }

#if 0
    // write header
    if (cbufPtr->dataLen > 0) {
      nwritten = writen(mSd, cbufPtr->data, cbufPtr->dataLen);
      if (nwritten == (ssize_t)-1) {
	LOG_VA_WARN("write(%d, len=%d) failed : %s", mSd, cbufPtr->dataLen, strerror(errno));
	return Error::COMM_BROKEN_CONNECTION;
      }
      cbufPtr->dataLen -= nwritten;
      if (nwritten < cbufPtr->dataLen) {
	cbufPtr->data += nwritten;
	LOG_VA_WARN("Only flushed %d still have %d", nwritten, cbufPtr->dataLen);
	break;
      }
    }

    // write data
    if (cbufPtr->ext != 0) {
      ssize_t remaining = cbufPtr->extLen - (cbufPtr->extPtr - cbufPtr->ext);
      if (remaining > 0) {
	nwritten = writen(mSd, cbufPtr->extPtr, remaining);
	if (nwritten == (ssize_t)-1) {
	  LOG_VA_WARN("write(%d, len=%d) failed : %s", mSd, remaining, strerror(errno));
	  return Error::COMM_BROKEN_CONNECTION;
	}
	else if (nwritten < remaining) {
	  cbufPtr->extPtr += nwritten;
	  LOG_VA_WARN("Only flushed %d of %d", nwritten, remaining);
	  break;
	}
      }
    }
#endif

    // buffer written successfully, now remove from queue (which will destroy buffer)
    mSendQueue.pop_front();
  }

  return Error::OK;
}
