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
#include "ConnectionMap.h"
using namespace hypertable;


#if defined(__linux__)

bool IOHandlerData::HandleEvent(struct epoll_event *event) {

  //DisplayEvent(event);

  if (mShutdown) {
    DeliverEvent( new Event(Event::DISCONNECT, mAddr, Error::OK) );
    mConnMap.Remove(mAddr);
    return true;
  }

  if (event->events & EPOLLOUT) {
    if (HandleWriteReadiness()) {
      DeliverEvent( new Event(Event::DISCONNECT, mAddr, Error::OK) );
      mConnMap.Remove(mAddr);
      return true;
    }
  }

  if (event->events & EPOLLIN) {
    size_t nread, totalRead = 0;
    while (true) {
      if (!mGotHeader) {
	uint8_t *ptr = ((uint8_t *)&mMessageHeader) + (sizeof(Message::HeaderT) - mMessageHeaderRemaining);
	nread = FileUtils::Read(mSd, ptr, mMessageHeaderRemaining);
	if (nread == (size_t)-1) {
	  if (errno != ECONNREFUSED) {
	    LOG_VA_ERROR("FileUtils::Read(%d, len=%d) failure : %s", mSd, mMessageHeaderRemaining, strerror(errno));
	  }
	  int error = (errno == ECONNREFUSED) ? Error::COMM_CONNECT_ERROR : Error::OK;
	  DeliverEvent( new Event(Event::DISCONNECT, mAddr, error ) );
	  mConnMap.Remove(mAddr);
	  return true;
	}
	else if (nread == 0 && totalRead == 0) {
	  // eof
	  DeliverEvent( new Event(Event::DISCONNECT, mAddr, Error::OK) );
	  mConnMap.Remove(mAddr);
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
	  memcpy(mMessage, &mMessageHeader, sizeof(Message::HeaderT));
	  mMessagePtr = mMessage + sizeof(Message::HeaderT);
	  mMessageRemaining = (mMessageHeader.totalLen) - sizeof(Message::HeaderT);
	  totalRead += nread;
	}
      }
      if (mGotHeader) {
	nread = FileUtils::Read(mSd, mMessagePtr, mMessageRemaining);
	if (nread < 0) {
	  LOG_VA_ERROR("FileUtils::Read(%d, len=%d) failure : %s", mSd, mMessageHeaderRemaining, strerror(errno));
	  DeliverEvent( new Event(Event::DISCONNECT, mAddr, Error::OK) );
	  mConnMap.Remove(mAddr);
	  return true;
	}
	else if (nread == 0 && totalRead == 0) {
	  // eof
	  DeliverEvent( new Event(Event::DISCONNECT, mAddr, Error::OK) );
	  mConnMap.Remove(mAddr);
	  return true;
	}
	else if (nread < mMessageRemaining) {
	  mMessagePtr += nread;
	  mMessageRemaining -= nread;
	}
	else {
	  CallbackHandler *cb = 0;
	  uint32_t id = ((Message::HeaderT *)mMessage)->id;
	  if ((((Message::HeaderT *)mMessage)->flags & Message::FLAGS_MASK_REQUEST) == 0 &&
	      (cb = mReactor->RemoveRequest(id)) == 0) {
	    LOG_VA_WARN("Received response for non-pending event (id=%d,version=%d,totalLen=%d)",
			id, ((Message::HeaderT *)mMessage)->version, ((Message::HeaderT *)mMessage)->totalLen);
	  }
	  else
	    DeliverEvent( new Event(Event::MESSAGE, mAddr, Error::OK, (Message::HeaderT *)mMessage), cb );
	  ResetIncomingMessageState();
	}
	totalRead += nread;
      }
    }
  }

  if (event->events & EPOLLERR) {
    LOG_VA_WARN("Received EPOLLERR on descriptor %d (%s:%d)", mSd, inet_ntoa(mAddr.sin_addr), ntohs(mAddr.sin_port));
    DeliverEvent( new Event(Event::DISCONNECT, mAddr, Error::OK) );
    mConnMap.Remove(mAddr);
    return true;
  }

  if (event->events & EPOLLHUP) {
    LOG_VA_WARN("Received EPOLLHUP on descriptor %d (%s:%d)", mSd, inet_ntoa(mAddr.sin_addr), ntohs(mAddr.sin_port));
    DeliverEvent( new Event(Event::DISCONNECT, mAddr, Error::OK) );
    mConnMap.Remove(mAddr);
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

  assert(mSd == (int)event->ident);

  if (event->flags & EV_EOF) {
    if (!mConnected)
      DeliverEvent( new Event(Event::DISCONNECT, mAddr, Error::COMM_CONNECT_ERROR) );
    else
      DeliverEvent( new Event(Event::DISCONNECT, mAddr, Error::OK) );
    mConnMap.Remove(mAddr);
    return true;
  }

  if (event->filter == EVFILT_WRITE) {
    if (HandleWriteReadiness()) {
      DeliverEvent( new Event(Event::DISCONNECT, mAddr, Error::OK) );
      mConnMap.Remove(mAddr);
      return true;
    }
  }

  if (event->filter == EVFILT_READ) {
    size_t available = (size_t)event->data;
    size_t nread;
    while (available > 0) {
      if (!mGotHeader) {
	uint8_t *ptr = ((uint8_t *)&mMessageHeader) + (sizeof(Message::HeaderT) - mMessageHeaderRemaining);
	if (mMessageHeaderRemaining < available) {
	  nread = FileUtils::Read(mSd, ptr, mMessageHeaderRemaining);
	  if (nread < 0) {
	    LOG_VA_ERROR("FileUtils::Read(%d, len=%d) failure : %s", mSd, mMessageHeaderRemaining, strerror(errno));
	    DeliverEvent( new Event(Event::DISCONNECT, mAddr, Error::OK) );
	    mConnMap.Remove(mAddr);
	    return true;
	  }
	  assert(nread == mMessageHeaderRemaining);
	  mGotHeader = true;
	  available -= nread;
	  mMessageHeaderRemaining = 0;
	  mMessage = new uint8_t [ mMessageHeader.totalLen ];
	  memcpy(mMessage, &mMessageHeader, sizeof(Message::HeaderT));
	  mMessagePtr = mMessage + sizeof(Message::HeaderT);
	  mMessageRemaining = (mMessageHeader.totalLen) - sizeof(Message::HeaderT);
	}
	else {
	  nread = FileUtils::Read(mSd, ptr, available);
	  if (nread < 0) {
	    LOG_VA_ERROR("FileUtils::Read(%d, len=%d) failure : %s", mSd, available, strerror(errno));
	    DeliverEvent( new Event(Event::DISCONNECT, mAddr, Error::OK) );
	    mConnMap.Remove(mAddr);
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
	    DeliverEvent( new Event(Event::DISCONNECT, mAddr, Error::OK) );
	    mConnMap.Remove(mAddr);
	    return true;
	  }
	  assert(nread == mMessageRemaining);
	  available -= nread;

	  CallbackHandler *cb = 0;
	  uint32_t id = ((Message::HeaderT *)mMessage)->id;
	  if ((((Message::HeaderT *)mMessage)->flags & Message::FLAGS_MASK_REQUEST) == 0 &&
	      (cb = mReactor->RemoveRequest(id)) == 0) {
	    LOG_VA_WARN("Received response for non-pending event (id=%d)", id);
	  }
	  else
	    DeliverEvent( new Event(Event::MESSAGE, mAddr, Error::OK, (Message::HeaderT *)mMessage), cb );
	  ResetIncomingMessageState();
	}
	else {
	  nread = FileUtils::Read(mSd, mMessagePtr, available);
	  if (nread < 0) {
	    LOG_VA_ERROR("FileUtils::Read(%d, len=%d) failure : %s", mSd, available, strerror(errno));
	    DeliverEvent( new Event(Event::DISCONNECT, mAddr, Error::OK) );
	    mConnMap.Remove(mAddr);
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
    DeliverEvent( new Event(Event::CONNECTION_ESTABLISHED, mAddr, Error::OK) );
  }
  else {
    if (FlushSendQueue() != Error::OK)
      return true;
  }
  if (mSendQueue.empty())
    RemovePollInterest(Reactor::WRITE_READY);	
  return false;
}



int IOHandlerData::SendMessage(CommBuf *cbuf, CallbackHandler *responseHandler) {
  boost::mutex::scoped_lock lock(mMutex);
  int error;
  struct timeval tv;
  bool initiallyEmpty = mSendQueue.empty() ? true : false;
  Message::HeaderT *mheader = (Message::HeaderT *)cbuf->data;

  LOG_ENTER;

  if (gettimeofday(&tv, 0) < 0) {
    LOG_VA_ERROR("gettimeofday() failed : %s", strerror(errno));
    exit(1);
  }

  // If request, Add message ID to request cache
  if (responseHandler != 0 && mheader->flags & Message::FLAGS_MASK_REQUEST)
    mReactor->AddRequest(mheader->id, this, responseHandler, tv.tv_sec + mTimeout);

  mSendQueue.push_back(*cbuf);
  cbuf->buf = 0;
  cbuf->ext = 0;

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

    CommBuf &cbuf = mSendQueue.front();

    count = 0;
    towrite = 0;
    if (cbuf.dataLen > 0) {
      vec[0].iov_base = cbuf.data;
      vec[0].iov_len = cbuf.dataLen;
      towrite = cbuf.dataLen;
      ++count;
    }
    if (cbuf.ext != 0) {
      ssize_t remaining = cbuf.extLen - (cbuf.extPtr - cbuf.ext);
      if (remaining > 0) {
	vec[count].iov_base = cbuf.extPtr;
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
      if (cbuf.dataLen > 0) {
	if (nwritten < (ssize_t)cbuf.dataLen) {
	  cbuf.dataLen -= nwritten;
	  cbuf.data += nwritten;
	  break;
	}
	else {
	  nwritten -= cbuf.dataLen;
	  cbuf.dataLen = 0;
	}
      }
      if (cbuf.ext != 0) {
	cbuf.extPtr += nwritten;
	break;
      }
    }

#if 0
    // write header
    if (cbuf.dataLen > 0) {
      nwritten = writen(mSd, cbuf.data, cbuf.dataLen);
      if (nwritten == (ssize_t)-1) {
	LOG_VA_WARN("write(%d, len=%d) failed : %s", mSd, cbuf.dataLen, strerror(errno));
	return Error::COMM_BROKEN_CONNECTION;
      }
      cbuf.dataLen -= nwritten;
      if (nwritten < cbuf.dataLen) {
	cbuf.data += nwritten;
	LOG_VA_WARN("Only flushed %d still have %d", nwritten, cbuf.dataLen);
	break;
      }
    }

    // write data
    if (cbuf.ext != 0) {
      ssize_t remaining = cbuf.extLen - (cbuf.extPtr - cbuf.ext);
      if (remaining > 0) {
	nwritten = writen(mSd, cbuf.extPtr, remaining);
	if (nwritten == (ssize_t)-1) {
	  LOG_VA_WARN("write(%d, len=%d) failed : %s", mSd, remaining, strerror(errno));
	  return Error::COMM_BROKEN_CONNECTION;
	}
	else if (nwritten < remaining) {
	  cbuf.extPtr += nwritten;
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
