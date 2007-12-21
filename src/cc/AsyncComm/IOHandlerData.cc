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
#include "Common/InetAddr.h"

#include "IOHandlerData.h"
using namespace Hypertable;

atomic_t IOHandlerData::ms_next_connection_id = ATOMIC_INIT(1);

#if defined(__linux__)

bool IOHandlerData::handle_event(struct epoll_event *event) {

  //DisplayEvent(event);

  if (event->events & EPOLLOUT) {
    if (handle_write_readiness()) {
      deliver_event( new Event(Event::DISCONNECT, m_id, m_addr, Error::OK) );
      return true;
    }
  }

  if (event->events & EPOLLIN) {
    size_t nread, totalRead = 0;
    while (true) {
      if (!m_got_header) {
	uint8_t *ptr = ((uint8_t *)&m_message_header) + (sizeof(Header::HeaderT) - m_message_header_remaining);
	nread = FileUtils::read(m_sd, ptr, m_message_header_remaining);
	if (nread == (size_t)-1) {
	  if (errno != ECONNREFUSED) {
	    LOG_VA_ERROR("FileUtils::read(%d, len=%d) failure : %s", m_sd, m_message_header_remaining, strerror(errno));
	  }
	  int error = (errno == ECONNREFUSED) ? Error::COMM_CONNECT_ERROR : Error::OK;
	  deliver_event( new Event(Event::DISCONNECT, m_id, m_addr, error ) );
	  return true;
	}
	else if (nread == 0 && totalRead == 0) {
	  // eof
	  deliver_event( new Event(Event::DISCONNECT, m_id, m_addr, Error::OK) );
	  return true;
	}
	else if (nread < m_message_header_remaining) {
	  m_message_header_remaining -= nread;
	  return false;
	}
	else {
	  m_got_header = true;
	  m_message_header_remaining = 0;
	  m_message = new uint8_t [ m_message_header.totalLen ];
	  memcpy(m_message, &m_message_header, sizeof(Header::HeaderT));
	  m_message_ptr = m_message + sizeof(Header::HeaderT);
	  m_message_remaining = (m_message_header.totalLen) - sizeof(Header::HeaderT);
	  totalRead += nread;
	}
      }
      if (m_got_header) {
	nread = FileUtils::read(m_sd, m_message_ptr, m_message_remaining);
	if (nread < 0) {
	  LOG_VA_ERROR("FileUtils::read(%d, len=%d) failure : %s", m_sd, m_message_header_remaining, strerror(errno));
	  deliver_event( new Event(Event::DISCONNECT, m_id, m_addr, Error::OK) );
	  return true;
	}
	else if (nread == 0 && totalRead == 0) {
	  // eof
	  deliver_event( new Event(Event::DISCONNECT, m_id, m_addr, Error::OK) );
	  return true;
	}
	else if (nread < m_message_remaining) {
	  m_message_ptr += nread;
	  m_message_remaining -= nread;
	}
	else {
	  DispatchHandler *dh = 0;
	  uint32_t id = ((Header::HeaderT *)m_message)->id;
	  if (id != 0 &&
	      (((Header::HeaderT *)m_message)->flags & Header::FLAGS_MASK_REQUEST) == 0 &&
	      (dh = m_reactor_ptr->remove_request(id)) == 0) {
	    LOG_VA_WARN("Received response for non-pending event (id=%d,version=%d,totalLen=%d)",
			id, ((Header::HeaderT *)m_message)->version, ((Header::HeaderT *)m_message)->totalLen);
	    delete [] m_message;
	  }
	  else
	    deliver_event( new Event(Event::MESSAGE, m_id, m_addr, Error::OK, (Header::HeaderT *)m_message), dh );
	  reset_incoming_message_state();
	}
	totalRead += nread;
      }
    }
  }

  if (event->events & EPOLLERR) {
    LOG_VA_WARN("Received EPOLLERR on descriptor %d (%s:%d)", m_sd, inet_ntoa(m_addr.sin_addr), ntohs(m_addr.sin_port));
    deliver_event( new Event(Event::DISCONNECT, m_id, m_addr, Error::OK) );
    return true;
  }

  if (event->events & EPOLLHUP) {
    LOG_VA_WARN("Received EPOLLHUP on descriptor %d (%s:%d)", m_sd, inet_ntoa(m_addr.sin_addr), ntohs(m_addr.sin_port));
    deliver_event( new Event(Event::DISCONNECT, m_id, m_addr, Error::OK) );
    return true;
  }

  return false;
}

#elif defined(__APPLE__)

/**
 *
 */
bool IOHandlerData::handle_event(struct kevent *event) {

  //DisplayEvent(event);

  assert(m_sd == (int)event->ident);

  if (event->flags & EV_EOF) {
    if (!m_connected)
      deliver_event( new Event(Event::DISCONNECT, m_id, m_addr, Error::COMM_CONNECT_ERROR) );
    else
      deliver_event( new Event(Event::DISCONNECT, m_id, m_addr, Error::OK) );
    return true;
  }

  if (event->filter == EVFILT_WRITE) {
    if (handle_write_readiness()) {
      deliver_event( new Event(Event::DISCONNECT, m_id, m_addr, Error::OK) );
      return true;
    }
  }

  if (event->filter == EVFILT_READ) {
    size_t available = (size_t)event->data;
    size_t nread;
    while (available > 0) {
      if (!m_got_header) {
	uint8_t *ptr = ((uint8_t *)&m_message_header) + (sizeof(Header::HeaderT) - m_message_header_remaining);
	if (m_message_header_remaining < available) {
	  nread = FileUtils::read(m_sd, ptr, m_message_header_remaining);
	  if (nread < 0) {
	    LOG_VA_ERROR("FileUtils::read(%d, len=%d) failure : %s", m_sd, m_message_header_remaining, strerror(errno));
	    deliver_event( new Event(Event::DISCONNECT, m_id, m_addr, Error::OK) );
	    return true;
	  }
	  assert(nread == m_message_header_remaining);
	  m_got_header = true;
	  available -= nread;
	  m_message_header_remaining = 0;
	  m_message = new uint8_t [ m_message_header.totalLen ];
	  memcpy(m_message, &m_message_header, sizeof(Header::HeaderT));
	  m_message_ptr = m_message + sizeof(Header::HeaderT);
	  m_message_remaining = (m_message_header.totalLen) - sizeof(Header::HeaderT);
	}
	else {
	  nread = FileUtils::read(m_sd, ptr, available);
	  if (nread < 0) {
	    LOG_VA_ERROR("FileUtils::read(%d, len=%d) failure : %s", m_sd, available, strerror(errno));
	    deliver_event( new Event(Event::DISCONNECT, m_id, m_addr, Error::OK) );
	    return true;
	  }
	  assert(nread == available);
	  m_message_header_remaining -= nread;
	  return false;
	}
      }
      if (m_got_header) {
	if (m_message_remaining <= available) {
	  nread = FileUtils::read(m_sd, m_message_ptr, m_message_remaining);
	  if (nread < 0) {
	    LOG_VA_ERROR("FileUtils::read(%d, len=%d) failure : %s", m_sd, m_message_remaining, strerror(errno));
	    deliver_event( new Event(Event::DISCONNECT, m_id, m_addr, Error::OK) );
	    return true;
	  }
	  assert(nread == m_message_remaining);
	  available -= nread;

	  DispatchHandler *dh = 0;
	  uint32_t id = ((Header::HeaderT *)m_message)->id;
	  if (id != 0 && 
	      (((Header::HeaderT *)m_message)->flags & Header::FLAGS_MASK_REQUEST) == 0 &&
	      (dh = m_reactor_ptr->remove_request(id)) == 0) {
	    LOG_VA_WARN("Received response for non-pending event (id=%d)", id);
	    delete [] m_message;
	  }
	  else
	    deliver_event( new Event(Event::MESSAGE, m_id, m_addr, Error::OK, (Header::HeaderT *)m_message), dh );
	  reset_incoming_message_state();
	}
	else {
	  nread = FileUtils::read(m_sd, m_message_ptr, available);
	  if (nread < 0) {
	    LOG_VA_ERROR("FileUtils::read(%d, len=%d) failure : %s", m_sd, available, strerror(errno));
	    deliver_event( new Event(Event::DISCONNECT, m_id, m_addr, Error::OK) );
	    return true;
	  }
	  assert(nread == available);
	  m_message_ptr += nread;
	  m_message_remaining -= nread;
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


bool IOHandlerData::handle_write_readiness() {

  if (m_connected == false) {
    socklen_t nameLen = sizeof(m_local_addr);

    int bufsize = 4*32768;
    if (setsockopt(m_sd, SOL_SOCKET, SO_SNDBUF, (char *)&bufsize, sizeof(bufsize)) < 0) {
      LOG_VA_ERROR("setsockopt(SO_SNDBUF) failed - %s", strerror(errno));
    }
    if (setsockopt(m_sd, SOL_SOCKET, SO_RCVBUF, (char *)&bufsize, sizeof(bufsize)) < 0) {
      LOG_VA_ERROR("setsockopt(SO_RCVBUF) failed - %s", strerror(errno));
    }

    if (getsockname(m_sd, (struct sockaddr *)&m_local_addr, &nameLen) < 0) {
      LOG_VA_ERROR("getsockname(%d) failed - %s", m_sd, strerror(errno));
      exit(1);
    }
    //clog << "Connection established." << endl;
    m_connected = true;
    deliver_event( new Event(Event::CONNECTION_ESTABLISHED, m_id, m_addr, Error::OK) );
  }
  else {
    boost::mutex::scoped_lock lock(m_mutex);
    if (flush_send_queue() != Error::OK)
      return true;
    if (m_send_queue.empty())
      remove_poll_interest(Reactor::WRITE_READY);	
  }
  return false;
}



int IOHandlerData::send_message(CommBufPtr &cbufPtr, time_t timeout, DispatchHandler *dispatchHandler) {
  boost::mutex::scoped_lock lock(m_mutex);
  int error;
  bool initiallyEmpty = m_send_queue.empty() ? true : false;
  Header::HeaderT *mheader = (Header::HeaderT *)cbufPtr->data;

  LOG_ENTER;
  
  // If request, Add message ID to request cache
  if (mheader->id != 0 && dispatchHandler != 0 && mheader->flags & Header::FLAGS_MASK_REQUEST) {
    boost::xtime expireTime;
    boost::xtime_get(&expireTime, boost::TIME_UTC);
    expireTime.sec += timeout;
    m_reactor_ptr->add_request(mheader->id, this, dispatchHandler, expireTime);
  }

  m_send_queue.push_back(cbufPtr);

  if ((error = flush_send_queue()) != Error::OK)
    return error;

  if (initiallyEmpty && !m_send_queue.empty()) {
    add_poll_interest(Reactor::WRITE_READY);
    //LOG_INFO("Adding Write interest");
  }
  else if (!initiallyEmpty && m_send_queue.empty()) {
    remove_poll_interest(Reactor::WRITE_READY);
    //LOG_INFO("Removing Write interest");
  }

  return Error::OK;
}



int IOHandlerData::flush_send_queue() {
  ssize_t nwritten, towrite;
  struct iovec vec[2];
  int count;

  while (!m_send_queue.empty()) {

    CommBufPtr &cbufPtr = m_send_queue.front();

    count = 0;
    towrite = 0;
    if (cbufPtr->dataLen > 0) {
      vec[0].iov_base = (void *)cbufPtr->data;
      vec[0].iov_len = cbufPtr->dataLen;
      towrite = cbufPtr->dataLen;
      ++count;
    }
    if (cbufPtr->ext != 0) {
      ssize_t remaining = cbufPtr->extLen - (cbufPtr->extPtr - cbufPtr->ext);
      if (remaining > 0) {
	vec[count].iov_base = (void *)cbufPtr->extPtr;
	vec[count].iov_len = remaining;
	towrite += remaining;
	++count;
      }
    }

    nwritten = FileUtils::writev(m_sd, vec, count);
    if (nwritten == (ssize_t)-1) {
      LOG_VA_WARN("FileUtils::writev(%d, len=%d) failed : %s", m_sd, towrite, strerror(errno));
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

    // buffer written successfully, now remove from queue (which will destroy buffer)
    m_send_queue.pop_front();
  }

  return Error::OK;
}
