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
using namespace Hypertable;

#if defined(__linux__)

bool IOHandlerDatagram::handle_event(struct epoll_event *event) {
  int error;

  //DisplayEvent(event);

  if (event->events & EPOLLOUT) {
    if ((error = handle_write_readiness()) != Error::OK) {
      deliver_event( new Event(Event::ERROR, 0, m_addr, error) );
      return true;
    }
  }

  if (event->events & EPOLLIN) {
    ssize_t nread;
    uint8_t *rmsg;
    struct sockaddr_in addr;
    socklen_t fromlen = sizeof(struct sockaddr_in);

    if ((nread = FileUtils::recvfrom(m_sd, m_message, 65536, (struct sockaddr *)&addr, &fromlen)) == (ssize_t)-1) {
      HT_ERRORF("FileUtils::recvfrom(%d) failure : %s", m_sd, strerror(errno));
      deliver_event( new Event(Event::ERROR, m_sd, addr, Error::COMM_RECEIVE_ERROR) );
      return true;
    }

    rmsg = new uint8_t [ nread ];
    memcpy(rmsg, m_message, nread);

    deliver_event( new Event(Event::MESSAGE, 0, addr, Error::OK, (Header::HeaderT *)rmsg) );

    return false;
  }

  if (event->events & EPOLLERR) {
    HT_WARNF("Received EPOLLERR on descriptor %d (%s:%d)", m_sd, inet_ntoa(m_addr.sin_addr), ntohs(m_addr.sin_port));
    deliver_event( new Event(Event::ERROR, 0, m_addr, Error::COMM_POLL_ERROR) );
    return true;
  }

  return false;
}

#elif defined(__APPLE__)

/**
 *
 */
bool IOHandlerDatagram::handle_event(struct kevent *event) {
  int error;

  //DisplayEvent(event);

  assert(m_sd == (int)event->ident);

  assert((event->flags & EV_EOF) == 0);

  if (event->filter == EVFILT_WRITE) {
    if ((error = handle_write_readiness()) != Error::OK) {
      deliver_event( new Event(Event::ERROR, 0, m_addr, error) );
      return true;
    }
  }

  if (event->filter == EVFILT_READ) {
    size_t available = (size_t)event->data;
    ssize_t nread;
    uint8_t *rmsg;
    struct sockaddr_in addr;
    socklen_t fromlen = sizeof(struct sockaddr_in);

    if ((nread = FileUtils::recvfrom(m_sd, m_message, 65536, (struct sockaddr *)&addr, &fromlen)) == (ssize_t)-1) {
      HT_ERRORF("FileUtils::recvfrom(%d, len=%d) failure : %s", m_sd, available, strerror(errno));
      deliver_event( new Event(Event::ERROR, m_sd, addr, Error::COMM_RECEIVE_ERROR) );
      return true;
    }

    rmsg = new uint8_t [ nread ];
    memcpy(rmsg, m_message, nread);

    deliver_event( new Event(Event::MESSAGE, 0, addr, Error::OK, (Header::HeaderT *)rmsg) );

    return false;
  }

  return false;

}
#else
ImplementMe;
#endif


int IOHandlerDatagram::handle_write_readiness() {
  boost::mutex::scoped_lock lock(m_mutex);
  int error;

  if ((error = flush_send_queue()) != Error::OK)
    return error;

  // is this necessary?
  if (m_send_queue.empty())
    remove_poll_interest(Reactor::WRITE_READY);

  return Error::OK;
}



int IOHandlerDatagram::send_message(struct sockaddr_in &addr, CommBufPtr &cbufPtr) {
  boost::mutex::scoped_lock lock(m_mutex);
  int error;
  bool initiallyEmpty = m_send_queue.empty() ? true : false;

  HT_LOG_ENTER;

  //HT_INFOF("Pushing message destined for %s:%d onto send queue", 
  //inet_ntoa(addr.sin_addr), ntohs(addr.sin_port));

  m_send_queue.push_back(SendRecT(addr, cbufPtr));

  if ((error = flush_send_queue()) != Error::OK)
    return error;

  if (initiallyEmpty && !m_send_queue.empty()) {
    add_poll_interest(Reactor::WRITE_READY);
    //HT_INFO("Adding Write interest");
  }
  else if (!initiallyEmpty && m_send_queue.empty()) {
    remove_poll_interest(Reactor::WRITE_READY);
    //HT_INFO("Removing Write interest");
  }

  return Error::OK;
}



int IOHandlerDatagram::flush_send_queue() {
  ssize_t nsent;

  while (!m_send_queue.empty()) {

    SendRecT &sendRec = m_send_queue.front();

    assert(sendRec.second->dataLen > 0);
    assert(sendRec.second->ext == 0);

    nsent = FileUtils::sendto(m_sd, sendRec.second->data, sendRec.second->dataLen,
			      (const sockaddr*)&sendRec.first, sizeof(struct sockaddr_in));

    if (nsent == (ssize_t)-1) {
      HT_WARNF("FileUtils::sendto(%d, len=%d, addr=%s:%d) failed : %s", m_sd, sendRec.second->dataLen,
		  inet_ntoa(sendRec.first.sin_addr), ntohs(sendRec.first.sin_port), strerror(errno));
      return Error::COMM_SEND_ERROR;
    }
    else if (nsent < (ssize_t)sendRec.second->dataLen) {
      HT_WARNF("Only sent %d bytes", nsent);
      if (nsent == 0)
	break;
      sendRec.second->dataLen -= nsent;
      sendRec.second->data += nsent;
      break;
    }

    //HT_INFOF("Successfully sent message to %s:%d", inet_ntoa(sendRec.first.sin_addr), ntohs(sendRec.first.sin_port));

    // buffer written successfully, now remove from queue (which will destroy buffer)
    m_send_queue.pop_front();
  }

  return Error::OK;
}
