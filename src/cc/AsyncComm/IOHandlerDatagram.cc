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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"

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

#define HT_DISABLE_LOG_DEBUG 1

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
      deliver_event(new Event(Event::ERROR, 0, m_addr, error));
      return true;
    }
  }

  if (event->events & EPOLLIN) {
    ssize_t nread;
    uint8_t *rmsg;
    struct sockaddr_in addr;
    socklen_t fromlen = sizeof(struct sockaddr_in);

    while ((nread = FileUtils::recvfrom(m_sd, m_message, 65536,
            (struct sockaddr *)&addr, &fromlen)) != (ssize_t)-1) {
      rmsg = new uint8_t [nread];
      memcpy(rmsg, m_message, nread);
      deliver_event(new Event(Event::MESSAGE, 0, addr, Error::OK,
                              (Header::Common *)rmsg));
      fromlen = sizeof(struct sockaddr_in);
    }

    if (errno != EAGAIN) {
      HT_ERRORF("FileUtils::recvfrom(%d) failure : %s", m_sd, strerror(errno));
      deliver_event(new Event(Event::ERROR, m_sd, addr,
                              Error::COMM_RECEIVE_ERROR));
      return true;
    }

    return false;
  }

  if (event->events & EPOLLERR) {
    HT_WARNF("Received EPOLLERR on descriptor %d (%s)",
             m_sd, InetAddr::format(m_addr));
    deliver_event(new Event(Event::ERROR, 0, m_addr, Error::COMM_POLL_ERROR));
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
      deliver_event(new Event(Event::ERROR, 0, m_addr, error));
      return true;
    }
  }

  if (event->filter == EVFILT_READ) {
    size_t available = (size_t)event->data;
    ssize_t nread;
    uint8_t *rmsg;
    struct sockaddr_in addr;
    socklen_t fromlen = sizeof(struct sockaddr_in);

    if ((nread = FileUtils::recvfrom(m_sd, m_message, 65536,
        (struct sockaddr *)&addr, &fromlen)) == (ssize_t)-1) {
      HT_ERRORF("FileUtils::recvfrom(%d, len=%d) failure : %s", m_sd,
                (int)available, strerror(errno));
      deliver_event(new Event(Event::ERROR, m_sd, addr,
                              Error::COMM_RECEIVE_ERROR));
      return true;
    }

    rmsg = new uint8_t [nread];
    memcpy(rmsg, m_message, nread);

    deliver_event(new Event(Event::MESSAGE, 0, addr, Error::OK,
                            (Header::Common *)rmsg));

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



int IOHandlerDatagram::send_message(struct sockaddr_in &addr, CommBufPtr &cbp) {
  boost::mutex::scoped_lock lock(m_mutex);
  int error;
  bool initially_empty = m_send_queue.empty() ? true : false;

  HT_LOG_ENTER;

  //HT_INFOF("Pushing message destined for %s:%d onto send queue",
  //inet_ntoa(addr.sin_addr), ntohs(addr.sin_port));

  m_send_queue.push_back(SendRec(addr, cbp));

  if ((error = flush_send_queue()) != Error::OK)
    return error;

  if (initially_empty && !m_send_queue.empty()) {
    add_poll_interest(Reactor::WRITE_READY);
    //HT_INFO("Adding Write interest");
  }
  else if (!initially_empty && m_send_queue.empty()) {
    remove_poll_interest(Reactor::WRITE_READY);
    //HT_INFO("Removing Write interest");
  }

  return Error::OK;
}



int IOHandlerDatagram::flush_send_queue() {
  ssize_t nsent, tosend;

  while (!m_send_queue.empty()) {

    SendRec &send_rec = m_send_queue.front();

    tosend = send_rec.second->data.size - (send_rec.second->data_ptr
                                           - send_rec.second->data.base);
    assert(tosend > 0);
    assert(send_rec.second->ext.base == 0);

    nsent = FileUtils::sendto(m_sd, send_rec.second->data_ptr, tosend,
                              (sockaddr *)&send_rec.first,
                              sizeof(struct sockaddr_in));

    if (nsent == (ssize_t)-1) {
      HT_WARNF("FileUtils::sendto(%d, len=%d, addr=%s:%d) failed : %s", m_sd,
               (int)tosend, inet_ntoa(send_rec.first.sin_addr),
               ntohs(send_rec.first.sin_port), strerror(errno));
      return Error::COMM_SEND_ERROR;
    }
    else if (nsent < tosend) {
      HT_WARNF("Only sent %d bytes", (int)nsent);
      if (nsent == 0)
        break;
      send_rec.second->data_ptr += nsent;
      break;
    }

    // buffer written successfully, now remove from queue (destroys buffer)
    m_send_queue.pop_front();
  }

  return Error::OK;
}
