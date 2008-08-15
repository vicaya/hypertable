/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
#include <sys/socket.h>
#include <sys/types.h>
#if defined(__APPLE__)
#include <sys/event.h>
#endif
}

#define HT_DISABLE_LOG_DEBUG 1

#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/InetAddr.h"

#include "IOHandlerData.h"
using namespace Hypertable;

atomic_t IOHandlerData::ms_next_connection_id = ATOMIC_INIT(1);

#if defined(__linux__)

namespace {

  /**
   * Used to read data off a socket that is monotored with edge-triggered epoll.
   * When this function returns with *errnop set to EAGAIN, it is safe to call
   * epoll_wait on this socket.
   */
  ssize_t et_socket_read(int fd, void *vptr, size_t n, int *errnop, bool *eofp) {
    size_t nleft;
    ssize_t nread;
    char *ptr;

    ptr = (char *)vptr;
    nleft = n;
    while (nleft > 0) {
      if ((nread = ::read(fd, ptr, nleft)) < 0) {
        if (errno == EINTR) {
          nread = 0; /* and call read() again */
          continue;
        }
        *errnop = errno;
        if (nleft < n)
          break;
        return -1;
      }
      else if (nread == 0) {
        *eofp = true;
        break;
      }

      nleft -= nread;
      ptr   += nread;
    }
    return n - nleft;
  }

  ssize_t et_socket_writev(int fd, const struct iovec *vector, int count, int *errnop) {
    ssize_t nwritten;
    while ((nwritten = ::writev(fd, vector, count)) <= 0) {
      if (errno == EINTR) {
        nwritten = 0; /* and call write() again */
        continue;
      }
      *errnop = errno;
      return -1;
    }
    return nwritten;
  }


}

bool IOHandlerData::handle_event(struct epoll_event *event) {
  int error = 0;
  bool eof = false;

  //DisplayEvent(event);

  if (event->events & EPOLLOUT) {
    if (handle_write_readiness()) {
      m_reactor_ptr->cancel_requests(this);
      deliver_event(new Event(Event::DISCONNECT, m_id, m_addr, Error::OK));
      return true;
    }
  }

  if (event->events & EPOLLIN) {
    size_t nread;
    while (true) {
      if (!m_got_header) {
        uint8_t *ptr = ((uint8_t *)&m_message_header) + (sizeof(Header::Common) - m_message_header_remaining);
        nread = et_socket_read(m_sd, ptr, m_message_header_remaining, &error, &eof);
        if (nread == (size_t)-1) {
          if (error == EAGAIN)
            break;
          if (errno != ECONNREFUSED) {
            HT_ERRORF("FileUtils::read(%d, len=%d) failure : %s", m_sd, m_message_header_remaining, strerror(errno));
          }
          m_reactor_ptr->cancel_requests(this);
          error = (errno == ECONNREFUSED) ? Error::COMM_CONNECT_ERROR : Error::OK;
          //HT_ERRORF("read returned -1 : %s", strerror(errno));
          deliver_event(new Event(Event::DISCONNECT, m_id, m_addr, error));
          return true;
        }
        else if (nread < m_message_header_remaining) {
          m_message_header_remaining -= nread;
          if (error == EAGAIN)
            break;
          error = 0;
        }
        else {
          m_got_header = true;
          m_message_header_remaining = 0;
          m_message = new uint8_t [m_message_header.total_len];
          memcpy(m_message, &m_message_header, sizeof(Header::Common));
          m_message_ptr = m_message + sizeof(Header::Common);
          m_message_remaining = (m_message_header.total_len) - sizeof(Header::Common);
        }
        if (eof)
          break;
      }
      if (m_got_header) {
        nread = et_socket_read(m_sd, m_message_ptr, m_message_remaining, &error, &eof);
        if (nread < 0) {
          if (error == EAGAIN)
            break;
          HT_ERRORF("FileUtils::read(%d, len=%d) failure : %s", m_sd, m_message_header_remaining, strerror(errno));
          m_reactor_ptr->cancel_requests(this);
          deliver_event(new Event(Event::DISCONNECT, m_id, m_addr, Error::OK));
          return true;
        }
        else if (nread < m_message_remaining) {
          m_message_ptr += nread;
          m_message_remaining -= nread;
          if (error == EAGAIN)
            break;
          error = 0;
        }
        else {
          DispatchHandler *dh = 0;
          uint32_t id = ((Header::Common *)m_message)->id;
          if ((((Header::Common *)m_message)->flags & Header::FLAGS_BIT_REQUEST) == 0 &&
              (id == 0 || (dh = m_reactor_ptr->remove_request(id)) == 0)) {
            if ((((Header::Common *)m_message)->flags & Header::FLAGS_BIT_IGNORE_RESPONSE) == 0) {
              HT_WARNF("Received response for non-pending event (id=%d,version=%d,total_len=%d)",
                          id, ((Header::Common *)m_message)->version, ((Header::Common *)m_message)->total_len);
            }
            delete [] m_message;
          }
          else
            deliver_event(new Event(Event::MESSAGE, m_id, m_addr, Error::OK, (Header::Common *)m_message), dh);
          reset_incoming_message_state();
        }
        if (eof)
          break;
      }
    }
  }

#if defined(HT_EPOLLET)
  if (event->events & POLLRDHUP) {
    HT_DEBUGF("Received POLLRDHUP on descriptor %d (%s:%d)", m_sd, inet_ntoa(m_addr.sin_addr), ntohs(m_addr.sin_port));
    m_reactor_ptr->cancel_requests(this);
    deliver_event(new Event(Event::DISCONNECT, m_id, m_addr, Error::OK));
    return true;
  }
#else
  if (eof) {
    HT_DEBUGF("Received EOF on descriptor %d (%s:%d)", m_sd, inet_ntoa(m_addr.sin_addr), ntohs(m_addr.sin_port));
    m_reactor_ptr->cancel_requests(this);
    deliver_event(new Event(Event::DISCONNECT, m_id, m_addr, Error::OK));
    return true;
  }
#endif

  if (event->events & EPOLLERR) {
    HT_ERRORF("Received EPOLLERR on descriptor %d (%s:%d)", m_sd, inet_ntoa(m_addr.sin_addr), ntohs(m_addr.sin_port));
    m_reactor_ptr->cancel_requests(this);
    deliver_event(new Event(Event::DISCONNECT, m_id, m_addr, Error::OK));
    return true;
  }

  if (event->events & EPOLLHUP) {
    HT_DEBUGF("Received EPOLLHUP on descriptor %d (%s:%d)", m_sd, inet_ntoa(m_addr.sin_addr), ntohs(m_addr.sin_port));
    m_reactor_ptr->cancel_requests(this);
    deliver_event(new Event(Event::DISCONNECT, m_id, m_addr, Error::OK));
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
    m_reactor_ptr->cancel_requests(this);
    if (!m_connected)
      deliver_event(new Event(Event::DISCONNECT, m_id, m_addr, Error::COMM_CONNECT_ERROR));
    else
      deliver_event(new Event(Event::DISCONNECT, m_id, m_addr, Error::OK));
    return true;
  }

  if (event->filter == EVFILT_WRITE) {
    if (handle_write_readiness()) {
      m_reactor_ptr->cancel_requests(this);
      deliver_event(new Event(Event::DISCONNECT, m_id, m_addr, Error::OK));
      return true;
    }
  }

  if (event->filter == EVFILT_READ) {
    size_t available = (size_t)event->data;
    size_t nread;
    while (available > 0) {
      if (!m_got_header) {
        uint8_t *ptr = ((uint8_t *)&m_message_header) + (sizeof(Header::Common) - m_message_header_remaining);
        if (m_message_header_remaining < available) {
          nread = FileUtils::read(m_sd, ptr, m_message_header_remaining);
          if (nread < 0) {
            HT_ERRORF("FileUtils::read(%d, len=%d) failure : %s", m_sd, m_message_header_remaining, strerror(errno));
            m_reactor_ptr->cancel_requests(this);
            deliver_event(new Event(Event::DISCONNECT, m_id, m_addr, Error::OK));
            return true;
          }
          assert(nread == m_message_header_remaining);
          m_got_header = true;
          available -= nread;
          m_message_header_remaining = 0;
          m_message = new uint8_t [m_message_header.total_len];
          memcpy(m_message, &m_message_header, sizeof(Header::Common));
          m_message_ptr = m_message + sizeof(Header::Common);
          m_message_remaining = (m_message_header.total_len) - sizeof(Header::Common);
        }
        else {
          nread = FileUtils::read(m_sd, ptr, available);
          if (nread < 0) {
            HT_ERRORF("FileUtils::read(%d, len=%d) failure : %s", m_sd, available, strerror(errno));
            m_reactor_ptr->cancel_requests(this);
            deliver_event(new Event(Event::DISCONNECT, m_id, m_addr, Error::OK));
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
            HT_ERRORF("FileUtils::read(%d, len=%d) failure : %s", m_sd, m_message_remaining, strerror(errno));
            m_reactor_ptr->cancel_requests(this);
            deliver_event(new Event(Event::DISCONNECT, m_id, m_addr, Error::OK));
            return true;
          }
          assert(nread == m_message_remaining);
          available -= nread;

          DispatchHandler *dh = 0;
          uint32_t id = ((Header::Common *)m_message)->id;
          if ((((Header::Common *)m_message)->flags & Header::FLAGS_BIT_REQUEST) == 0 &&
              (id == 0 || (dh = m_reactor_ptr->remove_request(id)) == 0)) {
            if ((((Header::Common *)m_message)->flags & Header::FLAGS_BIT_IGNORE_RESPONSE) == 0) {
              HT_WARNF("Received response for non-pending event (id=%d)", id);
            }
            delete [] m_message;
          }
          else
            deliver_event(new Event(Event::MESSAGE, m_id, m_addr, Error::OK, (Header::Common *)m_message), dh);
          reset_incoming_message_state();
        }
        else {
          nread = FileUtils::read(m_sd, m_message_ptr, available);
          if (nread < 0) {
            HT_ERRORF("FileUtils::read(%d, len=%d) failure : %s", m_sd, available, strerror(errno));
            m_reactor_ptr->cancel_requests(this);
            deliver_event(new Event(Event::DISCONNECT, m_id, m_addr, Error::OK));
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
    socklen_t name_len = sizeof(m_local_addr);
    int sockerr = 0;
    socklen_t sockerr_len = sizeof(sockerr);

    if (getsockopt(m_sd, SOL_SOCKET, SO_ERROR, &sockerr, &sockerr_len) < 0) {
      HT_ERRORF("getsockopt(SO_ERROR) failed - %s", strerror(errno));
    }

    if (sockerr) {
      HT_ERRORF("connect() completion error - %s", strerror(sockerr));
      return true;
    }

    int bufsize = 4*32768;
    if (setsockopt(m_sd, SOL_SOCKET, SO_SNDBUF, (char *)&bufsize, sizeof(bufsize)) < 0) {
      HT_ERRORF("setsockopt(SO_SNDBUF) failed - %s", strerror(errno));
    }
    if (setsockopt(m_sd, SOL_SOCKET, SO_RCVBUF, (char *)&bufsize, sizeof(bufsize)) < 0) {
      HT_ERRORF("setsockopt(SO_RCVBUF) failed - %s", strerror(errno));
    }

    if (getsockname(m_sd, (struct sockaddr *)&m_local_addr, &name_len) < 0) {
      HT_ERRORF("getsockname(%d) failed - %s", m_sd, strerror(errno));
      return true;
    }
    //clog << "Connection established." << endl;
    m_connected = true;
    deliver_event(new Event(Event::CONNECTION_ESTABLISHED, m_id, m_addr, Error::OK));
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



int IOHandlerData::send_message(CommBufPtr &cbp, time_t timeout, DispatchHandler *disp_handler) {
  boost::mutex::scoped_lock lock(m_mutex);
  int error;
  bool initially_empty = m_send_queue.empty() ? true : false;
  Header::Common *mheader = (Header::Common *)cbp->data.base;

  HT_LOG_ENTER;

  // If request, Add message ID to request cache
  if (mheader->id != 0 && disp_handler != 0 && mheader->flags & Header::FLAGS_BIT_REQUEST) {
    boost::xtime expire_time;
    boost::xtime_get(&expire_time, boost::TIME_UTC);
    expire_time.sec += timeout;
    m_reactor_ptr->add_request(mheader->id, this, disp_handler, expire_time);
  }

  m_send_queue.push_back(cbp);

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



#if defined(__linux__)

int IOHandlerData::flush_send_queue() {
  ssize_t nwritten, towrite, remaining;
  struct iovec vec[2];
  int count;
  int error = 0;

  while (!m_send_queue.empty()) {

    CommBufPtr &cbp = m_send_queue.front();

    count = 0;
    towrite = 0;
    remaining = cbp->data.size - (cbp->data_ptr - cbp->data.base);
    if (remaining > 0) {
      vec[0].iov_base = (void *)cbp->data_ptr;
      vec[0].iov_len = remaining;
      towrite = remaining;
      ++count;
    }
    if (cbp->ext.base != 0) {
      remaining = cbp->ext.size - (cbp->ext_ptr - cbp->ext.base);
      if (remaining > 0) {
        vec[count].iov_base = (void *)cbp->ext_ptr;
        vec[count].iov_len = remaining;
        towrite += remaining;
        ++count;
      }
    }

    nwritten = et_socket_writev(m_sd, vec, count, &error);
    if (nwritten == (ssize_t)-1) {
      if (error == EAGAIN)
        return Error::OK;
      HT_WARNF("FileUtils::writev(%d, len=%d) failed : %s", m_sd, towrite, strerror(errno));
      return Error::COMM_BROKEN_CONNECTION;
    }
    else if (nwritten < towrite) {
      if (nwritten == 0) {
        if (error == EAGAIN)
          break;
        if (error) {
          HT_WARNF("FileUtils::writev(%d, len=%d) failed : %s", m_sd, towrite, strerror(error));
          return Error::COMM_BROKEN_CONNECTION;
        }
        continue;
      }
      remaining = cbp->data.size - (cbp->data_ptr - cbp->data.base);
      if (remaining > 0) {
        if (nwritten < remaining) {
          cbp->data_ptr += nwritten;
          if (error == EAGAIN)
            break;
          error = 0;
          continue;
        }
        else {
          nwritten -= remaining;
          cbp->data_ptr += remaining;
        }
      }
      if (cbp->ext.base != 0) {
        cbp->ext_ptr += nwritten;
        if (error == EAGAIN)
          break;
        error = 0;
        continue;
      }
    }

    // buffer written successfully, now remove from queue (which will destroy buffer)
    m_send_queue.pop_front();
  }

  return Error::OK;
}

#elif defined(__APPLE__)

int IOHandlerData::flush_send_queue() {
  ssize_t nwritten, towrite, remaining;
  struct iovec vec[2];
  int count;

  while (!m_send_queue.empty()) {

    CommBufPtr &cbp = m_send_queue.front();

    count = 0;
    towrite = 0;
    remaining = cbp->data.size - (cbp->data_ptr - cbp->data.base);
    if (remaining > 0) {
      vec[0].iov_base = (void *)cbp->data_ptr;
      vec[0].iov_len = remaining;
      towrite = remaining;
      ++count;
    }
    if (cbp->ext.base != 0) {
      remaining = cbp->ext.size - (cbp->ext_ptr - cbp->ext.base);
      if (remaining > 0) {
        vec[count].iov_base = (void *)cbp->ext_ptr;
        vec[count].iov_len = remaining;
        towrite += remaining;
        ++count;
      }
    }

    nwritten = FileUtils::writev(m_sd, vec, count);
    if (nwritten == (ssize_t)-1) {
      HT_WARNF("FileUtils::writev(%d, len=%d) failed : %s", m_sd, towrite, strerror(errno));
      return Error::COMM_BROKEN_CONNECTION;
    }
    else if (nwritten < towrite) {
      if (nwritten == 0)
        break;
      remaining = cbp->data.size - (cbp->data_ptr - cbp->data.base);
      if (remaining > 0) {
        if (nwritten < remaining) {
          cbp->data_ptr += nwritten;
          break;
        }
        else {
          nwritten -= remaining;
          cbp->data_ptr += remaining;
        }
      }
      if (cbp->ext.base != 0) {
        cbp->ext_ptr += nwritten;
        break;
      }
    }

    // buffer written successfully, now remove from queue (which will destroy buffer)
    m_send_queue.pop_front();
  }

  return Error::OK;
}


#else
  ImplementMe;
#endif
