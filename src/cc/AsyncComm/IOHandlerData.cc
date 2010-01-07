/**
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
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

extern "C" {
#include <arpa/inet.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/types.h>
#if defined(__APPLE__) || defined(__FreeBSD__)
#include <sys/event.h>
#endif
#include <sys/uio.h>
}

#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/InetAddr.h"
#include "Common/Time.h"

#include "IOHandlerData.h"

using namespace Hypertable;
using namespace std;


namespace {

  /**
   * Used to read data off a socket that is monotored with edge-triggered epoll.
   * When this function returns with *errnop set to EAGAIN, it is safe to call
   * epoll_wait on this socket.
   */
  ssize_t
  et_socket_read(int fd, void *vptr, size_t n, int *errnop, bool *eofp) {
    size_t nleft = n;
    ssize_t nread;
    char *ptr = (char *)vptr;

    while (nleft > 0) {
      if ((nread = ::read(fd, ptr, nleft)) < 0) {
        if (errno == EINTR) {
          nread = 0; /* and call read() again */
          continue;
        }
        *errnop = errno;

        if (*errnop == EAGAIN || nleft < n)
          break;     /* already read something, most likely EAGAIN */

        return -1;   /* other errors and nothing read */
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

  ssize_t
  et_socket_writev(int fd, const iovec *vector, int count, int *errnop) {
    ssize_t nwritten;
    while ((nwritten = writev(fd, vector, count)) <= 0) {
      if (errno == EINTR) {
        nwritten = 0; /* and call write() again */
        continue;
      }
      *errnop = errno;
      return -1;
    }
    return nwritten;
  }

} // local namespace


bool
IOHandlerData::handle_event(struct pollfd *event, clock_t arrival_clocks, time_t arrival_time) {
  int error = 0;
  bool eof = false;

  //DisplayEvent(event);

  try {
    if (event->revents & POLLOUT) {
      if (handle_write_readiness()) {
        handle_disconnect();
        return true;
      }
    }

    if (event->revents & POLLIN) {
      size_t nread;
      while (true) {
        if (!m_got_header) {
          nread = et_socket_read(m_sd, m_message_header_ptr,
                                 m_message_header_remaining, &error, &eof);
          if (nread == (size_t)-1) {
            if (errno != ECONNREFUSED) {
              HT_ERRORF("socket read(%d, len=%d) failure : %s", m_sd,
                        (int)m_message_header_remaining, strerror(errno));
              error = Error::OK;
            }
            else
              error = Error::COMM_CONNECT_ERROR;

            handle_disconnect(error);
            return true;
          }
          else if (nread < m_message_header_remaining) {
            m_message_header_remaining -= nread;
            m_message_header_ptr += nread;
            if (error == EAGAIN)
              break;
            error = 0;
          }
          else {
            m_message_header_ptr += nread;
            handle_message_header(arrival_clocks, arrival_time);
          }

          if (eof)
            break;
        }
        else { // got header
          nread = et_socket_read(m_sd, m_message_ptr, m_message_remaining,
                                 &error, &eof);
          if (nread == (size_t)-1) {
            HT_ERRORF("socket read(%d, len=%d) failure : %s", m_sd,
                      (int)m_message_header_remaining, strerror(errno));
            handle_disconnect();
            return true;
          }
          else if (nread < m_message_remaining) {
            m_message_ptr += nread;
            m_message_remaining -= nread;
            if (error == EAGAIN)
              break;
            error = 0;
          }
          else
            handle_message_body();

          if (eof)
            break;
        }
      }
    }

    if (eof) {
      HT_DEBUGF("Received EOF on descriptor %d (%s:%d)", m_sd,
		inet_ntoa(m_addr.sin_addr), ntohs(m_addr.sin_port));
      handle_disconnect();
      return true;
    }

    if (event->revents & POLLERR) {
      HT_ERRORF("Received POLLERR on descriptor %d (%s:%d)", m_sd,
                inet_ntoa(m_addr.sin_addr), ntohs(m_addr.sin_port));
      handle_disconnect();
      return true;
    }

    if (event->revents & POLLHUP) {
      HT_DEBUGF("Received POLLHUP on descriptor %d (%s:%d)", m_sd,
		inet_ntoa(m_addr.sin_addr), ntohs(m_addr.sin_port));
      handle_disconnect();
      return true;
    }

    HT_ASSERT((event->revents & POLLNVAL) == 0);

  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    handle_disconnect();
    return true;
  }

  return false;
}

#if defined(__linux__)

bool
IOHandlerData::handle_event(struct epoll_event *event, clock_t arrival_clocks, time_t arrival_time) {
  int error = 0;
  bool eof = false;

  //DisplayEvent(event);

  try {
    if (event->events & EPOLLOUT) {
      if (handle_write_readiness()) {
        handle_disconnect();
        return true;
      }
    }

    if (event->events & EPOLLIN) {
      size_t nread;
      while (true) {
        if (!m_got_header) {
          nread = et_socket_read(m_sd, m_message_header_ptr,
                                 m_message_header_remaining, &error, &eof);
          if (nread == (size_t)-1) {
            if (errno != ECONNREFUSED) {
              HT_ERRORF("socket read(%d, len=%d) failure : %s", m_sd,
                        (int)m_message_header_remaining, strerror(errno));
              error = Error::OK;
            }
            else
              error = Error::COMM_CONNECT_ERROR;

            handle_disconnect(error);
            return true;
          }
          else if (nread < m_message_header_remaining) {
            m_message_header_remaining -= nread;
            m_message_header_ptr += nread;
            if (error == EAGAIN)
              break;
            error = 0;
          }
          else {
            m_message_header_ptr += nread;
            handle_message_header(arrival_clocks, arrival_time);
          }

          if (eof)
            break;
        }
        else { // got header
          nread = et_socket_read(m_sd, m_message_ptr, m_message_remaining,
                                 &error, &eof);
          if (nread == (size_t)-1) {
            HT_ERRORF("socket read(%d, len=%d) failure : %s", m_sd,
                      (int)m_message_header_remaining, strerror(errno));
            handle_disconnect();
            return true;
          }
          else if (nread < m_message_remaining) {
            m_message_ptr += nread;
            m_message_remaining -= nread;
            if (error == EAGAIN)
              break;
            error = 0;
          }
          else
            handle_message_body();

          if (eof)
            break;
        }
      }
    }

    if (ReactorFactory::ms_epollet) {
      if (event->events & POLLRDHUP) {
        HT_DEBUGF("Received POLLRDHUP on descriptor %d (%s:%d)", m_sd,
                  inet_ntoa(m_addr.sin_addr), ntohs(m_addr.sin_port));
        handle_disconnect();
        return true;
      }
    }
    else {
      if (eof) {
        HT_DEBUGF("Received EOF on descriptor %d (%s:%d)", m_sd,
                  inet_ntoa(m_addr.sin_addr), ntohs(m_addr.sin_port));
        handle_disconnect();
        return true;
      }
    }

    if (event->events & EPOLLERR) {
      HT_ERRORF("Received EPOLLERR on descriptor %d (%s:%d)", m_sd,
                inet_ntoa(m_addr.sin_addr), ntohs(m_addr.sin_port));
      handle_disconnect();
      return true;
    }

    if (event->events & EPOLLHUP) {
      HT_DEBUGF("Received EPOLLHUP on descriptor %d (%s:%d)", m_sd,
                inet_ntoa(m_addr.sin_addr), ntohs(m_addr.sin_port));
      handle_disconnect();
      return true;
    }
  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    handle_disconnect();
    return true;
  }

  return false;
}

#elif defined(__sun__)

bool IOHandlerData::handle_event(port_event_t *event, clock_t arrival_clocks, time_t arrival_time) {
  int error = 0;
  bool eof = false;

  //display_event(event);

  try {

    if (event->portev_events & POLLOUT) {
      if (handle_write_readiness()) {
	HT_INFO("handle_disconnect() write readiness");
        handle_disconnect();
        return true;
      }
    }

    if (event->portev_events & POLLIN) {
      size_t nread;
      while (true) {
        if (!m_got_header) {
          nread = et_socket_read(m_sd, m_message_header_ptr,
                                 m_message_header_remaining, &error, &eof);
          if (nread == (size_t)-1) {
            if (errno != ECONNREFUSED) {
              HT_ERRORF("socket read(%d, len=%d) failure : %s", m_sd,
                        (int)m_message_header_remaining, strerror(errno));
              error = Error::OK;
            }
            else
              error = Error::COMM_CONNECT_ERROR;

            handle_disconnect(error);
            return true;
          }
          else if (nread < m_message_header_remaining) {
            m_message_header_remaining -= nread;
            m_message_header_ptr += nread;
            if (error == EAGAIN)
              break;
            error = 0;
          }
          else {
            m_message_header_ptr += nread;
            handle_message_header(arrival_clocks, arrival_time);
          }

          if (eof)
            break;
        }
        else { // got header
          nread = et_socket_read(m_sd, m_message_ptr, m_message_remaining,
                                 &error, &eof);
          if (nread == (size_t)-1) {
            HT_ERRORF("socket read(%d, len=%d) failure : %s", m_sd,
                      (int)m_message_header_remaining, strerror(errno));
            handle_disconnect();
            return true;
          }
          else if (nread < m_message_remaining) {
            m_message_ptr += nread;
            m_message_remaining -= nread;
            if (error == EAGAIN)
              break;
            error = 0;
          }
          else
            handle_message_body();

          if (eof)
            break;
        }
      }
    }

    if (eof) {
      HT_DEBUGF("Received EOF on descriptor %d (%s:%d)", m_sd,
		inet_ntoa(m_addr.sin_addr), ntohs(m_addr.sin_port));
      handle_disconnect();
      return true;
    }


    if (event->portev_events & POLLERR) {
      HT_ERRORF("Received POLLERR on descriptor %d (%s:%d)", m_sd,
                inet_ntoa(m_addr.sin_addr), ntohs(m_addr.sin_port));
      handle_disconnect();
      return true;
    }

    if (event->portev_events & POLLHUP) {
      HT_DEBUGF("Received POLLHUP on descriptor %d (%s:%d)", m_sd,
                inet_ntoa(m_addr.sin_addr), ntohs(m_addr.sin_port));
      handle_disconnect();
      return true;
    }

    if (event->portev_events & POLLREMOVE) {
      HT_DEBUGF("Received POLLREMOVE on descriptor %d (%s:%d)", m_sd,
                inet_ntoa(m_addr.sin_addr), ntohs(m_addr.sin_port));
      handle_disconnect();
      return true;
    }
    
  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    handle_disconnect();
    return true;
  }

  return false;
}

#elif defined(__APPLE__) || defined(__FreeBSD__)

/**
 *
 */
bool IOHandlerData::handle_event(struct kevent *event, clock_t arrival_clocks, time_t arrival_time) {

  //DisplayEvent(event);

  try {
    assert(m_sd == (int)event->ident);

    if (event->flags & EV_EOF) {
      handle_disconnect();
      return true;
    }

    if (event->filter == EVFILT_WRITE) {
      if (handle_write_readiness()) {
        handle_disconnect();
        return true;
      }
    }

    if (event->filter == EVFILT_READ) {
      size_t available = (size_t)event->data;
      size_t nread;
      while (available > 0) {
        if (!m_got_header) {
          if (m_message_header_remaining <= available) {
            nread = FileUtils::read(m_sd, m_message_header_ptr,
                                    m_message_header_remaining);
            if (nread == (size_t)-1) {
              HT_ERRORF("FileUtils::read(%d, len=%d) failure : %s", m_sd,
                        (int)m_message_header_remaining, strerror(errno));
              handle_disconnect();
              return true;
            }
            assert(nread == m_message_header_remaining);
            available -= nread;
            m_message_header_ptr += nread;
            handle_message_header(arrival_clocks, arrival_time);
          }
          else {
            nread = FileUtils::read(m_sd, m_message_header_ptr, available);
            if (nread == (size_t)-1) {
              HT_ERRORF("FileUtils::read(%d, len=%d) failure : %s", m_sd,
                        (int)available, strerror(errno));
              handle_disconnect();
              return true;
            }
            assert(nread == available);
            m_message_header_remaining -= nread;
            m_message_header_ptr += nread;
            return false;
          }
        }
        if (m_got_header) {
          if (m_message_remaining <= available) {
            nread = FileUtils::read(m_sd, m_message_ptr, m_message_remaining);
            if (nread == (size_t)-1) {
              HT_ERRORF("FileUtils::read(%d, len=%d) failure : %s", m_sd,
                        (int)m_message_remaining, strerror(errno));
              handle_disconnect();
              return true;
            }
            assert(nread == m_message_remaining);
            available -= nread;
            handle_message_body();
          }
          else {
            nread = FileUtils::read(m_sd, m_message_ptr, available);
            if (nread == (size_t)-1) {
              HT_ERRORF("FileUtils::read(%d, len=%d) failure : %s", m_sd,
                        (int)available, strerror(errno));
              handle_disconnect();
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
  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    handle_disconnect();
    return true;
  }

  return false;
}
#else
  ImplementMe;
#endif


void IOHandlerData::handle_message_header(clock_t arrival_clocks, time_t arrival_time) {
  size_t header_len = (size_t)m_message_header[1];

  // check to see if there is any variable length header
  // after the fixed length portion that needs to be read
  if (header_len > (size_t)(m_message_header_ptr - m_message_header)) {
    m_message_header_remaining = header_len - (size_t)(m_message_header_ptr
                                                       - m_message_header);
    return;
  }

  m_event->load_header(m_sd, m_message_header, header_len);
  m_event->arrival_clocks = arrival_clocks;
  m_event->arrival_time = arrival_time;

  m_message = new uint8_t [m_event->header.total_len - header_len];
  m_message_ptr = m_message;
  m_message_remaining = m_event->header.total_len - header_len;
  m_message_header_remaining = 0;
  m_got_header = true;

}


void IOHandlerData::handle_message_body() {
  DispatchHandler *dh = 0;

  if ((m_event->header.flags & CommHeader::FLAGS_BIT_REQUEST) == 0 &&
      (m_event->header.id == 0
      || (dh = m_reactor_ptr->remove_request(m_event->header.id)) == 0)) {
    if ((m_event->header.flags & CommHeader::FLAGS_BIT_IGNORE_RESPONSE) == 0) {
      HT_WARNF("Received response for non-pending event (id=%d,version"
               "=%d,total_len=%d)", m_event->header.id, m_event->header.version,
               m_event->header.total_len);
    }
    delete [] m_message;
    delete m_event;
  }
  else {
    m_event->payload = m_message;
    m_event->payload_len = m_event->header.total_len
                           - m_event->header.header_len;
    //HT_INFOF("Just received messaage of size %d", m_event->header.total_len);
    deliver_event( m_event, dh );
  }

  reset_incoming_message_state();
}


void IOHandlerData::handle_disconnect(int error) {
  m_reactor_ptr->cancel_requests(this);
  deliver_event(new Event(Event::DISCONNECT, m_addr, error));
}


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
    if (setsockopt(m_sd, SOL_SOCKET, SO_SNDBUF, (char *)&bufsize,
        sizeof(bufsize)) < 0) {
      HT_ERRORF("setsockopt(SO_SNDBUF) failed - %s", strerror(errno));
    }
    if (setsockopt(m_sd, SOL_SOCKET, SO_RCVBUF, (char *)&bufsize,
        sizeof(bufsize)) < 0) {
      HT_ERRORF("setsockopt(SO_RCVBUF) failed - %s", strerror(errno));
    }

    if (getsockname(m_sd, (struct sockaddr *)&m_local_addr, &name_len) < 0) {
      HT_ERRORF("getsockname(%d) failed - %s", m_sd, strerror(errno));
      return true;
    }
    //HT_INFO("Connection established.");
    m_connected = true;
    deliver_event(new Event(Event::CONNECTION_ESTABLISHED, m_addr,
                            Error::OK));
  }

  {
    ScopedLock lock(m_mutex);
    //HT_INFO("about to flush send queue");
    if (flush_send_queue() != Error::OK) {
      HT_DEBUG("error flushing send queue");
      return true;
    }
    //HT_INFO("about to remove poll interest");
    if (m_send_queue.empty())
      remove_poll_interest(Reactor::WRITE_READY);
  }
  return false;
}


int
IOHandlerData::send_message(CommBufPtr &cbp, uint32_t timeout_ms,
                            DispatchHandler *disp_handler) {
  ScopedLock lock(m_mutex);
  int error;
  bool initially_empty = m_send_queue.empty() ? true : false;

  /**
  if (!m_connected)
    return Error::COMM_NOT_CONNECTED;
  **/

  // If request, Add message ID to request cache
  if (cbp->header.id != 0 && disp_handler != 0
      && cbp->header.flags & CommHeader::FLAGS_BIT_REQUEST) {
    boost::xtime expire_time;
    boost::xtime_get(&expire_time, boost::TIME_UTC);
    xtime_add_millis(expire_time, timeout_ms);
    m_reactor_ptr->add_request(cbp->header.id, this, disp_handler, expire_time);
  }

  //HT_INFOF("About to send message of size %d", cbp->header.total_len);

  m_send_queue.push_back(cbp);

  if ((error = flush_send_queue()) != Error::OK)
    HT_WARNF("Problem flushing send queue - %s", Error::get_text(error));

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
      HT_WARNF("FileUtils::writev(%d, len=%d) failed : %s", m_sd, (int)towrite,
               strerror(errno));
      return Error::COMM_BROKEN_CONNECTION;
    }
    else if (nwritten < towrite) {
      if (nwritten == 0) {
        if (error == EAGAIN)
          break;
        if (error) {
          HT_WARNF("FileUtils::writev(%d, len=%d) failed : %s", m_sd,
                   (int)towrite, strerror(error));
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

    // buffer written successfully, now remove from queue (destroys buffer)
    m_send_queue.pop_front();
  }

  return Error::OK;
}

#elif defined(__APPLE__) || defined (__sun__) || defined(__FreeBSD__)

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
      HT_WARNF("FileUtils::writev(%d, len=%d) failed : %s", m_sd, (int)towrite,
               strerror(errno));
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

    // buffer written successfully, now remove from queue (destroys buffer)
    m_send_queue.pop_front();
  }

  return Error::OK;
}

#else
  ImplementMe;
#endif
