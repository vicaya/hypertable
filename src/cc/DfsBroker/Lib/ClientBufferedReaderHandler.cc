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

#include "Common/Error.h"

#include "ClientBufferedReaderHandler.h"
#include "Client.h"

using namespace Hypertable;

/**
 *
 */
ClientBufferedReaderHandler::ClientBufferedReaderHandler(DfsBroker::Client *client, uint32_t fd, uint32_t buf_size, uint32_t outstanding, uint64_t start_offset, uint64_t end_offset) : m_client(client), m_fd(fd), m_read_size(buf_size), m_eof(false), m_error(Error::OK) {

  m_max_outstanding = outstanding;

  m_end_offset = end_offset;
  m_outstanding_offset = start_offset;
  m_actual_offset = start_offset;

  /**
   * Seek to initial offset
   */
  if (start_offset > 0) {
    if ((m_error = m_client->seek(m_fd, start_offset)) != Error::OK) {
      // TODO: throw exception
      HT_ERRORF("Problem seeking to position '%lld' in file", start_offset);
      m_eof = true;
      return;
    }
  }

  {
    boost::mutex::scoped_lock lock(m_mutex);
    uint32_t toread;

    for (m_outstanding=0; m_outstanding<m_max_outstanding; m_outstanding++) {
      if (m_end_offset && (m_end_offset-m_outstanding_offset) < m_read_size) {
	if ((toread = (uint32_t)(m_end_offset - m_outstanding_offset)) == 0)
	  break;
      }
      else
	toread = m_read_size;
      if ((m_error = m_client->read(m_fd, toread, this)) != Error::OK) {
	m_eof = true;
	break;
      }
      m_outstanding_offset -= toread;
    }
    m_ptr = m_end_ptr = 0;
  }
}



ClientBufferedReaderHandler::~ClientBufferedReaderHandler() {
  boost::mutex::scoped_lock lock(m_mutex);
  m_eof = true;
  while (m_outstanding > 0)
    m_cond.wait(lock);
}



/**
 *
 */
void ClientBufferedReaderHandler::handle(EventPtr &eventPtr) {
  boost::mutex::scoped_lock lock(m_mutex);

  m_outstanding--;

  if (eventPtr->type == Event::MESSAGE) {
    if ((m_error = (int)Protocol::response_code(eventPtr)) != Error::OK) {
      HT_ERRORF("DfsBroker 'read' error (amount=%d, fd=%d) : %s",
		   m_read_size, m_fd, Protocol::string_format_message(eventPtr).c_str());
      m_eof = true;
      return;
    }
    m_queue.push(eventPtr);

    {
      uint64_t offset;
      uint32_t amount;
      Filesystem::decode_response_read_header(eventPtr, &offset, &amount, 0);
      m_actual_offset += amount;
      if (amount < m_read_size) {
	//HT_ERRORF("short read %ld < %ld (actual=%ld, end=%ld)", amount, (int32_t)m_read_size, m_actual_offset, m_end_offset);
	m_eof = true;
      }
    }
  }
  else if (eventPtr->type == Event::ERROR) {
    HT_ERRORF("%s", eventPtr->toString().c_str());    
    m_error = eventPtr->error;
    m_eof = true;
  }
  else {
    HT_ERRORF("%s", eventPtr->toString().c_str());
    assert(!"Not supposed to receive this type of event!");
  }

  m_cond.notify_all();
}



/**
 *
 */
int ClientBufferedReaderHandler::read(uint8_t *buf, uint32_t len, uint32_t *nreadp) {
  boost::mutex::scoped_lock lock(m_mutex);
  uint8_t *ptr = buf;
  uint32_t nleft = len;
  uint32_t available;

  while (true) {

    while (m_queue.empty() && !m_eof)
      m_cond.wait(lock);

    if (m_error != Error::OK || m_queue.empty()) {
      HT_ERRORF("return 1 error=%d", m_error);
      return m_error;
    }

    if (m_ptr == 0) {
      uint64_t offset;
      uint32_t amount;
      EventPtr &eventPtr = m_queue.front();
      Filesystem::decode_response_read_header(eventPtr, &offset, &amount, &m_ptr);
      m_end_ptr = m_ptr + amount;
    }

    available = m_end_ptr-m_ptr;

    if (available >= nleft) {
      memcpy(ptr, m_ptr, nleft);
      *nreadp = len;
      m_ptr += nleft;
      if ((m_end_ptr-m_ptr) == 0) {
	m_queue.pop();
	m_ptr = 0;
	read_ahead();
      }
      break;
    }
    else if (available == 0) {
      if (m_eof && m_queue.size() == 1) {
	m_queue.pop();
	m_ptr = m_end_ptr = 0;
	*nreadp = len - nleft;
	break;
      }
    }

    memcpy(ptr, m_ptr, available);
    ptr += available;
    nleft -= available;
    m_queue.pop();
    m_ptr = 0;
    read_ahead();
  }
  return Error::OK;
}



/**
 *
 */
void ClientBufferedReaderHandler::read_ahead() {
  uint32_t n = m_max_outstanding - (m_outstanding + m_queue.size());
  uint32_t toread;

  assert(m_max_outstanding >= (m_outstanding + m_queue.size()));
  
  if (m_eof)
    return;

  for (uint32_t i=0; i<n; i++) {
    if (m_end_offset && (m_end_offset-m_outstanding_offset) < m_read_size) {
      if ((toread = (uint32_t)(m_end_offset - m_outstanding_offset)) == 0)
	break;
    }
    else
      toread = m_read_size;
    if ((m_error = m_client->read(m_fd, toread, this)) != Error::OK) {
      m_eof = true;
      break;
    }
    m_outstanding++;
    m_outstanding_offset -= toread;
  }
}


