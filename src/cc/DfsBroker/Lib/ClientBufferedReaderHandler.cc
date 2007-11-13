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

namespace {
  const uint32_t DEFAULT_READ_SIZE = 8192;
}

/**
 *
 */
ClientBufferedReaderHandler::ClientBufferedReaderHandler(DfsBroker::Client *client, uint32_t fd, uint32_t bufSize) : m_client(client), m_fd(fd), m_eof(false), m_error(Error::OK) {

  m_max_outstanding = (bufSize + (DEFAULT_READ_SIZE-1)) / DEFAULT_READ_SIZE;

  if (m_max_outstanding < 2)
    m_max_outstanding = 2;

  for (m_outstanding=0; m_outstanding<m_max_outstanding; m_outstanding++) {
    if ((m_error = m_client->read(m_fd, DEFAULT_READ_SIZE, this)) != Error::OK) {
      m_eof = true;
      break;
    }
  }

  m_ptr = m_end_ptr = 0;
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
      LOG_VA_ERROR("DfsBroker 'read' error (amount=%d, fd=%d) : %s",
		   DEFAULT_READ_SIZE, m_fd, Protocol::string_format_message(eventPtr).c_str());
      m_eof = true;
      return;
    }
    m_queue.push(eventPtr);
    DfsBroker::Protocol::ResponseHeaderReadT *readHeader = (DfsBroker::Protocol::ResponseHeaderReadT *)eventPtr->message;
    if (readHeader->amount < DEFAULT_READ_SIZE)
      m_eof = true;
  }
  else if (eventPtr->type == Event::ERROR) {
    LOG_VA_ERROR("%s", eventPtr->toString().c_str());    
    m_error = eventPtr->error;
    m_eof = true;
  }
  else {
    LOG_VA_ERROR("%s", eventPtr->toString().c_str());
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

    if (m_error != Error::OK || m_queue.empty())
      return m_error;

    if (m_ptr == 0) {
      EventPtr &eventPtr = m_queue.front();
      DfsBroker::Protocol::ResponseHeaderReadT *readHeader = (DfsBroker::Protocol::ResponseHeaderReadT *)eventPtr->message;
      m_ptr = (uint8_t *)&readHeader[1];
      m_end_ptr = m_ptr + readHeader->amount;
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
      return Error::OK;
    }

    memcpy(ptr, m_ptr, available);
    ptr += available;
    nleft -= available;
    m_queue.pop();
    m_ptr = 0;
    read_ahead();
  }
}



/**
 *
 */
void ClientBufferedReaderHandler::read_ahead() {
  uint32_t n = m_max_outstanding - (m_outstanding + m_queue.size());

  assert(m_max_outstanding >= (m_outstanding + m_queue.size()));
  
  if (m_eof)
    return;

  for (uint32_t i=0; i<n; i++) {
    if ((m_error = m_client->read(m_fd, DEFAULT_READ_SIZE, this)) != Error::OK) {
      m_eof = true;
      break;
    }
    m_outstanding++;
  }
}


