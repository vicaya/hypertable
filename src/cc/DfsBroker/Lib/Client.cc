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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#include <cassert>

#include "AsyncComm/Comm.h"
#include "AsyncComm/HeaderBuilder.h"
#include "AsyncComm/Serialization.h"

#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"

#include "Client.h"

using namespace Hypertable;
using namespace Hypertable::DfsBroker;


Client::Client(ConnectionManagerPtr &conn_manager_ptr, struct sockaddr_in &addr, time_t timeout) : m_conn_manager_ptr(conn_manager_ptr), m_addr(addr), m_timeout(timeout) {
  m_comm = conn_manager_ptr->get_comm();
  conn_manager_ptr->add(m_addr, m_timeout, "DFS Broker");
}



Client::Client(ConnectionManagerPtr &conn_manager_ptr, PropertiesPtr &props_ptr) : m_conn_manager_ptr(conn_manager_ptr) {
  const char *host;
  uint16_t port;

  m_comm = conn_manager_ptr->get_comm();

  {
    if ((port = (uint16_t)props_ptr->get_int("DfsBroker.Port", 0)) == 0) {
      HT_ERROR(".Port property not specified.");
      exit(1);
    }

    if ((host = props_ptr->get("DfsBroker.Host", (const char *)0)) == 0) {
      HT_ERROR(".Host property not specified.");
      exit(1);
    }

    if ((m_timeout = props_ptr->get_int("DfsBroker.Timeout", 0)) == 0)
      m_timeout = props_ptr->get_int("Hypertable.Request.Timeout", 60);

    InetAddr::initialize(&m_addr, host, port);
  }

  conn_manager_ptr->add(m_addr, 10, "DFS Broker");
  
}

Client::Client(Comm *comm, struct sockaddr_in &addr, time_t timeout) : m_comm(comm), m_conn_manager_ptr(0), m_addr(addr), m_timeout(timeout) {
}



int Client::open(std::string &name, DispatchHandler *handler) {
  CommBufPtr cbufPtr( m_protocol.create_open_request(name, 0) );
  return send_message(cbufPtr, handler);
}



int Client::open(std::string &name, int32_t *fdp) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  *fdp = -1;
  CommBufPtr cbufPtr( m_protocol.create_open_request(name, 0) );
  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      HT_ERRORF("Dfs 'open' error, name=%s : %s", name.c_str(), m_protocol.string_format_message(eventPtr).c_str());
    }
    error = decode_response_open(eventPtr, fdp);
  }
  return error;
}


int Client::open_buffered(std::string &name, uint32_t buf_size, uint32_t outstanding, int32_t *fdp, uint64_t start_offset, uint64_t end_offset) {
  int error;

  if ((error = open(name, fdp)) != Error::OK)
    return error;

  {
    boost::mutex::scoped_lock lock(m_mutex);
    assert(m_buffered_reader_map.find(*fdp) == m_buffered_reader_map.end());
    m_buffered_reader_map[*fdp] = new ClientBufferedReaderHandler(this, *fdp, buf_size, outstanding, start_offset, end_offset);
  }

  return Error::OK;
  
}


int Client::create(std::string &name, bool overwrite, int32_t bufferSize,
		   int32_t replication, int64_t blockSize, DispatchHandler *handler) {
  CommBufPtr cbufPtr( m_protocol.create_create_request(name, overwrite, bufferSize, replication, blockSize) );
  return send_message(cbufPtr, handler);
}


int Client::create(std::string &name, bool overwrite, int32_t bufferSize,
		   int32_t replication, int64_t blockSize, int32_t *fdp) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  *fdp = -1;
  CommBufPtr cbufPtr( m_protocol.create_create_request(name, overwrite, bufferSize, replication, blockSize) );
  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      HT_ERRORF("Dfs 'create' error, name=%s : %s", name.c_str(), m_protocol.string_format_message(eventPtr).c_str());
    }
    error = decode_response_create(eventPtr, fdp);
  }
  return error;
}



int Client::close(int32_t fd, DispatchHandler *handler) {
  ClientBufferedReaderHandler *brHandler = 0;
  CommBufPtr cbufPtr( m_protocol.create_close_request(fd) );

  {
    boost::mutex::scoped_lock lock(m_mutex);
    BufferedReaderMapT::iterator iter = m_buffered_reader_map.find(fd);
    if (iter != m_buffered_reader_map.end()) {
      brHandler = (*iter).second;
      m_buffered_reader_map.erase(iter);
    }
  }
  delete brHandler;

  return send_message(cbufPtr, handler);
}



int Client::close(int32_t fd) {
  ClientBufferedReaderHandler *brHandler = 0;
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( m_protocol.create_close_request(fd) );

  {
    boost::mutex::scoped_lock lock(m_mutex);
    BufferedReaderMapT::iterator iter = m_buffered_reader_map.find(fd);
    if (iter != m_buffered_reader_map.end()) {
      brHandler = (*iter).second;
      m_buffered_reader_map.erase(iter);
    }
  }
  delete brHandler;

  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      HT_ERRORF("Dfs 'close' error, fd=%d : %s", fd, m_protocol.string_format_message(eventPtr).c_str());
    }
    error = decode_response(eventPtr);
  }

  return error;
}



int Client::read(int32_t fd, uint32_t amount, DispatchHandler *handler) {
  CommBufPtr cbufPtr( m_protocol.create_read_request(fd, amount) );
  return send_message(cbufPtr, handler);
}



int Client::read(int32_t fd, uint32_t amount, uint8_t *dst, uint32_t *nreadp) {
  ClientBufferedReaderHandler *brHandler = 0;
  
  {
    boost::mutex::scoped_lock lock(m_mutex);
    BufferedReaderMapT::iterator iter = m_buffered_reader_map.find(fd);
    if (iter != m_buffered_reader_map.end())
      brHandler = (*iter).second;
  }

  if (brHandler)
    return brHandler->read(dst, amount, nreadp);

  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( m_protocol.create_read_request(fd, amount) );
  int error = send_message(cbufPtr, &syncHandler);
  *nreadp = 0;
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      HT_ERRORF("Dfs 'read' error (amount=%d, fd=%d) : %s",
		   amount, fd, m_protocol.string_format_message(eventPtr).c_str());
    }
    error = decode_response_read(eventPtr, dst, nreadp);
  }
  return error;
}



int Client::append(int32_t fd, const void *buf, uint32_t amount, DispatchHandler *handler) {
  CommBufPtr cbufPtr( m_protocol.create_append_request(fd, buf, amount) );
  return send_message(cbufPtr, handler);
}



int Client::append(int32_t fd, const void *buf, uint32_t amount) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( m_protocol.create_append_request(fd, buf, amount) );
  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      HT_ERRORF("Dfs 'append' error, fd=%d, amount=%d : %s", fd, amount, m_protocol.string_format_message(eventPtr).c_str());
    }
    uint64_t offset;
    uint32_t actual_amount;
    error = decode_response_append(eventPtr, &offset, &actual_amount);
    if (amount != actual_amount) {
      HT_ERRORF("Short DFS file append fd=%d : tried to append %d but only got %d", fd, amount, actual_amount);
      error = Error::DFSBROKER_IO_ERROR;
    }
  }
  return error;
}


int Client::seek(int32_t fd, uint64_t offset, DispatchHandler *handler) {
  CommBufPtr cbufPtr( m_protocol.create_seek_request(fd, offset) );
  return send_message(cbufPtr, handler);
}


int Client::seek(int32_t fd, uint64_t offset) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( m_protocol.create_seek_request(fd, offset) );
  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      HT_ERRORF("Dfs 'seek' error, fd=%d, offset=%lld : %s", fd, offset, m_protocol.string_format_message(eventPtr).c_str());
      error = decode_response(eventPtr);
    }
  }
  return error;
}


int Client::remove(std::string &name, DispatchHandler *handler) {
  CommBufPtr cbufPtr( m_protocol.create_remove_request(name) );
  return send_message(cbufPtr, handler);
}


int Client::remove(std::string &name) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( m_protocol.create_remove_request(name) );
  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      HT_ERRORF("Dfs 'remove' error, name=%s : %s", name.c_str(), m_protocol.string_format_message(eventPtr).c_str());
      error = decode_response(eventPtr);
    }
  }
  return error;
}



int Client::shutdown(uint16_t flags, DispatchHandler *handler) {
  CommBufPtr cbufPtr( m_protocol.create_shutdown_request(flags) );
  return send_message(cbufPtr, handler);
}


int Client::status() {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( m_protocol.create_status_request() );
  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      HT_ERRORF(" 'status' error : %s", m_protocol.string_format_message(eventPtr).c_str());
      error = decode_response(eventPtr);
    }
  }
  return error;
}


int Client::length(std::string &name, DispatchHandler *handler) {
  CommBufPtr cbufPtr( m_protocol.create_length_request(name) );
  return send_message(cbufPtr, handler);
}



int Client::length(std::string &name, int64_t *lenp) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( m_protocol.create_length_request(name) );
  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      HT_ERRORF("Dfs 'length' error, name=%s : %s", name.c_str(), m_protocol.string_format_message(eventPtr).c_str());
    }
    error = decode_response_length(eventPtr, lenp);
  }
  return error;
}



int Client::pread(int32_t fd, uint64_t offset, uint32_t amount, DispatchHandler *handler) {
  CommBufPtr cbufPtr( m_protocol.create_position_read_request(fd, offset, amount) );
  return send_message(cbufPtr, handler);
}



int Client::pread(int32_t fd, uint64_t offset, uint32_t amount, uint8_t *dst, uint32_t *nreadp) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( m_protocol.create_position_read_request(fd, offset, amount) );
  int error = send_message(cbufPtr, &syncHandler);
  *nreadp = 0;
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      HT_ERRORF("Dfs 'pread' error (offset=%lld, amount=%d, fd=%d) : %s",
		   offset, amount, fd, m_protocol.string_format_message(eventPtr).c_str());
    }
    error = decode_response_pread(eventPtr, dst, nreadp);
  }
  return error;
}


int Client::mkdirs(std::string &name, DispatchHandler *handler) {
  CommBufPtr cbufPtr( m_protocol.create_mkdirs_request(name) );
  return send_message(cbufPtr, handler);
}


int Client::mkdirs(std::string &name) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( m_protocol.create_mkdirs_request(name) );
  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      HT_ERRORF("Dfs 'mkdirs' error, name=%s : %s", name.c_str(), m_protocol.string_format_message(eventPtr).c_str());
      error = decode_response(eventPtr);
    }
  }
  return error;
}


int Client::flush(int32_t fd, DispatchHandler *handler) {
  CommBufPtr cbufPtr( m_protocol.create_flush_request(fd) );
  return send_message(cbufPtr, handler);
}


int Client::flush(int32_t fd) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( m_protocol.create_flush_request(fd) );
  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      HT_ERRORF("Dfs 'flush' error, fd=%d : %s", fd, m_protocol.string_format_message(eventPtr).c_str());
      error = decode_response(eventPtr);
    }
  }
  return error;
}


int Client::rmdir(std::string &name, DispatchHandler *handler) {
  CommBufPtr cbufPtr( m_protocol.create_rmdir_request(name) );
  return send_message(cbufPtr, handler);
}


int Client::rmdir(std::string &name) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( m_protocol.create_rmdir_request(name) );
  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      HT_ERRORF("Dfs 'rmdir' error, name=%s : %s", name.c_str(), m_protocol.string_format_message(eventPtr).c_str());
      error = decode_response(eventPtr);
    }
  }
  return error;
}



/**
 *
 */
int Client::readdir(std::string &name, DispatchHandler *handler) {
  CommBufPtr cbufPtr( m_protocol.create_readdir_request(name) );
  return send_message(cbufPtr, handler);
}



/**
 *
 */
int Client::readdir(std::string &name, std::vector<std::string> &listing) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( m_protocol.create_readdir_request(name) );
  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      HT_ERRORF("Dfs 'readdir' error, name=%s : %s", name.c_str(), m_protocol.string_format_message(eventPtr).c_str());
    }
    error = decode_response_readdir(eventPtr, listing);
  }
  return error;
}



int Client::send_message(CommBufPtr &cbufPtr, DispatchHandler *handler) {
  int error;

  if ((error = m_comm->send_request(m_addr, m_timeout, cbufPtr, handler)) != Error::OK) {
    HT_WARNF("Comm::send_request to %s:%d failed - %s",
		inet_ntoa(m_addr.sin_addr), ntohs(m_addr.sin_port), Error::get_text(error));
  }
  return error;
}
