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

#include "AsyncComm/Comm.h"
#include "AsyncComm/HeaderBuilder.h"
#include "AsyncComm/Serialization.h"

#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"

#include "Client.h"

using namespace Hypertable;
using namespace Hypertable::DfsBroker;
typedef boost::mutex::scoped_lock ScopedLock;

Client::Client(ConnectionManagerPtr &conn_manager_ptr,
               struct sockaddr_in &addr, time_t timeout) :
               m_conn_manager_ptr(conn_manager_ptr), m_addr(addr),
               m_timeout(timeout) {
  m_comm = conn_manager_ptr->get_comm();
  conn_manager_ptr->add(m_addr, m_timeout, "DFS Broker");
}


Client::Client(ConnectionManagerPtr &conn_manager_ptr,
               PropertiesPtr &props_ptr) :
               m_conn_manager_ptr(conn_manager_ptr) {
  const char *host;
  uint16_t port;

  m_comm = conn_manager_ptr->get_comm();
  {
    if ((port = (uint16_t)props_ptr->get_int("DfsBroker.Port", 0)) == 0)
      throw Exception(Error::DFSBROKER_INVALID_CONFIG,
                      "DfsBroker.Port property not specified.");

    if ((host = props_ptr->get("DfsBroker.Host", NULL)) == 0)
      throw Exception(Error::DFSBROKER_INVALID_CONFIG,
                      "DfsBroker.Host property not specified.");

    if ((m_timeout = props_ptr->get_int("DfsBroker.Timeout", 0)) == 0)
      m_timeout = props_ptr->get_int("Hypertable.Request.Timeout", 60);

    InetAddr::initialize(&m_addr, host, port);
  }

  conn_manager_ptr->add(m_addr, 10, "DFS Broker");
  
}

Client::Client(Comm *comm, struct sockaddr_in &addr, time_t timeout) :
               m_comm(comm), m_conn_manager_ptr(0), m_addr(addr),
               m_timeout(timeout) {
}


void
Client::open(const String &name, DispatchHandler *handler) {
  CommBufPtr cbufPtr(m_protocol.create_open_request(name, 0));

  try {
    send_message(cbufPtr, handler);
  }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error opening DFS file: %s",
                    name.c_str()), e);
  }
}


int
Client::open(const String &name) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr(m_protocol.create_open_request(name, 0));

  try {
    send_message(cbufPtr, &syncHandler);
    
    if (!syncHandler.wait_for_reply(eventPtr))
      throw Exception(Protocol::response_code(eventPtr.get()),
                      m_protocol.string_format_message(eventPtr).c_str());

    return decode_response_open(eventPtr);
  }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error opening DFS file: %s",
                    name.c_str()), e);
  }
}


int
Client::open_buffered(const String &name, uint32_t buf_size,
                      uint32_t outstanding, uint64_t start_offset,
                      uint64_t end_offset) {
  try {
    int fd = open(name);
    {
      ScopedLock lock(m_mutex);
      HT_EXPECT(m_buffered_reader_map.find(fd) == m_buffered_reader_map.end(),
                Error::FAILED_EXPECTATION);
      m_buffered_reader_map[fd] =
          new ClientBufferedReaderHandler(this, fd, buf_size, outstanding,
                                          start_offset, end_offset);
    }
    return fd;
  }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error opening buffered DFS file=%s "
				     "buf_size=%u outstanding=%u "
                                     "start_offset=%llu end_offset=%llu",
                                     name.c_str(), buf_size, outstanding,
				     (Llu)start_offset, (Llu)end_offset), e);
  }
}


void
Client::create(const String &name, bool overwrite, int32_t bufferSize,
               int32_t replication, int64_t blockSize,
               DispatchHandler *handler) {
  CommBufPtr cbufPtr(m_protocol.create_create_request(name, overwrite,
                     bufferSize, replication, blockSize));
  try {
    send_message(cbufPtr, handler);
  }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error creating DFS file: %s:",
                    name.c_str()), e);
  }
}


int
Client::create(const String &name, bool overwrite, int32_t bufferSize,
               int32_t replication, int64_t blockSize) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr(m_protocol.create_create_request(name, overwrite,
                     bufferSize, replication, blockSize));
  try {
    send_message(cbufPtr, &syncHandler);

    if (!syncHandler.wait_for_reply(eventPtr))
      throw Exception(Protocol::response_code(eventPtr.get()),
                      m_protocol.string_format_message(eventPtr).c_str());

    return decode_response_create(eventPtr);
  }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error creating DFS file: %s", 
                    name.c_str()), e);
  }
}


void
Client::close(int32_t fd, DispatchHandler *handler) {
  ClientBufferedReaderHandler *brHandler = 0;
  CommBufPtr cbufPtr(m_protocol.create_close_request(fd));
  {
    ScopedLock lock(m_mutex);
    BufferedReaderMapT::iterator iter = m_buffered_reader_map.find(fd);
    if (iter != m_buffered_reader_map.end()) {
      brHandler = (*iter).second;
      m_buffered_reader_map.erase(iter);
    }
  }
  delete brHandler;

  try {
    send_message(cbufPtr, handler);
  }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error closing DFS fd: %d", (int)fd), e);
  }
}


void
Client::close(int32_t fd) {
  ClientBufferedReaderHandler *brHandler = 0;
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr(m_protocol.create_close_request(fd));
  {
    ScopedLock lock(m_mutex);
    BufferedReaderMapT::iterator iter = m_buffered_reader_map.find(fd);

    if (iter != m_buffered_reader_map.end()) {
      brHandler = (*iter).second;
      m_buffered_reader_map.erase(iter);
    }
  }
  delete brHandler;

  try {
    send_message(cbufPtr, &syncHandler);

    if (!syncHandler.wait_for_reply(eventPtr))
      throw Exception(Protocol::response_code(eventPtr.get()),
                      m_protocol.string_format_message(eventPtr).c_str());
  }
  catch(Exception &e) {
    throw Exception(e.code(), format("Error closing DFS fd: %d", (int)fd), e);
  }
}


void
Client::read(int32_t fd, size_t len, DispatchHandler *handler) {
  CommBufPtr cbufPtr(m_protocol.create_read_request(fd, len));

  try {
    send_message(cbufPtr, handler);
  }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error sending read request for %u bytes "
                    "from DFS fd: %d", (unsigned)len, (int)fd), e);
  }
}


size_t
Client::read(int32_t fd, void *dst, size_t len) {
  ClientBufferedReaderHandler *brHandler = 0;
  {
    ScopedLock lock(m_mutex);
    BufferedReaderMapT::iterator iter = m_buffered_reader_map.find(fd);

    if (iter != m_buffered_reader_map.end())
      brHandler = (*iter).second;
  }
  try {
    if (brHandler)
      return brHandler->read(dst, len);
      
    DispatchHandlerSynchronizer syncHandler;
    EventPtr eventPtr;
    CommBufPtr cbufPtr(m_protocol.create_read_request(fd, len));
    send_message(cbufPtr, &syncHandler);

    if (!syncHandler.wait_for_reply(eventPtr))
      throw Exception(Protocol::response_code(eventPtr.get()),
                      m_protocol.string_format_message(eventPtr).c_str());

    return decode_response_read(eventPtr, dst, len);
  }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error reading %u bytes from DFS fd %d",
                    (unsigned)len, (int)fd), e);
  }
}


void
Client::append(int32_t fd, void *buf, size_t len, DispatchHandler *handler) {
  CommBufPtr cbufPtr(m_protocol.create_append_request(fd, buf, len));

  try { send_message(cbufPtr, handler); }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error appending %u bytes to DFS fd %d",
                    (unsigned)len, (int)fd), e);
  }
}


size_t
Client::append(int32_t fd, void *buf, size_t len) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr(m_protocol.create_append_request(fd, buf, len));

  try {
    send_message(cbufPtr, &syncHandler);

    if (!syncHandler.wait_for_reply(eventPtr))
      throw Exception(Protocol::response_code(eventPtr.get()),
                      m_protocol.string_format_message(eventPtr).c_str());
    uint64_t offset;
    size_t ret = decode_response_append(eventPtr, &offset);

    if (len != ret)
      throw Exception(Error::DFSBROKER_IO_ERROR, format("tried to append %u "
                      "bytes but got %u", (unsigned)len, (unsigned)ret));
    return ret;
  }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error appending %u bytes to DFS fd %d",
                    (unsigned)len, (int)fd), e);
  }
}


void
Client::seek(int32_t fd, uint64_t offset, DispatchHandler *handler) {
  CommBufPtr cbufPtr(m_protocol.create_seek_request(fd, offset));

  try { send_message(cbufPtr, handler); }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error seeking to %llu on DFS fd %d",
				     (Llu)offset, (int)fd), e);
  }
}


void
Client::seek(int32_t fd, uint64_t offset) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr(m_protocol.create_seek_request(fd, offset));

  try {
    send_message(cbufPtr, &syncHandler);

    if (!syncHandler.wait_for_reply(eventPtr))
      throw Exception(Protocol::response_code(eventPtr.get()),
                      m_protocol.string_format_message(eventPtr).c_str());
  }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error seeking to %llu on DFS fd %d",
				     (Llu)offset, (int)fd), e);
  }
}


void
Client::remove(const String &name, DispatchHandler *handler) {
  CommBufPtr cbufPtr(m_protocol.create_remove_request(name));

  try { send_message(cbufPtr, handler); }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error removing DFS file: %s", 
                    name.c_str()), e);
  }
}


void
Client::remove(const String &name, bool force) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr(m_protocol.create_remove_request(name));

  try {
    send_message(cbufPtr, &syncHandler);

    if (!syncHandler.wait_for_reply(eventPtr)) {
      int error = Protocol::response_code(eventPtr.get());

      if (!force || error != Error::DFSBROKER_FILE_NOT_FOUND)
        throw Exception(error,
                        m_protocol.string_format_message(eventPtr).c_str());
    }
  }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error removing DFS file: %s",
                    name.c_str()), e);
  }
}


void
Client::shutdown(uint16_t flags, DispatchHandler *handler) {
  CommBufPtr cbufPtr(m_protocol.create_shutdown_request(flags));

  try { send_message(cbufPtr, handler); }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error sending DFS shutdown (flags=%d)",
                    (int)flags), e);
  }
}


int
Client::status() {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr(m_protocol.create_status_request());

  try {
    send_message(cbufPtr, &syncHandler);

    if (!syncHandler.wait_for_reply(eventPtr))
      throw Exception(Protocol::response_code(eventPtr.get()),
                      m_protocol.string_format_message(eventPtr).c_str());

    return decode_response(eventPtr);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_ERROR_END;
    return e.code();
  }
}


void
Client::length(const String &name, DispatchHandler *handler) {
  CommBufPtr cbufPtr(m_protocol.create_length_request(name));
  
  try { send_message(cbufPtr, handler); }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error sending length request for DFS "
                    "file: %s", name.c_str()), e);
  }
}


int64_t
Client::length(const String &name) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr(m_protocol.create_length_request(name));

  try {
    send_message(cbufPtr, &syncHandler);

    if (!syncHandler.wait_for_reply(eventPtr))
      throw Exception(Protocol::response_code(eventPtr.get()),
                      m_protocol.string_format_message(eventPtr).c_str());

    return decode_response_length(eventPtr);
  }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error getting length of DFS file: %s",
                    name.c_str()), e);
  }
}


void
Client::pread(int32_t fd, size_t len, uint64_t offset,
              DispatchHandler *handler) {
  CommBufPtr cbufPtr(m_protocol.create_position_read_request(fd, offset, len));

  try { send_message(cbufPtr, handler); }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error sending pread request at byte %llu "
                                     "on DFS fd %d", (Llu)offset, (int)fd), e);
  }
}


size_t
Client::pread(int32_t fd, void *dst, size_t len, uint64_t offset) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr(m_protocol.create_position_read_request(fd, offset, len));

  try {
    send_message(cbufPtr, &syncHandler);
    
    if (!syncHandler.wait_for_reply(eventPtr))
      throw Exception(Protocol::response_code(eventPtr.get()),
                      m_protocol.string_format_message(eventPtr).c_str());

    return decode_response_pread(eventPtr, dst, len);
  }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error preading at byte %llu on DFS fd %d",
                                     (Llu)offset, (int)fd), e);
  }
}


void
Client::mkdirs(const String &name, DispatchHandler *handler) {
  CommBufPtr cbufPtr(m_protocol.create_mkdirs_request(name));

  try { send_message(cbufPtr, handler); }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error sending mkdirs request for DFS "
                    "directory: %s", name.c_str()), e);
  }
}


void
Client::mkdirs(const String &name) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr(m_protocol.create_mkdirs_request(name));

  try {
    send_message(cbufPtr, &syncHandler);

    if (!syncHandler.wait_for_reply(eventPtr))
      throw Exception(Protocol::response_code(eventPtr.get()),
                      m_protocol.string_format_message(eventPtr).c_str());
  }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error mkdirs for DFS directory %s",
                    name.c_str()), e);
  }
}


void
Client::flush(int32_t fd, DispatchHandler *handler) {
  CommBufPtr cbufPtr(m_protocol.create_flush_request(fd));
  
  try { send_message(cbufPtr, handler); }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error flushing DFS fd %d", (int)fd), e);
  }
}


void
Client::flush(int32_t fd) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr(m_protocol.create_flush_request(fd));

  try {
    send_message(cbufPtr, &syncHandler);

    if (!syncHandler.wait_for_reply(eventPtr))
      throw Exception(Protocol::response_code(eventPtr.get()),
                      m_protocol.string_format_message(eventPtr).c_str());
  }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error flushing DFS fd %d", (int)fd), e);
  }
}


void
Client::rmdir(const String &name, DispatchHandler *handler) {
  CommBufPtr cbufPtr(m_protocol.create_rmdir_request(name));

  try { send_message(cbufPtr, handler); }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error sending rmdir request for DFS "
                    "directory: %s", name.c_str()), e);
  }
}


void
Client::rmdir(const String &name, bool force) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr(m_protocol.create_rmdir_request(name));

  try {
    send_message(cbufPtr, &syncHandler);

    if (!syncHandler.wait_for_reply(eventPtr)) {
      int error = Protocol::response_code(eventPtr.get());

      if (!force || error != Error::DFSBROKER_FILE_NOT_FOUND)
        throw Exception(error,
                        m_protocol.string_format_message(eventPtr).c_str());
    }
  }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error removing DFS directory: %s",
                    name.c_str()), e);
  }
}


/**
 *
 */
void
Client::readdir(const String &name, DispatchHandler *handler) {
  CommBufPtr cbufPtr(m_protocol.create_readdir_request(name));
  
  try { send_message(cbufPtr, handler); }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error sending readdir request for DFS "
                    "directory: %s", name.c_str()), e);
  }
}


/**
 *
 */
void
Client::readdir(const String &name, std::vector<String> &listing) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr(m_protocol.create_readdir_request(name));

  try {
    send_message(cbufPtr, &syncHandler);

    if (!syncHandler.wait_for_reply(eventPtr))
      throw Exception(Protocol::response_code(eventPtr.get()),
                      m_protocol.string_format_message(eventPtr).c_str());

    decode_response_readdir(eventPtr, listing);
  }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error reading directory entries for DFS "
                    "directory: %s", name.c_str()), e);
  }
}


void
Client::exists(const String &name, DispatchHandler *handler) {
  CommBufPtr cbufPtr(m_protocol.create_exists_request(name));

  try { send_message(cbufPtr, handler); }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error sending 'exists' request for DFS "
                    "path: %s", name.c_str()), e);
  }
}


bool
Client::exists(const String &name) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr(m_protocol.create_exists_request(name));

  try {
    send_message(cbufPtr, &syncHandler);

    if (!syncHandler.wait_for_reply(eventPtr))
      throw Exception(Protocol::response_code(eventPtr.get()),
                      m_protocol.string_format_message(eventPtr).c_str());

    return decode_response_exists(eventPtr);
  }
  catch (Exception &e) {
    throw Exception(e.code(), format("Error checking existence of DFS path: %s",
                    name.c_str()), e);
  }
}


void
Client::send_message(CommBufPtr &cbufPtr, DispatchHandler *handler) {
  int error = m_comm->send_request(m_addr, m_timeout, cbufPtr, handler);

  if (error != Error::OK)
    throw Exception(error, format("DFS send_request to %s:%d failed - %s",
                    inet_ntoa(m_addr.sin_addr), ntohs(m_addr.sin_port),
                    Error::get_text(error)));
}
