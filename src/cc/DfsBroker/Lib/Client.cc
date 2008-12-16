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

#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/Serialization.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/CommHeader.h"


#include "Client.h"

using namespace Hypertable;
using namespace Serialization;
using namespace Hypertable::DfsBroker;

Client::Client(ConnectionManagerPtr &conn_mgr, const sockaddr_in &addr,
               uint32_t timeout_ms)
    : m_conn_mgr(conn_mgr), m_addr(addr), m_timeout_ms(timeout_ms) {
  m_comm = conn_mgr->get_comm();
  conn_mgr->add(m_addr, m_timeout_ms, "DFS Broker");
}


Client::Client(ConnectionManagerPtr &conn_mgr, PropertiesPtr &cfg)
    : m_conn_mgr(conn_mgr) {
  m_comm = conn_mgr->get_comm();
  uint16_t port = cfg->get_i16("DfsBroker.Port");
  String host = cfg->get_str("DfsBroker.Host");
  m_timeout_ms = cfg->get_i32("DfsBroker.Timeout");

  InetAddr::initialize(&m_addr, host.c_str(), port);

  conn_mgr->add(m_addr, m_timeout_ms, "DFS Broker");
}

Client::Client(Comm *comm, const sockaddr_in &addr, uint32_t timeout_ms)
    : m_comm(comm), m_conn_mgr(0), m_addr(addr), m_timeout_ms(timeout_ms) {
}

Client::Client(const String &host, int port, uint32_t timeout_ms)
    : m_timeout_ms(timeout_ms) {
  InetAddr::initialize(&m_addr, host.c_str(), port);
  m_comm = Comm::instance();
  m_conn_mgr = new ConnectionManager(m_comm);
  m_conn_mgr->add(m_addr, timeout_ms, "DFS Broker");
}

void
Client::open(const String &name, DispatchHandler *handler) {
  CommBufPtr cbp(m_protocol.create_open_request(name, 0));

  try {
    send_message(cbp, handler);
  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error opening DFS file: %s", name.c_str());
  }
}


int
Client::open(const String &name) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(m_protocol.create_open_request(name, 0));

  try {
    send_message(cbp, &sync_handler);

    if (!sync_handler.wait_for_reply(event_ptr))
      HT_THROW(Protocol::response_code(event_ptr.get()),
               m_protocol.string_format_message(event_ptr).c_str());

    return decode_response_open(event_ptr);
  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error opening DFS file: %s", name.c_str());
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
      HT_ASSERT(m_buffered_reader_map.find(fd) == m_buffered_reader_map.end());
      m_buffered_reader_map[fd] =
          new ClientBufferedReaderHandler(this, fd, buf_size, outstanding,
                                          start_offset, end_offset);
    }
    return fd;
  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error opening buffered DFS file=%s buf_size=%u "
        "outstanding=%u start_offset=%llu end_offset=%llu", name.c_str(),
        buf_size, outstanding, (Llu)start_offset, (Llu)end_offset);
  }
}


void
Client::create(const String &name, bool overwrite, int32_t bufsz,
               int32_t replication, int64_t blksz,
               DispatchHandler *handler) {
  CommBufPtr cbp(m_protocol.create_create_request(name, overwrite,
                     bufsz, replication, blksz));
  try {
    send_message(cbp, handler);
  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error creating DFS file: %s:", name.c_str());
  }
}


int
Client::create(const String &name, bool overwrite, int32_t bufsz,
               int32_t replication, int64_t blksz) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(m_protocol.create_create_request(name, overwrite,
                     bufsz, replication, blksz));
  try {
    send_message(cbp, &sync_handler);

    if (!sync_handler.wait_for_reply(event_ptr))
      HT_THROW(Protocol::response_code(event_ptr.get()),
               m_protocol.string_format_message(event_ptr).c_str());

    return decode_response_create(event_ptr);
  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error creating DFS file: %s", name.c_str());
  }
}


void
Client::close(int32_t fd, DispatchHandler *handler) {
  ClientBufferedReaderHandler *reader_handler = 0;
  CommBufPtr cbp(m_protocol.create_close_request(fd));
  {
    ScopedLock lock(m_mutex);
    BufferedReaderMap::iterator iter = m_buffered_reader_map.find(fd);
    if (iter != m_buffered_reader_map.end()) {
      reader_handler = (*iter).second;
      m_buffered_reader_map.erase(iter);
    }
  }
  delete reader_handler;

  try {
    send_message(cbp, handler);
  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error closing DFS fd: %d", (int)fd);
  }
}


void
Client::close(int32_t fd) {
  ClientBufferedReaderHandler *reader_handler = 0;
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(m_protocol.create_close_request(fd));
  {
    ScopedLock lock(m_mutex);
    BufferedReaderMap::iterator iter = m_buffered_reader_map.find(fd);

    if (iter != m_buffered_reader_map.end()) {
      reader_handler = (*iter).second;
      m_buffered_reader_map.erase(iter);
    }
  }
  delete reader_handler;

  try {
    send_message(cbp, &sync_handler);

    if (!sync_handler.wait_for_reply(event_ptr))
      HT_THROW(Protocol::response_code(event_ptr.get()),
               m_protocol.string_format_message(event_ptr).c_str());
  }
  catch(Exception &e) {
    HT_THROW2F(e.code(), e, "Error closing DFS fd: %d", (int)fd);
  }
}


void
Client::read(int32_t fd, size_t len, DispatchHandler *handler) {
  CommBufPtr cbp(m_protocol.create_read_request(fd, len));

  try {
    send_message(cbp, handler);
  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error sending read request for %u bytes "
               "from DFS fd: %d", (unsigned)len, (int)fd);
  }
}


size_t
Client::read(int32_t fd, void *dst, size_t len) {
  ClientBufferedReaderHandler *reader_handler = 0;
  {
    ScopedLock lock(m_mutex);
    BufferedReaderMap::iterator iter = m_buffered_reader_map.find(fd);

    if (iter != m_buffered_reader_map.end())
      reader_handler = (*iter).second;
  }
  try {
    if (reader_handler)
      return reader_handler->read(dst, len);

    DispatchHandlerSynchronizer sync_handler;
    EventPtr event_ptr;
    CommBufPtr cbp(m_protocol.create_read_request(fd, len));
    send_message(cbp, &sync_handler);

    if (!sync_handler.wait_for_reply(event_ptr))
      HT_THROW(Protocol::response_code(event_ptr.get()),
               m_protocol.string_format_message(event_ptr).c_str());

    return decode_response_read(event_ptr, dst, len);
  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error reading %u bytes from DFS fd %d",
               (unsigned)len, (int)fd);
  }
}


void
Client::append(int32_t fd, StaticBuffer &buffer, uint32_t flags,
               DispatchHandler *handler) {
  CommBufPtr cbp(m_protocol.create_append_request(fd, buffer, flags));

  try { send_message(cbp, handler); }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error appending %u bytes to DFS fd %d",
               (unsigned)buffer.size, (int)fd);
  }
}


size_t
Client::append(int32_t fd, StaticBuffer &buffer, uint32_t flags) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(m_protocol.create_append_request(fd, buffer, flags));

  try {
    send_message(cbp, &sync_handler);

    if (!sync_handler.wait_for_reply(event_ptr))
      HT_THROW(Protocol::response_code(event_ptr.get()),
               m_protocol.string_format_message(event_ptr).c_str());
    uint64_t offset;
    size_t ret = decode_response_append(event_ptr, &offset);

    if (buffer.size != ret)
      HT_THROWF(Error::DFSBROKER_IO_ERROR, "tried to append %u bytes but got "
                "%u", (unsigned)buffer.size, (unsigned)ret);
    return ret;
  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error appending %u bytes to DFS fd %d",
               (unsigned)buffer.size, (int)fd);
  }
}


void
Client::seek(int32_t fd, uint64_t offset, DispatchHandler *handler) {
  CommBufPtr cbp(m_protocol.create_seek_request(fd, offset));

  try { send_message(cbp, handler); }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error seeking to %llu on DFS fd %d",
               (Llu)offset, (int)fd);
  }
}


void
Client::seek(int32_t fd, uint64_t offset) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(m_protocol.create_seek_request(fd, offset));

  try {
    send_message(cbp, &sync_handler);

    if (!sync_handler.wait_for_reply(event_ptr))
      HT_THROW(Protocol::response_code(event_ptr.get()),
               m_protocol.string_format_message(event_ptr).c_str());
  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error seeking to %llu on DFS fd %d",
               (Llu)offset, (int)fd);
  }
}


void
Client::remove(const String &name, DispatchHandler *handler) {
  CommBufPtr cbp(m_protocol.create_remove_request(name));

  try { send_message(cbp, handler); }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error removing DFS file: %s", name.c_str());
  }
}


void
Client::remove(const String &name, bool force) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(m_protocol.create_remove_request(name));

  try {
    send_message(cbp, &sync_handler);

    if (!sync_handler.wait_for_reply(event_ptr)) {
      int error = Protocol::response_code(event_ptr.get());

      if (!force || error != Error::DFSBROKER_FILE_NOT_FOUND)
        HT_THROW(error, m_protocol.string_format_message(event_ptr).c_str());
    }
  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error removing DFS file: %s", name.c_str());
  }
}


void
Client::shutdown(uint16_t flags, DispatchHandler *handler) {
  CommBufPtr cbp(m_protocol.create_shutdown_request(flags));

  try { send_message(cbp, handler); }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "sending DFS shutdown (flags=%d)", (int)flags);
  }
}


void
Client::status() {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(m_protocol.create_status_request());

  try {
    send_message(cbp, &sync_handler);

    if (!sync_handler.wait_for_reply(event_ptr))
      HT_THROW(Protocol::response_code(event_ptr.get()),
               m_protocol.string_format_message(event_ptr).c_str());

    int error = decode_response(event_ptr);
    if (error != Error::OK)
      HT_THROW(error, "");
  }
  catch (Exception &e) {
    HT_THROW2(e.code(), e, e.what());
  }
}


void
Client::length(const String &name, DispatchHandler *handler) {
  CommBufPtr cbp(m_protocol.create_length_request(name));

  try { send_message(cbp, handler); }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error sending length request for DFS file: %s",
               name.c_str());
  }
}


int64_t
Client::length(const String &name) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(m_protocol.create_length_request(name));

  try {
    send_message(cbp, &sync_handler);

    if (!sync_handler.wait_for_reply(event_ptr))
      HT_THROW(Protocol::response_code(event_ptr.get()),
               m_protocol.string_format_message(event_ptr).c_str());

    return decode_response_length(event_ptr);
  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error getting length of DFS file: %s",
               name.c_str());
  }
}


void
Client::pread(int32_t fd, size_t len, uint64_t offset,
              DispatchHandler *handler) {
  CommBufPtr cbp(m_protocol.create_position_read_request(fd, offset, len));

  try { send_message(cbp, handler); }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error sending pread request at byte %llu "
               "on DFS fd %d", (Llu)offset, (int)fd);
  }
}


size_t
Client::pread(int32_t fd, void *dst, size_t len, uint64_t offset) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(m_protocol.create_position_read_request(fd, offset, len));

  try {
    send_message(cbp, &sync_handler);

    if (!sync_handler.wait_for_reply(event_ptr))
      HT_THROW(Protocol::response_code(event_ptr.get()),
               m_protocol.string_format_message(event_ptr).c_str());

    return decode_response_pread(event_ptr, dst, len);
  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error preading at byte %llu on DFS fd %d",
               (Llu)offset, (int)fd);
  }
}


void
Client::mkdirs(const String &name, DispatchHandler *handler) {
  CommBufPtr cbp(m_protocol.create_mkdirs_request(name));

  try { send_message(cbp, handler); }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error sending mkdirs request for DFS "
               "directory: %s", name.c_str());
  }
}


void
Client::mkdirs(const String &name) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(m_protocol.create_mkdirs_request(name));

  try {
    send_message(cbp, &sync_handler);

    if (!sync_handler.wait_for_reply(event_ptr))
      HT_THROW(Protocol::response_code(event_ptr.get()),
               m_protocol.string_format_message(event_ptr).c_str());
  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error mkdirs DFS directory %s", name.c_str());
  }
}


void
Client::flush(int32_t fd, DispatchHandler *handler) {
  CommBufPtr cbp(m_protocol.create_flush_request(fd));

  try { send_message(cbp, handler); }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error flushing DFS fd %d", (int)fd);
  }
}


void
Client::flush(int32_t fd) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(m_protocol.create_flush_request(fd));

  try {
    send_message(cbp, &sync_handler);

    if (!sync_handler.wait_for_reply(event_ptr))
      HT_THROW(Protocol::response_code(event_ptr.get()),
               m_protocol.string_format_message(event_ptr).c_str());
  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error flushing DFS fd %d", (int)fd);
  }
}


void
Client::rmdir(const String &name, DispatchHandler *handler) {
  CommBufPtr cbp(m_protocol.create_rmdir_request(name));

  try { send_message(cbp, handler); }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error sending rmdir request for DFS directory: "
               "%s", name.c_str());
  }
}


void
Client::rmdir(const String &name, bool force) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(m_protocol.create_rmdir_request(name));

  try {
    send_message(cbp, &sync_handler);

    if (!sync_handler.wait_for_reply(event_ptr)) {
      int error = Protocol::response_code(event_ptr.get());

      if (!force || error != Error::DFSBROKER_FILE_NOT_FOUND)
        HT_THROW(error, m_protocol.string_format_message(event_ptr).c_str());
    }
  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error removing DFS directory: %s", name.c_str());
  }
}


/**
 *
 */
void
Client::readdir(const String &name, DispatchHandler *handler) {
  CommBufPtr cbp(m_protocol.create_readdir_request(name));

  try { send_message(cbp, handler); }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error sending readdir request for DFS directory"
               ": %s", name.c_str());
  }
}


/**
 *
 */
void
Client::readdir(const String &name, std::vector<String> &listing) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(m_protocol.create_readdir_request(name));

  try {
    send_message(cbp, &sync_handler);

    if (!sync_handler.wait_for_reply(event_ptr))
      HT_THROW(Protocol::response_code(event_ptr.get()),
               m_protocol.string_format_message(event_ptr).c_str());

    decode_response_readdir(event_ptr, listing);
  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error reading directory entries for DFS "
               "directory: %s", name.c_str());
  }
}


void
Client::exists(const String &name, DispatchHandler *handler) {
  CommBufPtr cbp(m_protocol.create_exists_request(name));

  try { send_message(cbp, handler); }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "sending 'exists' request for DFS path: %s",
               name.c_str());
  }
}


bool
Client::exists(const String &name) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(m_protocol.create_exists_request(name));

  try {
    send_message(cbp, &sync_handler);

    if (!sync_handler.wait_for_reply(event_ptr))
      HT_THROW(Protocol::response_code(event_ptr.get()),
               m_protocol.string_format_message(event_ptr).c_str());

    return decode_response_exists(event_ptr);
  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error checking existence of DFS path: %s",
               name.c_str());
  }
}


void
Client::rename(const String &src, const String &dst, DispatchHandler *handler) {
  CommBufPtr cbp(m_protocol.create_rename_request(src, dst));

  try { send_message(cbp, handler); }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error sending 'rename' request for DFS "
               "path: %s -> %s", src.c_str(), dst.c_str());
  }
}


void
Client::rename(const String &src, const String &dst) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(m_protocol.create_rename_request(src, dst));

  try {
    send_message(cbp, &sync_handler);

    if (!sync_handler.wait_for_reply(event_ptr))
      HT_THROW(Protocol::response_code(event_ptr.get()),
               m_protocol.string_format_message(event_ptr).c_str());
  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error renaming of DFS path: %s -> %s",
               src.c_str(), dst.c_str());
  }
}


void
Client::debug(int32_t command, StaticBuffer &serialized_parameters, DispatchHandler *handler) {
  CommBufPtr cbp(m_protocol.create_debug_request(command, serialized_parameters));

  try { send_message(cbp, handler); }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error sending debug command %d request", command);
  }
}


void
Client::debug(int32_t command, StaticBuffer &serialized_parameters) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(m_protocol.create_debug_request(command, serialized_parameters));

  try {
    send_message(cbp, &sync_handler);

    if (!sync_handler.wait_for_reply(event_ptr))
      HT_THROW(Protocol::response_code(event_ptr.get()),
               m_protocol.string_format_message(event_ptr).c_str());
  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error sending debug command %d request", command);
  }
}



void
Client::send_message(CommBufPtr &cbp, DispatchHandler *handler) {
  int error = m_comm->send_request(m_addr, m_timeout_ms, cbp, handler);

  if (error != Error::OK)
    HT_THROWF(error, "DFS send_request to %s failed", m_addr.format().c_str());
}
