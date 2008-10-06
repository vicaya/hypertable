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

#include "Common/Error.h"
#include "AsyncComm/HeaderBuilder.h"
#include "Common/Serialization.h"

#include "Protocol.h"

using namespace std;
using namespace Hyperspace;
using namespace Hypertable;
using namespace Serialization;


const char *Hyperspace::Protocol::command_strs[COMMAND_MAX] = {
  "keepalive",
  "handshake",
  "open",
  "stat",
  "cancel",
  "close",
  "poison",
  "mkdir",
  "attrset",
  "attrget",
  "attrdel",
  "exists",
  "delete",
  "readdir",
  "lock",
  "release",
  "checksequencer",
  "status"
};


/**
 *
 */
const char *Hyperspace::Protocol::command_text(short command) {
  if (command < 0 || command >= COMMAND_MAX)
    return "UNKNOWN";
  return command_strs[command];
}

/**
 *
 */
CommBuf *
Hyperspace::Protocol::create_client_keepalive_request(uint64_t session_id,
    uint64_t last_known_event, bool shutdown) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE);
  CommBuf *cbuf = new CommBuf(hbuilder, 19);
  cbuf->append_i16(COMMAND_KEEPALIVE);
  cbuf->append_i64(session_id);
  cbuf->append_i64(last_known_event);
  cbuf->append_bool(shutdown);
  return cbuf;
}


/**
 *
 */
CommBuf *
Hyperspace::Protocol::create_server_keepalive_request(uint64_t session_id,
                                                      int error) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE);
  CommBuf *cbuf = new CommBuf(hbuilder, 18);
  cbuf->append_i16(COMMAND_KEEPALIVE);
  cbuf->append_i64(session_id);
  cbuf->append_i32(error);
  cbuf->append_i32(0);
  return cbuf;
}


/**
 *
 */
CommBuf *
Hyperspace::Protocol::create_server_keepalive_request(
    SessionDataPtr &session_data) {
  ScopedLock lock(session_data->mutex);
  CommBuf *cbuf = 0;
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE);
  uint32_t len = 18;
  list<Notification *>::iterator iter;

  for (iter = session_data->notifications.begin();
       iter != session_data->notifications.end(); ++iter) {
    len += 8;  // handle
    len += (*iter)->event_ptr->encoded_length();
  }

  cbuf = new CommBuf(hbuilder, len);
  cbuf->append_i16(COMMAND_KEEPALIVE);
  cbuf->append_i64(session_data->id);
  cbuf->append_i32(Error::OK);
  cbuf->append_i32(session_data->notifications.size());
  for (iter = session_data->notifications.begin();
       iter != session_data->notifications.end(); ++iter) {
    cbuf->append_i64((*iter)->handle);
    (*iter)->event_ptr->encode(cbuf);
  }

  return cbuf;
}


/**
 *
 */
CommBuf *Hyperspace::Protocol::create_handshake_request(uint64_t session_id) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE);
  CommBuf *cbuf = new CommBuf(hbuilder, 10);
  cbuf->append_i16(COMMAND_HANDSHAKE);
  cbuf->append_i64(session_id);
  return cbuf;
}


/**
 *
 */
CommBuf *
Hyperspace::Protocol::create_open_request(const std::string &name,
    uint32_t flags, HandleCallbackPtr &callback,
    std::vector<Attribute> &init_attrs) {
  size_t len = 14 + encoded_length_vstr(name.size());
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, filename_to_group(name));
  for (size_t i=0; i<init_attrs.size(); i++)
    len += encoded_length_vstr(name.size())
           + encoded_length_vstr(init_attrs[i].value_len);

  CommBuf *cbuf = new CommBuf(hbuilder, len);

  cbuf->append_i16(COMMAND_OPEN);
  cbuf->append_i32(flags);
  if (callback)
    cbuf->append_i32(callback->get_event_mask());
  else
    cbuf->append_i32(0);
  cbuf->append_vstr(name);

  // append initial attributes
  cbuf->append_i32(init_attrs.size());
  for (size_t i=0; i<init_attrs.size(); i++) {
    cbuf->append_vstr(init_attrs[i].name);
    cbuf->append_vstr(init_attrs[i].value, init_attrs[i].value_len);
  }

  return cbuf;
}


CommBuf *Hyperspace::Protocol::create_close_request(uint64_t handle) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE,
                         (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL));
  CommBuf *cbuf = new CommBuf(hbuilder, 10);
  cbuf->append_i16(COMMAND_CLOSE);
  cbuf->append_i64(handle);
  return cbuf;
}

CommBuf *Hyperspace::Protocol::create_mkdir_request(const std::string &name) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, filename_to_group(name));
  CommBuf *cbuf = new CommBuf(hbuilder, 2 + encoded_length_vstr(name.size()));
  cbuf->append_i16(COMMAND_MKDIR);
  cbuf->append_vstr(name);
  return cbuf;
}


CommBuf *Hyperspace::Protocol::create_delete_request(const std::string &name) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, filename_to_group(name));
  CommBuf *cbuf = new CommBuf(hbuilder, 2 + encoded_length_vstr(name.size()));
  cbuf->append_i16(COMMAND_DELETE);
  cbuf->append_vstr(name);
  return cbuf;
}


CommBuf *
Hyperspace::Protocol::create_attr_set_request(uint64_t handle,
    const std::string &name, const void *value, size_t value_len) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE,
                         (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL));
  CommBuf *cbuf = new CommBuf(hbuilder, 10 + encoded_length_vstr(name.size())
                              + encoded_length_vstr(value_len));
  cbuf->append_i16(COMMAND_ATTRSET);
  cbuf->append_i64(handle);
  cbuf->append_vstr(name);
  cbuf->append_vstr(value, value_len);
  return cbuf;
}


CommBuf *
Hyperspace::Protocol::create_attr_get_request(uint64_t handle,
                                              const std::string &name) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE,
                         (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL));
  CommBuf *cbuf = new CommBuf(hbuilder, 10 + encoded_length_vstr(name.size()));
  cbuf->append_i16(COMMAND_ATTRGET);
  cbuf->append_i64(handle);
  cbuf->append_vstr(name);
  return cbuf;
}


CommBuf *
Hyperspace::Protocol::create_attr_del_request(uint64_t handle,
                                              const std::string &name) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE,
                         (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL));
  CommBuf *cbuf = new CommBuf(hbuilder, 10 + encoded_length_vstr(name.size()));
  cbuf->append_i16(COMMAND_ATTRDEL);
  cbuf->append_i64(handle);
  cbuf->append_vstr(name);
  return cbuf;
}


CommBuf *Hyperspace::Protocol::create_readdir_request(uint64_t handle) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE,
                         (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL));
  CommBuf *cbuf = new CommBuf(hbuilder, 10);
  cbuf->append_i16(COMMAND_READDIR);
  cbuf->append_i64(handle);
  return cbuf;
}


CommBuf *Hyperspace::Protocol::create_exists_request(const std::string &name) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, filename_to_group(name));
  CommBuf *cbuf = new CommBuf(hbuilder, 2 + encoded_length_vstr(name.size()));
  cbuf->append_i16(COMMAND_EXISTS);
  cbuf->append_vstr(name);
  return cbuf;
}


CommBuf *
Hyperspace::Protocol::create_lock_request(uint64_t handle, uint32_t mode,
                                          bool try_lock) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE,
                         (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL));
  CommBuf *cbuf = new CommBuf(hbuilder, 15);
  cbuf->append_i16(COMMAND_LOCK);
  cbuf->append_i64(handle);
  cbuf->append_i32(mode);
  cbuf->append_byte(try_lock);
  return cbuf;
}


CommBuf *Hyperspace::Protocol::create_release_request(uint64_t handle) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE,
                         (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL));
  CommBuf *cbuf = new CommBuf(hbuilder, 10);
  cbuf->append_i16(COMMAND_RELEASE);
  cbuf->append_i64(handle);
  return cbuf;
}


/**
 *
 */
CommBuf *Hyperspace::Protocol::create_status_request() {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE);
  CommBuf *cbuf = new CommBuf(hbuilder, 2);
  cbuf->append_i16(COMMAND_STATUS);
  return cbuf;
}
