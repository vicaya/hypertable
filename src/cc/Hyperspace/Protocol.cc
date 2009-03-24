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
#include "Common/Serialization.h"
#include "Common/StringExt.h"

#include "AsyncComm/CommHeader.h"

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
  "attrlist",
  "attrexists",
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
const char *Hyperspace::Protocol::command_text(uint64_t command) {
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
  CommHeader header(COMMAND_KEEPALIVE);
  header.flags |= CommHeader::FLAGS_BIT_URGENT;
  CommBuf *cbuf = new CommBuf(header, 17);
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
  CommHeader header(COMMAND_KEEPALIVE);
  header.flags |= CommHeader::FLAGS_BIT_URGENT;
  CommBuf *cbuf = new CommBuf(header, 16);
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
  uint32_t len = 16;
  CommBuf *cbuf = 0;
  CommHeader header(COMMAND_KEEPALIVE);
  header.flags |= CommHeader::FLAGS_BIT_URGENT;

  cbuf = session_data->serialize_notifications_for_keepalive(header, len);
  return cbuf;
}


/**
 *
 */
CommBuf *Hyperspace::Protocol::create_handshake_request(uint64_t session_id,
                                                        const std::string &name) {
  CommHeader header(COMMAND_HANDSHAKE);
  header.flags |= CommHeader::FLAGS_BIT_URGENT;
  size_t len = 8 + encoded_length_vstr(name);
  CommBuf *cbuf = new CommBuf(header, len);
  cbuf->append_i64(session_id);
  cbuf->append_vstr(name);
  return cbuf;
}


/**
 *
 */
CommBuf *
Hyperspace::Protocol::create_open_request(const std::string &name,
    uint32_t flags, HandleCallbackPtr &callback,
    std::vector<Attribute> &init_attrs) {
  size_t len = 12 + encoded_length_vstr(name.size());
  CommHeader header(COMMAND_OPEN);
  for (size_t i=0; i<init_attrs.size(); i++)
    len += encoded_length_vstr(init_attrs[i].name)
           + encoded_length_vstr(init_attrs[i].value_len);

  CommBuf *cbuf = new CommBuf(header, len);

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
  CommHeader header(COMMAND_CLOSE);
  header.gid = (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL);
  CommBuf *cbuf = new CommBuf(header, 8);
  cbuf->append_i64(handle);
  return cbuf;
}

CommBuf *Hyperspace::Protocol::create_mkdir_request(const std::string &name) {
  CommHeader header(COMMAND_MKDIR);
  header.gid = filename_to_group(name);
  CommBuf *cbuf = new CommBuf(header, encoded_length_vstr(name.size()));
  cbuf->append_vstr(name);
  return cbuf;
}


CommBuf *Hyperspace::Protocol::create_delete_request(const std::string &name) {
  CommHeader header(COMMAND_DELETE);
  header.gid = filename_to_group(name);
  CommBuf *cbuf = new CommBuf(header, encoded_length_vstr(name.size()));
  cbuf->append_vstr(name);
  return cbuf;
}


CommBuf *
Hyperspace::Protocol::create_attr_set_request(uint64_t handle,
    const std::string &name, const void *value, size_t value_len) {
  CommHeader header(COMMAND_ATTRSET);
  header.gid = (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL);
  CommBuf *cbuf = new CommBuf(header, 8 + encoded_length_vstr(name.size())
                              + encoded_length_vstr(value_len));
  cbuf->append_i64(handle);
  cbuf->append_vstr(name);
  cbuf->append_vstr(value, value_len);
  return cbuf;
}


CommBuf *
Hyperspace::Protocol::create_attr_get_request(uint64_t handle,
                                              const std::string &name) {
  CommHeader header(COMMAND_ATTRGET);
  header.gid = (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL);
  CommBuf *cbuf = new CommBuf(header, 8 + encoded_length_vstr(name.size()));
  cbuf->append_i64(handle);
  cbuf->append_vstr(name);
  return cbuf;
}


CommBuf *
Hyperspace::Protocol::create_attr_del_request(uint64_t handle,
                                              const std::string &name) {
  CommHeader header(COMMAND_ATTRDEL);
  header.gid = (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL);
  CommBuf *cbuf = new CommBuf(header, 8 + encoded_length_vstr(name.size()));
  cbuf->append_i64(handle);
  cbuf->append_vstr(name);
  return cbuf;
}

CommBuf *
Hyperspace::Protocol::create_attr_exists_request(uint64_t handle, const std::string &name) {
  CommHeader header(COMMAND_ATTREXISTS);
  header.gid = (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL);
  CommBuf *cbuf = new CommBuf(header, 8 + encoded_length_vstr(name.size()));
  cbuf->append_i64(handle);
  cbuf->append_vstr(name);
  return cbuf;
}

CommBuf *
Hyperspace::Protocol::create_attr_list_request(uint64_t handle) {
  CommHeader header(COMMAND_ATTRLIST);
  header.gid = (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL);
  CommBuf *cbuf = new CommBuf(header, 8);
  cbuf->append_i64(handle);
  return cbuf;
}

CommBuf *Hyperspace::Protocol::create_readdir_request(uint64_t handle) {
  CommHeader header(COMMAND_READDIR);
  header.gid = (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL);
  CommBuf *cbuf = new CommBuf(header, 8);
  cbuf->append_i64(handle);
  return cbuf;
}


CommBuf *Hyperspace::Protocol::create_exists_request(const std::string &name) {
  CommHeader header(COMMAND_EXISTS);
  header.gid = filename_to_group(name);
  CommBuf *cbuf = new CommBuf(header, encoded_length_vstr(name.size()));
  cbuf->append_vstr(name);
  return cbuf;
}


CommBuf *
Hyperspace::Protocol::create_lock_request(uint64_t handle, uint32_t mode,
                                          bool try_lock) {
  CommHeader header(COMMAND_LOCK);
  header.gid = (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL);
  CommBuf *cbuf = new CommBuf(header, 13);
  cbuf->append_i64(handle);
  cbuf->append_i32(mode);
  cbuf->append_byte(try_lock);
  return cbuf;
}


CommBuf *Hyperspace::Protocol::create_release_request(uint64_t handle) {
  CommHeader header(COMMAND_RELEASE);
  header.gid = (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL);
  CommBuf *cbuf = new CommBuf(header, 8);
  cbuf->append_i64(handle);
  return cbuf;
}


/**
 *
 */
CommBuf *Hyperspace::Protocol::create_status_request() {
  CommHeader header(COMMAND_STATUS);
  header.flags |= CommHeader::FLAGS_BIT_URGENT;
  CommBuf *cbuf = new CommBuf(header, 0);
  return cbuf;
}
