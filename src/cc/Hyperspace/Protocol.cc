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
#include <iostream>

#include "Common/Error.h"
#include "AsyncComm/HeaderBuilder.h"
#include "AsyncComm/Serialization.h"

#include "Protocol.h"

using namespace Hyperspace;
using namespace Hypertable;
using namespace std;


const char *Hyperspace::Protocol::commandStrings[COMMAND_MAX] = {
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
  return commandStrings[command];
}

/**
 *
 */
CommBuf *Hyperspace::Protocol::create_client_keepalive_request(uint64_t sessionId, uint64_t lastKnownEvent) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE);
  CommBuf *cbuf = new CommBuf(hbuilder, 18);
  cbuf->append_short(COMMAND_KEEPALIVE);
  cbuf->append_long(sessionId);
  cbuf->append_long(lastKnownEvent);
  return cbuf;
}


/**
 *
 */
CommBuf *Hyperspace::Protocol::create_server_keepalive_request(uint64_t sessionId, int error) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE);
  CommBuf *cbuf = new CommBuf(hbuilder, 14);
  cbuf->append_short(COMMAND_KEEPALIVE);
  cbuf->append_long(sessionId);
  cbuf->append_int(error);
  cbuf->append_int(0);
  return cbuf;
}


/**
 *
 */
CommBuf *Hyperspace::Protocol::create_server_keepalive_request(SessionDataPtr &sessionPtr) {
  boost::mutex::scoped_lock lock(sessionPtr->mutex);
  CommBuf *cbuf = 0;
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE);
  uint32_t len = 18;
  list<Notification *>::iterator iter;

  for (iter = sessionPtr->notifications.begin(); iter != sessionPtr->notifications.end(); iter++) {
    len += 8;  // handle
    len += (*iter)->eventPtr->encoded_length();
  }

  cbuf = new CommBuf(hbuilder, len);
  cbuf->append_short(COMMAND_KEEPALIVE);
  cbuf->append_long(sessionPtr->id);
  cbuf->append_int(Error::OK);
  cbuf->append_int(sessionPtr->notifications.size());
  for (iter = sessionPtr->notifications.begin(); iter != sessionPtr->notifications.end(); iter++) {
    cbuf->append_long((*iter)->handle);
    (*iter)->eventPtr->encode(cbuf);
    /**
    HT_ERRORF("Encoding session=%lld handle=%lld eventMask=%d eventId=%lld",
		 sessionPtr->id, (*iter)->handle, (*iter)->eventPtr->get_mask(), (*iter)->eventPtr->get_id());
    **/
  }

  return cbuf;
}


/**
 *
 */
CommBuf *Hyperspace::Protocol::create_handshake_request(uint64_t sessionId) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE);
  CommBuf *cbuf = new CommBuf(hbuilder, 10);
  cbuf->append_short(COMMAND_HANDSHAKE);
  cbuf->append_long(sessionId);
  return cbuf;
}


/**
 *
 */
CommBuf *Hyperspace::Protocol::create_open_request(std::string &name, uint32_t flags, HandleCallbackPtr &callbackPtr, std::vector<AttributeT> &initAttrs) {
  size_t len = 14 + Serialization::encoded_length_string(name);
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, fileNameToGroupId(name));
  for (size_t i=0; i<initAttrs.size(); i++)
    len += Serialization::encoded_length_string(name) + Serialization::encoded_length_byte_array(initAttrs[i].valueLen);

  CommBuf *cbuf = new CommBuf(hbuilder, len);

  cbuf->append_short(COMMAND_OPEN);
  cbuf->append_int(flags);
  if (callbackPtr)
    cbuf->append_int(callbackPtr->get_event_mask());
  else
    cbuf->append_int(0);
  cbuf->append_string(name);

  // append initial attributes
  cbuf->append_int(initAttrs.size());
  for (size_t i=0; i<initAttrs.size(); i++) {
    cbuf->append_string(initAttrs[i].name);
    cbuf->append_byte_array(initAttrs[i].value, initAttrs[i].valueLen);
  }

  return cbuf;
}


CommBuf *Hyperspace::Protocol::create_close_request(uint64_t handle) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL));
  CommBuf *cbuf = new CommBuf(hbuilder, 10);
  cbuf->append_short(COMMAND_CLOSE);
  cbuf->append_long(handle);
  return cbuf;
}

CommBuf *Hyperspace::Protocol::create_mkdir_request(std::string &name) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, fileNameToGroupId(name));
  CommBuf *cbuf = new CommBuf(hbuilder, 2 + Serialization::encoded_length_string(name));
  cbuf->append_short(COMMAND_MKDIR);
  cbuf->append_string(name);
  return cbuf;
}


CommBuf *Hyperspace::Protocol::create_delete_request(std::string &name) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, fileNameToGroupId(name));
  CommBuf *cbuf = new CommBuf(hbuilder, 2 + Serialization::encoded_length_string(name));
  cbuf->append_short(COMMAND_DELETE);
  cbuf->append_string(name);
  return cbuf;
}


CommBuf *Hyperspace::Protocol::create_attr_set_request(uint64_t handle, std::string &name, const void *value, size_t valueLen) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL));
  CommBuf *cbuf = new CommBuf(hbuilder, 10 + Serialization::encoded_length_string(name) + Serialization::encoded_length_byte_array(valueLen));
  cbuf->append_short(COMMAND_ATTRSET);
  cbuf->append_long(handle);
  cbuf->append_string(name);
  cbuf->append_byte_array(value, valueLen);
  return cbuf;
}


CommBuf *Hyperspace::Protocol::create_attr_get_request(uint64_t handle, std::string &name) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL));
  CommBuf *cbuf = new CommBuf(hbuilder, 10 + Serialization::encoded_length_string(name));
  cbuf->append_short(COMMAND_ATTRGET);
  cbuf->append_long(handle);
  cbuf->append_string(name);
  return cbuf;
}


CommBuf *Hyperspace::Protocol::create_attr_del_request(uint64_t handle, std::string &name) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL));
  CommBuf *cbuf = new CommBuf(hbuilder, 10 + Serialization::encoded_length_string(name));
  cbuf->append_short(COMMAND_ATTRDEL);
  cbuf->append_long(handle);
  cbuf->append_string(name);
  return cbuf;
}


CommBuf *Hyperspace::Protocol::create_readdir_request(uint64_t handle) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL));
  CommBuf *cbuf = new CommBuf(hbuilder, 10);
  cbuf->append_short(COMMAND_READDIR);
  cbuf->append_long(handle);
  return cbuf;
}


CommBuf *Hyperspace::Protocol::create_exists_request(std::string &name) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, fileNameToGroupId(name));
  CommBuf *cbuf = new CommBuf(hbuilder, 2 + Serialization::encoded_length_string(name));
  cbuf->append_short(COMMAND_EXISTS);
  cbuf->append_string(name);
  return cbuf;
}


CommBuf *Hyperspace::Protocol::create_lock_request(uint64_t handle, uint32_t mode, bool tryAcquire) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL));
  CommBuf *cbuf = new CommBuf(hbuilder, 15);
  cbuf->append_short(COMMAND_LOCK);
  cbuf->append_long(handle);
  cbuf->append_int(mode);
  cbuf->append_byte(tryAcquire);
  return cbuf;
}


CommBuf *Hyperspace::Protocol::create_release_request(uint64_t handle) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL));
  CommBuf *cbuf = new CommBuf(hbuilder, 10);
  cbuf->append_short(COMMAND_RELEASE);
  cbuf->append_long(handle);
  return cbuf;
}


/**
 *
 */
CommBuf *Hyperspace::Protocol::create_status_request() {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE);
  CommBuf *cbuf = new CommBuf(hbuilder, 2);
  cbuf->append_short(COMMAND_STATUS);
  return cbuf;
}
