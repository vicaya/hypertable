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
using namespace hypertable;
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
const char *Hyperspace::Protocol::CommandText(short command) {
  if (command < 0 || command >= COMMAND_MAX)
    return "UNKNOWN";
  return commandStrings[command];
}

/**
 *
 */
CommBuf *Hyperspace::Protocol::CreateClientKeepaliveRequest(uint64_t sessionId, uint64_t lastKnownEvent) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE);
  CommBuf *cbuf = new CommBuf(hbuilder, 18);
  cbuf->AppendShort(COMMAND_KEEPALIVE);
  cbuf->AppendLong(sessionId);
  cbuf->AppendLong(lastKnownEvent);
  return cbuf;
}


/**
 *
 */
CommBuf *Hyperspace::Protocol::CreateServerKeepaliveRequest(uint64_t sessionId, int error) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE);
  CommBuf *cbuf = new CommBuf(hbuilder, 14);
  cbuf->AppendShort(COMMAND_KEEPALIVE);
  cbuf->AppendLong(sessionId);
  cbuf->AppendInt(error);
  cbuf->AppendInt(0);
  return cbuf;
}


/**
 *
 */
CommBuf *Hyperspace::Protocol::CreateServerKeepaliveRequest(SessionDataPtr &sessionPtr) {
  boost::mutex::scoped_lock lock(sessionPtr->mutex);
  CommBuf *cbuf = 0;
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE);
  uint32_t len = 18;
  list<Notification *>::iterator iter;

  for (iter = sessionPtr->notifications.begin(); iter != sessionPtr->notifications.end(); iter++) {
    len += 20;
    len += Serialization::EncodedLengthString((*iter)->eventPtr->GetName());
  }

  cbuf = new CommBuf(hbuilder, len);
  cbuf->AppendShort(COMMAND_KEEPALIVE);
  cbuf->AppendLong(sessionPtr->id);
  cbuf->AppendInt(Error::OK);
  cbuf->AppendInt(sessionPtr->notifications.size());
  for (iter = sessionPtr->notifications.begin(); iter != sessionPtr->notifications.end(); iter++) {
    cbuf->AppendLong((*iter)->handle);
    cbuf->AppendLong((*iter)->eventPtr->GetId());
    cbuf->AppendInt((*iter)->eventPtr->GetType());
    cbuf->AppendString((*iter)->eventPtr->GetName());
  }

  return cbuf;
}


/**
 *
 */
CommBuf *Hyperspace::Protocol::CreateHandshakeRequest(uint64_t sessionId) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE);
  hbuilder.AssignUniqueId();
  CommBuf *cbuf = new CommBuf(hbuilder, 10);
  cbuf->AppendShort(COMMAND_HANDSHAKE);
  cbuf->AppendLong(sessionId);
  return cbuf;
}

CommBuf *Hyperspace::Protocol::CreateOpenRequest(std::string &name, uint32_t flags, HandleCallbackPtr &callbackPtr) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, fileNameToGroupId(name));
  hbuilder.AssignUniqueId();
  CommBuf *cbuf = new CommBuf(hbuilder, 10 + Serialization::EncodedLengthString(name));
  cbuf->AppendShort(COMMAND_OPEN);
  cbuf->AppendInt(flags);
  cbuf->AppendInt(callbackPtr->GetEventMask());
  cbuf->AppendString(name);
  return cbuf;
}


CommBuf *Hyperspace::Protocol::CreateCloseRequest(uint64_t handle) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL));
  hbuilder.AssignUniqueId();
  CommBuf *cbuf = new CommBuf(hbuilder, 10);
  cbuf->AppendShort(COMMAND_CLOSE);
  cbuf->AppendLong(handle);
  return cbuf;
}

CommBuf *Hyperspace::Protocol::CreateMkdirRequest(std::string &name) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, fileNameToGroupId(name));
  hbuilder.AssignUniqueId();
  CommBuf *cbuf = new CommBuf(hbuilder, 2 + Serialization::EncodedLengthString(name));
  cbuf->AppendShort(COMMAND_MKDIR);
  cbuf->AppendString(name);
  return cbuf;
}


CommBuf *Hyperspace::Protocol::CreateDeleteRequest(std::string &name) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, fileNameToGroupId(name));
  hbuilder.AssignUniqueId();
  CommBuf *cbuf = new CommBuf(hbuilder, 2 + Serialization::EncodedLengthString(name));
  cbuf->AppendShort(COMMAND_DELETE);
  cbuf->AppendString(name);
  return cbuf;
}


CommBuf *Hyperspace::Protocol::CreateAttrSetRequest(uint64_t handle, std::string &name, const void *value, size_t valueLen) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL));
  hbuilder.AssignUniqueId();
  CommBuf *cbuf = new CommBuf(hbuilder, 10 + Serialization::EncodedLengthString(name) + Serialization::EncodedLengthByteArray(valueLen));
  cbuf->AppendShort(COMMAND_ATTRSET);
  cbuf->AppendLong(handle);
  cbuf->AppendString(name);
  cbuf->AppendByteArray(value, valueLen);
  return cbuf;
}


CommBuf *Hyperspace::Protocol::CreateAttrGetRequest(uint64_t handle, std::string &name) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL));
  hbuilder.AssignUniqueId();
  CommBuf *cbuf = new CommBuf(hbuilder, 10 + Serialization::EncodedLengthString(name));
  cbuf->AppendShort(COMMAND_ATTRGET);
  cbuf->AppendLong(handle);
  cbuf->AppendString(name);
  return cbuf;
}


CommBuf *Hyperspace::Protocol::CreateAttrDelRequest(uint64_t handle, std::string &name) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL));
  hbuilder.AssignUniqueId();
  CommBuf *cbuf = new CommBuf(hbuilder, 10 + Serialization::EncodedLengthString(name));
  cbuf->AppendShort(COMMAND_ATTRDEL);
  cbuf->AppendLong(handle);
  cbuf->AppendString(name);
  return cbuf;
}


CommBuf *Hyperspace::Protocol::CreateExistsRequest(std::string &name) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, fileNameToGroupId(name));
  hbuilder.AssignUniqueId();
  CommBuf *cbuf = new CommBuf(hbuilder, 2 + Serialization::EncodedLengthString(name));
  cbuf->AppendShort(COMMAND_EXISTS);
  cbuf->AppendString(name);
  return cbuf;
}


CommBuf *Hyperspace::Protocol::CreateLockRequest(uint64_t handle, uint32_t mode, bool tryAcquire) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL));
  hbuilder.AssignUniqueId();
  CommBuf *cbuf = new CommBuf(hbuilder, 15);
  cbuf->AppendShort(COMMAND_LOCK);
  cbuf->AppendLong(handle);
  cbuf->AppendInt(mode);
  cbuf->AppendByte(tryAcquire);
  return cbuf;
}
