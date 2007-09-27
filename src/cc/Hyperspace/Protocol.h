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
#ifndef HYPERSPACE_PROTOCOL_H
#define HYPERSPACE_PROTOCOL_H

extern "C" {
#include <stdint.h>
#include <string.h>
}

#include "AsyncComm/CommBuf.h"
#include "AsyncComm/Protocol.h"

#include "HandleCallback.h"
#include "Notification.h"
#include "SessionData.h"

using namespace hypertable;

namespace Hyperspace {

  class Protocol : public hypertable::Protocol {

  public:

    virtual const char *CommandText(short command);

    static CommBuf *CreateClientKeepaliveRequest(uint64_t sessionId, uint64_t lastKnownEvent);
    static CommBuf *CreateServerKeepaliveRequest(uint64_t sessionId, int error); 
    static CommBuf *CreateServerKeepaliveRequest(SessionDataPtr &sessionPtr);
    static CommBuf *CreateHandshakeRequest(uint64_t sessionId);

    static CommBuf *CreateOpenRequest(std::string &name, uint32_t flags, HandleCallbackPtr &callbackPtr);
    static CommBuf *CreateCloseRequest(uint64_t handle);
    static CommBuf *CreateMkdirRequest(std::string &name);
    static CommBuf *CreateDeleteRequest(std::string &name);
    static CommBuf *CreateAttrSetRequest(uint64_t handle, std::string &name, const void *value, size_t valueLen);
    static CommBuf *CreateAttrGetRequest(uint64_t handle, std::string &name);
    static CommBuf *CreateAttrDelRequest(uint64_t handle, std::string &name);
    static CommBuf *CreateExistsRequest(std::string &name);

    static CommBuf *CreateLockRequest(uint64_t handle, uint32_t mode, bool tryAcquire);
    static CommBuf *CreateReleaseRequest(uint64_t handle);

    static CommBuf *CreateEventNotification(uint64_t handle, std::string &name, const void *value, size_t valueLen);

    static const uint16_t COMMAND_KEEPALIVE      = 0;
    static const uint16_t COMMAND_HANDSHAKE      = 1; 
    static const uint16_t COMMAND_OPEN           = 2;
    static const uint16_t COMMAND_STAT           = 3; 
    static const uint16_t COMMAND_CANCEL         = 4;
    static const uint16_t COMMAND_CLOSE          = 5;
    static const uint16_t COMMAND_POISON         = 6;
    static const uint16_t COMMAND_MKDIR          = 7;
    static const uint16_t COMMAND_ATTRSET        = 8;
    static const uint16_t COMMAND_ATTRGET        = 9;
    static const uint16_t COMMAND_ATTRDEL        = 10;
    static const uint16_t COMMAND_EXISTS         = 11;
    static const uint16_t COMMAND_DELETE         = 12;
    static const uint16_t COMMAND_READDIR        = 13;
    static const uint16_t COMMAND_LOCK           = 14;
    static const uint16_t COMMAND_RELEASE        = 15;
    static const uint16_t COMMAND_CHECKSEQUENCER = 16;
    static const uint16_t COMMAND_STATUS         = 17;
    static const uint16_t COMMAND_MAX            = 18;

    static const char * commandStrings[COMMAND_MAX];

  private:

    static uint32_t fileNameToGroupId(const char *fname) {
      const char *ptr;
      uint32_t gid = 0;
      // add initial '/' if it's not there
      if (fname[0] != '/')
	gid += (uint32_t)'/';
      for (ptr=fname; *ptr; ++ptr)
	gid += (uint32_t)*ptr;
      // remove trailing slash
      if (*(ptr-1) == '/')
	gid -= (uint32_t)'/';
      return gid;
    }

    static uint32_t fileNameToGroupId(std::string &name) {
      return fileNameToGroupId(name.c_str());
    }

  };

}

#endif // HYPERSPACE_PROTOCOL_H
