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

namespace Hyperspace {

  class Protocol : public hypertable::Protocol {

  public:

    virtual const char *CommandText(short command);

    static hypertable::CommBuf *CreateKeepAliveRequest(uint32_t sessionId);
    static hypertable::CommBuf *CreateHandshakeRequest(uint32_t sessionId);

    /**
    CommBuf *CreateMkdirsRequest(const char *fname);
    CommBuf *CreateCreateRequest(const char *fname);
    CommBuf *CreateAttrSetRequest(const char *fname, const char *aname, const char *avalue);
    CommBuf *CreateAttrGetRequest(const char *fname, const char *aname);
    CommBuf *CreateAttrDelRequest(const char *fname, const char *aname);
    CommBuf *CreateExistsRequest(const char *fname);
    CommBuf *CreateDeleteRequest(const char *fname);
    CommBuf *CreateStatusRequest();
    */

    static const uint16_t COMMAND_KEEPALIVE      = 0;
    static const uint16_t COMMAND_HANDSHAKE      = 1; 
    static const uint16_t COMMAND_OPEN           = 2;
    static const uint16_t COMMAND_CANCEL         = 3;
    static const uint16_t COMMAND_CLOSE          = 4;
    static const uint16_t COMMAND_POISON         = 5;
    static const uint16_t COMMAND_MKDIR          = 6;
    static const uint16_t COMMAND_ATTRSET        = 7;
    static const uint16_t COMMAND_ATTRGET        = 8;
    static const uint16_t COMMAND_ATTRDEL        = 9;
    static const uint16_t COMMAND_EXISTS         = 10;
    static const uint16_t COMMAND_DELETE         = 11;
    static const uint16_t COMMAND_READDIR        = 12;
    static const uint16_t COMMAND_ACQUIRE        = 13;
    static const uint16_t COMMAND_TRYACQUIRE     = 14;
    static const uint16_t COMMAND_RELEASE        = 15;
    static const uint16_t COMMAND_CHECKSEQUENCER = 16;
    static const uint16_t COMMAND_STATUS         = 17;
    static const uint16_t COMMAND_MAX            = 18;

    static const char * commandStrings[COMMAND_MAX];

  private:

    uint32_t fileNameToGroupId(const char *fname) {
      uint32_t gid = 0;
      for (const char *ptr=fname; *ptr; ++ptr)
	gid += (uint32_t)*ptr;
      return gid;
    }

  };

}

#endif // HYPERSPACE_PROTOCOL_H
