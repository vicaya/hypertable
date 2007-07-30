/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
#ifndef HYPERSPACE_HTFSPROTOCOL_H
#define HYPERSPACE_HTFSPROTOCOL_H

extern "C" {
#include <stdint.h>
#include <string.h>
}

#include "AsyncComm/CommBuf.h"
#include "AsyncComm/Protocol.h"

using namespace hypertable;

namespace hypertable {

  class HyperspaceProtocol : public Protocol {

  public:

    virtual const char *CommandText(short command);

    CommBuf *CreateMkdirsRequest(const char *fname);

    CommBuf *CreateCreateRequest(const char *fname);

    CommBuf *CreateAttrSetRequest(const char *fname, const char *aname, const char *avalue);

    CommBuf *CreateAttrGetRequest(const char *fname, const char *aname);

    CommBuf *CreateAttrDelRequest(const char *fname, const char *aname);

    CommBuf *CreateExistsRequest(const char *fname);

    static const uint16_t COMMAND_CREATE  = 0;
    static const uint16_t COMMAND_DELETE  = 1;
    static const uint16_t COMMAND_MKDIRS  = 2;
    static const uint16_t COMMAND_ATTRSET = 3;
    static const uint16_t COMMAND_ATTRGET = 4;
    static const uint16_t COMMAND_ATTRDEL = 5;
    static const uint16_t COMMAND_EXISTS  = 6;
    static const uint16_t COMMAND_MAX     = 7;

    static const char * commandStrings[COMMAND_MAX];

  };

}

#endif // HYPERSPACE_HTFSPROTOCOL_H
