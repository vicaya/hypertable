/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
