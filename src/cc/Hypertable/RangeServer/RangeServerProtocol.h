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
#ifndef HYPERTABLE_RANGESERVERPROTOCOL_H
#define HYPERTABLE_RANGESERVERPROTOCOL_H

#include "AsyncComm/Protocol.h"

namespace hypertable {

  class RangeServerProtocol : public Protocol {

  public:
    static const short COMMAND_LOAD_RANGE       = 0;
    static const short COMMAND_UPDATE           = 1;
    static const short COMMAND_CREATE_SCANNER   = 2;
    static const short COMMAND_FETCH_SCANBLOCK  = 3;
    static const short COMMAND_COMPACT          = 4;
    static const short COMMAND_MAX              = 5;

    static const char *mCommandStrings[];

    virtual const char *CommandText(short command);
  };

}

#endif // HYPERTABLE_RANGESERVERPROTOCOL_H
