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
#include "Common/StringExt.h"

#include "Global.h"
#include "RequestCompact.h"
#include "RequestCreateScanner.h"
#include "RequestFetchScanblock.h"
#include "RequestLoadRange.h"
#include "RequestUpdate.h"
#include "RequestFactory.h"

using namespace hypertable;

Runnable *RequestFactory::newInstance(Event &event, short command) throw(ProtocolException) {

  if (command < 0 || command >= RangeServerProtocol::COMMAND_MAX) {
    std::string message = (std::string)"Invalid command (" + command + ")";
    throw ProtocolException(message);
  }

  switch (command) {
  case RangeServerProtocol::COMMAND_LOAD_RANGE:
    return new RequestLoadRange(event);
  case RangeServerProtocol::COMMAND_UPDATE:
    return new RequestUpdate(event);
  case RangeServerProtocol::COMMAND_CREATE_SCANNER:
    return new RequestCreateScanner(event);
  case RangeServerProtocol::COMMAND_FETCH_SCANBLOCK:
    return new RequestFetchScanblock(event);
  case RangeServerProtocol::COMMAND_COMPACT:
    return new RequestCompact(event);
  default:
    std::string message = (string)"Command '" + Global::protocol->CommandText(command) + "' (" + command + ") not implemented";
    throw ProtocolException(message);
  }
}

