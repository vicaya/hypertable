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

