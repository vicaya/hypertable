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

#include <cstdlib>
#include <cstring>
#include <iostream>

#include "AsyncComm/DispatchHandlerSynchronizer.h"
#include "AsyncComm/Event.h"

#include "Common/Error.h"
#include "Common/Usage.h"

#include "CommandFetchScanblock.h"
#include "DisplayScanData.h"
#include "ParseRangeSpec.h"
#include "FetchSchema.h"
#include "Global.h"

using namespace hypertable;
using namespace std;

const char *CommandFetchScanblock::msUsage[] = {
  "fetch scanblock <scanner-id>",
  "",
  "  This command issues a FETCH SCANBLOCK command to the range server.  If no",
  "  <scanner-id> argument is supplied, it uses the cached scanner ID from the",
  "  most recently executed CREATE SCANNER command.",
  (const char *)0
};



int CommandFetchScanblock::run() {
  int error;
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  uint8_t *msgPtr, *endPtr;
  size_t remaining;
  ScanResultT scanResult;
  ByteString32T *key, *value;
  int32_t scannerId = -1;

  if (mArgs.size() > 1) {
    cerr << "Wrong number of arguments.  Type 'help' for usage." << endl;
    return -1;
  }

  if (mArgs.size() > 0) {
    if (mArgs[0].second != "") {
      cerr << "Invalid scanner ID." << endl;
      return -1;
    }
    scannerId = atoi(mArgs[0].first.c_str());
  }
  else
    scannerId = Global::outstandingScannerId;

  /**
   * 
   */
  if ((error = Global::rangeServer->FetchScanblock(mAddr, scannerId, &syncHandler)) != Error::OK)
    return error;

  if (!syncHandler.WaitForReply(eventPtr)) {
    LOG_VA_ERROR("Problem creating scanner - %s", (Protocol::StringFormatMessage(eventPtr)).c_str());
    return -1;
  }

  msgPtr = eventPtr->message;
  remaining = eventPtr->messageLen;

  if (!DecodeScanResult(&msgPtr, &remaining, &scanResult)) {
    LOG_ERROR("Problem decoding result - truncated.");
    return -1;
  }

  Global::outstandingScannerId = scanResult.id;

  msgPtr = scanResult.data;
  endPtr = scanResult.data + scanResult.dataLen;
  
  while (msgPtr < endPtr) {
    key = (ByteString32T *)msgPtr;
    msgPtr += 4 + key->len;
    value = (ByteString32T *)msgPtr;
    msgPtr += 4 + value->len;
    DisplayScanData(key, value, Global::outstandingSchemaPtr);
  }

  if ((scanResult.flags & END_OF_SCAN) == END_OF_SCAN)
    Global::outstandingScannerId = -1;

  return Error::OK;
}
