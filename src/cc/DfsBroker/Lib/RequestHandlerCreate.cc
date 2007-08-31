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

#include "Common/Error.h"
#include "Common/Logger.h"

#include "AsyncComm/ResponseCallback.h"
#include "AsyncComm/Serialization.h"

#include "RequestHandlerCreate.h"

using namespace hypertable;
using namespace hypertable::DfsBroker;

/**
 *
 */
void RequestHandlerCreate::run() {
  ResponseCallbackOpen cb(mComm, mEventPtr);
  const char *fileName;
  size_t remaining = mEventPtr->messageLen - 2;
  uint8_t *msgPtr = mEventPtr->message + 2;
  uint16_t sval, replication;
  uint32_t ival, bufferSize;
  uint64_t blockSize;
  bool overwrite;

  if (remaining < 18)
    goto abort;

  // overwrite flag
  Serialization::DecodeShort(&msgPtr, &remaining, &sval);
  overwrite = (sval == 0) ? false : true;

  // replication
  Serialization::DecodeInt(&msgPtr, &remaining, &ival);
  replication = (short)ival;

  // buffer size
  Serialization::DecodeInt(&msgPtr, &remaining, &bufferSize);

  // block size
  Serialization::DecodeLong(&msgPtr, &remaining, &blockSize);

  // file name
  if (!Serialization::DecodeString(&msgPtr, &remaining, &fileName))
    goto abort;

  // validate filename
  if (fileName[strlen(fileName)-1] == '/') {
    LOG_VA_ERROR("open failed: bad filename - %s", fileName);
    cb.error(Error::DFSBROKER_BAD_FILENAME, fileName);
    return;
  }

  mBroker->Create(&cb, fileName, overwrite, bufferSize, replication, blockSize);

  return;

 abort:
  LOG_ERROR("Encoding problem with CREATE message");
  cb.error(Error::PROTOCOL_ERROR, "Encoding problem with CREATE message");
  return;
}
