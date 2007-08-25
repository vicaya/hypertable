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

#include "RequestHandlerCreate.h"

using namespace hypertable;
using namespace hypertable::DfsBroker;

/**
 *
 */
void RequestHandlerCreate::run() {
  ResponseCallbackOpen cb(mComm, mEventPtr);
  const char *fileName;
  size_t skip;
  size_t remaining = mEventPtr->messageLen - sizeof(int16_t);
  uint8_t *msgPtr = mEventPtr->message + sizeof(int16_t);
  int error;
  uint16_t sval, replication;
  uint32_t ival, bufferSize;
  uint64_t blockSize;
  bool overwrite;

  if (remaining < 18)
    goto abort;

  // overwrite flag
  memcpy(&sval, msgPtr, 2);
  msgPtr += 2;
  remaining -= 2;
  overwrite = (sval == 0) ? false : true;

  // replication
  memcpy(&ival, msgPtr, 4);
  msgPtr += 4;
  remaining -= 4;
  replication = (short)ival;

  // buffer size
  memcpy(&bufferSize, msgPtr, 4);
  msgPtr += 4;
  remaining -= 4;

  // block size
  memcpy(&blockSize, msgPtr, 8);
  msgPtr += 8;
  remaining -= 8;

  // file name
  if ((skip = CommBuf::DecodeString(msgPtr, remaining, &fileName)) == 0)
    goto abort;

  mBroker->Create(&cb, fileName, overwrite, bufferSize, replication, blockSize);

  return;

 abort:
  LOG_ERROR("Encoding problem with CREATE message");
  cb.error(Error::PROTOCOL_ERROR, "Encoding problem with CREATE message");
  return;
}
