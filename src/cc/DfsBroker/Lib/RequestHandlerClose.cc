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

#include "RequestHandlerClose.h"

using namespace hypertable;
using namespace hypertable::DfsBroker;

/**
 *
 */
void RequestHandlerClose::run() {
  ResponseCallback cb(mComm, mEventPtr);
  uint32_t fd;
  size_t skip;
  size_t remaining = mEventPtr->messageLen - sizeof(int16_t);
  uint8_t *msgPtr = mEventPtr->message + sizeof(int16_t);
  int error;

  if (remaining < 4)
    goto abort;

  memcpy(&fd, msgPtr, 4);

  mBroker->Close(&cb, fd);

  return;

 abort:
  LOG_ERROR("Encoding problem with CLOSE message");
  cb.error(Error::PROTOCOL_ERROR, "Encoding problem with CLOSE message");
  return;
}
