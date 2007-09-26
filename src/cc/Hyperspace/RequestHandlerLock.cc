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

#include "Master.h"
#include "RequestHandlerLock.h"
#include "ResponseCallbackLock.h"

using namespace Hyperspace;
using namespace hypertable;

/**
 *
 */
void RequestHandlerLock::run() {
  ResponseCallbackLock cb(mComm, mEventPtr);
  size_t remaining = mEventPtr->messageLen - 2;
  uint8_t *msgPtr = mEventPtr->message + 2;
  const char *name;
  uint64_t handle;
  uint32_t mode;
  uint8_t tryAcquire;

  // handle
  if (!Serialization::DecodeLong(&msgPtr, &remaining, &handle))
    goto abort;

  // mode
  if (!Serialization::DecodeInt(&msgPtr, &remaining, &mode))
    goto abort;

  // tryAcquire
  if (!Serialization::DecodeByte(&msgPtr, &remaining, &tryAcquire))
    goto abort;

  mMaster->Lock(&cb, mSessionId, handle, mode, (bool)tryAcquire);

  return;

 abort:
  LOG_ERROR("Encoding problem with LOCK message");
  cb.error(Error::PROTOCOL_ERROR, "Encoding problem with LOCK message");
}
