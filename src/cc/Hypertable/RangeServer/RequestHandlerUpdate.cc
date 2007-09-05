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

#include "Hypertable/Lib/Types.h"

#include "RangeServer.h"
#include "RequestHandlerUpdate.h"

using namespace hypertable;

/**
 *
 */
void RequestHandlerUpdate::run() {
  ResponseCallbackUpdate cb(mComm, mEventPtr);
  RangeSpecificationT rangeSpec;
  size_t remaining = mEventPtr->messageLen - 2;
  uint8_t *msgPtr = mEventPtr->message + 2;
  BufferT mods;

  // Range Specification
  if (!DecodeRangeSpecification(&msgPtr, &remaining, &rangeSpec))
    goto abort;

  cerr << "Succesfully decoded range spec." << endl;

  // Modifications
  if (!Serialization::DecodeByteArray(&msgPtr, &remaining, &mods.buf, &mods.len))
    goto abort;

  mRangeServer->Update(&cb, &rangeSpec, mods);

  return;

 abort:
  LOG_ERROR("Encoding problem with Update message");
  cb.error(Error::PROTOCOL_ERROR, "Encoding problem with Update message");
  return;
}
