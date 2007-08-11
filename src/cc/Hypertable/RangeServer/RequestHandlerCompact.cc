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

#include "RangeServer.h"
#include "Request.h"
#include "RequestHandlerCompact.h"

using namespace hypertable;

/**
 *
 */
void RequestHandlerCompact::run() {
  ResponseCallback cb(mComm, mEventPtr);
  RangeSpecificationT rangeSpec;
  uint8_t compactionType = 0;
  size_t skip;
  size_t remaining = mEventPtr->messageLen - sizeof(int16_t);
  uint8_t *msgPtr = mEventPtr->message + sizeof(int16_t);
  std::string errMsg;

  /**
   * Deserialize Range Specification
   */
  if ((skip = DeserializeRangeSpecification(msgPtr, remaining, &rangeSpec)) == 0)
    goto abort;

  msgPtr += skip;
  remaining -= skip;
  if (remaining == 0)
    goto abort;

  /**
   * Deserialize Compaction Type
   */
  compactionType = *msgPtr++;

  mRangeServer->Compact(&cb, &rangeSpec, compactionType);

  return;

 abort:
  LOG_ERROR("Encoding problem with Compact message");
  errMsg = "Encoding problem with Compact message";
  cb.error(Error::PROTOCOL_ERROR, errMsg);
  return;
}
