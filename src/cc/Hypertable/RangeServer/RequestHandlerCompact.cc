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
  TabletIdentifierT tablet;
  uint8_t compactionType = 0;
  const char *accessGroup = 0;
  size_t skip;
  size_t remaining = mEventPtr->messageLen - sizeof(int16_t);
  uint8_t *msgPtr = mEventPtr->message + sizeof(int16_t);
  std::string errMsg;

  /**
   * Deserialize Tablet Identifier
   */
  if ((skip = DeserializeTabletIdentifier(msgPtr, remaining, &tablet)) == 0)
    goto abort;

  msgPtr += skip;
  remaining -= skip;
  if (remaining == 0)
    goto abort;

  /**
   * Deserialize Compaction Type
   */
  compactionType = *msgPtr++;
  remaining--;

  /**
   * Deserialize Access Group
   */
  if ((skip = CommBuf::DecodeString(msgPtr, remaining, (const char **)&accessGroup)) == 0)
    goto abort;

  if (*accessGroup == 0)
    accessGroup = 0;

  mRangeServer->Compact(&cb, &tablet, compactionType, accessGroup);

  return;

 abort:
  LOG_ERROR("Encoding problem with Compact message");
  errMsg = "Encoding problem with Compact message";
  cb.error(Error::PROTOCOL_ERROR, errMsg);
  return;
}
