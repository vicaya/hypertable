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

#include "AsyncComm/Protocol.h"
#include "AsyncComm/Serialization.h"

#include "ScanResult.h"

using namespace hypertable;

/**
 *
 */
int ScanResult::Load(EventPtr &eventPtr) {
  uint8_t *msgPtr = eventPtr->message + 4;
  uint8_t *endPtr;
  size_t remaining = eventPtr->messageLen - 4;
  uint32_t len;
  ByteString32T *key, *value;

  mEventPtr = eventPtr;

  if ((mError = (int)Protocol::ResponseCode(eventPtr)) != Error::OK)
    return mError;

  if (!(Serialization::DecodeShort(&msgPtr, &remaining, &mFlags)))
    return false;

  if (!(Serialization::DecodeInt(&msgPtr, &remaining, (uint32_t *)&mId)))
    return false;

  if (!(Serialization::DecodeInt(&msgPtr, &remaining, &len)))
    return false;

  endPtr = msgPtr + len;

  mVec.clear();

  while (msgPtr < endPtr) {
    key = (ByteString32T *)msgPtr;
    msgPtr += 4 + key->len;
    value = (ByteString32T *)msgPtr;
    msgPtr += 4 + value->len;
    mVec.push_back(std::make_pair(key, value));
  }
  
  return mError;
}
