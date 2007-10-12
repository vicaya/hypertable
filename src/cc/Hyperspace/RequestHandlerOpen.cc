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

#include "Master.h"
#include "Protocol.h"
#include "RequestHandlerOpen.h"

using namespace Hyperspace;
using namespace hypertable;

/**
 *
 */
void RequestHandlerOpen::run() {
  ResponseCallbackOpen cb(mComm, mEventPtr);
  const char *name;
  uint32_t flags;
  uint32_t eventMask;  
  size_t remaining = mEventPtr->messageLen - 2;
  uint8_t *msgPtr = mEventPtr->message + 2;
  uint32_t attrCount;
  AttributeT attr;
  std::vector<AttributeT> initAttrs;

  // flags
  if (!Serialization::DecodeInt(&msgPtr, &remaining, &flags))
    goto abort;

  // event mask
  if (!Serialization::DecodeInt(&msgPtr, &remaining, &eventMask))
    goto abort;

  // directory name
  if (!Serialization::DecodeString(&msgPtr, &remaining, &name))
    goto abort;

  // initial attribute count
  if (!Serialization::DecodeInt(&msgPtr, &remaining, &attrCount))
    goto abort;

  // 
  for (uint32_t i=0; i<attrCount; i++) {
    if (!Serialization::DecodeString(&msgPtr, &remaining, &attr.name))
      goto abort;
    if (!Serialization::DecodeByteArray(&msgPtr, &remaining, (uint8_t **)&attr.value, &attr.valueLen))
      goto abort;
    initAttrs.push_back(attr);
  }

  mMaster->Open(&cb, mSessionId, name, flags, eventMask, initAttrs);

  return;

 abort:
  LOG_ERROR("Encoding problem with OPEN message");
  cb.error(Error::PROTOCOL_ERROR, "Encoding problem with OPEN message");
}
