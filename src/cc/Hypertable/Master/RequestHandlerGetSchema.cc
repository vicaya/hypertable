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

#include "AsyncComm/Comm.h"
#include "AsyncComm/Serialization.h"

#include "Master.h"
#include "RequestHandlerGetSchema.h"
#include "ResponseCallbackGetSchema.h"

using namespace hypertable;

/**
 *
 */
void RequestHandlerGetSchema::run() {
  ResponseCallbackGetSchema cb(mComm, mEventPtr);
  const char *tableName;
  size_t remaining = mEventPtr->messageLen - 2;
  uint8_t *msgPtr = mEventPtr->message + 2;

  // table name
  if (!Serialization::DecodeString(&msgPtr, &remaining, &tableName)) {
    LOG_ERROR("Encoding problem with Create Table message");
    cb.error(Error::PROTOCOL_ERROR, "Encoding problem with Create Table message");
    return;
  }

  mMaster->GetSchema(&cb, tableName);
}
