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
  size_t remaining = mEventPtr->messageLen - sizeof(int16_t);
  uint8_t *msgPtr = mEventPtr->message + sizeof(int16_t);

  // table name
  if (CommBuf::DecodeString(msgPtr, remaining, &tableName) == 0) {
    std::string errMsg = "Encoding problem with Create Table message";
    LOG_ERROR("Encoding problem with Create Table message");
    cb.error(Error::PROTOCOL_ERROR, errMsg);
  }

  mMaster->GetSchema(&cb, tableName);

}
