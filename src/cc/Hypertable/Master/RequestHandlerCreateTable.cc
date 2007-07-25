/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * This program is distributed in the hope that it will be useful,
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

#include "Master.h"
#include "RequestHandlerCreateTable.h"

using namespace hypertable;

/**
 *
 */
void RequestHandlerCreateTable::run() {
  ResponseCallback cb(mComm, mEventPtr);
  const char *tableName;
  const char *schemaString;
  size_t skip;
  size_t remaining = mEventPtr->messageLen - sizeof(int16_t);
  uint8_t *msgPtr = mEventPtr->message + sizeof(int16_t);
  std::string errMsg;

  // table name
  if ((skip = CommBuf::DecodeString(msgPtr, remaining, &tableName)) == 0)
    goto abort;

  msgPtr += skip;
  remaining -= skip;

  // schema string
  if ((skip = CommBuf::DecodeString(msgPtr, remaining, &schemaString)) == 0)
    goto abort;

  mMaster->CreateTable(&cb, tableName, schemaString);

  return;

 abort:
  LOG_ERROR("Encoding problem with Create Table message");
  errMsg = "Encoding problem with Create Table message";
  cb.error(Error::PROTOCOL_ERROR, errMsg);
  return;
}
