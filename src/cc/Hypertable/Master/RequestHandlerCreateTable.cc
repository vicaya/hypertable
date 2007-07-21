/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
  ResponseCallback cb(mComm, mEvent);
  const char *tableName;
  const char *schemaString;
  size_t skip;
  size_t remaining = mEvent.messageLen - sizeof(int16_t);
  uint8_t *msgPtr = mEvent.message + sizeof(int16_t);
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
