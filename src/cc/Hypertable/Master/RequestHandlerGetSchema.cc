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

#include "AsyncComm/Comm.h"

#include "Master.h"
#include "RequestHandlerGetSchema.h"
#include "ResponseCallbackGetSchema.h"

using namespace hypertable;

/**
 *
 */
void RequestHandlerGetSchema::run() {
  ResponseCallbackGetSchema cb(mComm, mEvent);
  const char *tableName;
  size_t remaining = mEvent.messageLen - sizeof(int16_t);
  uint8_t *msgPtr = mEvent.message + sizeof(int16_t);

  // table name
  if (CommBuf::DecodeString(msgPtr, remaining, &tableName) == 0) {
    std::string errMsg = "Encoding problem with Create Table message";
    LOG_ERROR("Encoding problem with Create Table message");
    cb.error(Error::PROTOCOL_ERROR, errMsg);
  }

  mMaster->GetSchema(&cb, tableName);

}
