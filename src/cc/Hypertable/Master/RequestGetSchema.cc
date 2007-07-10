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


#include <iostream>

#include "Common/Error.h"
#include "Common/Logger.h"

#include "AsyncComm/MessageBuilderSimple.h"
using namespace hypertable;

#include "Hypertable/Schema.h"
#include "Global.h"
#include "RequestGetSchema.h"

using namespace hypertable;

void RequestGetSchema::run() {
  const char *tableName;
  size_t remaining = mEvent.messageLen - sizeof(int16_t);
  uint8_t *msgPtr = mEvent.message + sizeof(int16_t);
  CommBuf *cbuf = 0;
  MessageBuilderSimple mbuilder;
  int error = Error::OK;
  std::string tableFile;
  std::string errMsg;
  DynamicBuffer schemaBuf(0);

  if (CommBuf::DecodeString(msgPtr, remaining, &tableName) == 0) {
    error = Error::PROTOCOL_ERROR;
    errMsg = "Table name not encoded properly in request packet";
    goto abort;
  }

  tableFile = (std::string)"/hypertable/tables/" + tableName;

  /**
   * Check for table existence
   */
  if ((error = Global::hyperspace->Exists(tableFile.c_str())) != Error::OK) {
    errMsg = tableName;
    goto abort;
  }

  /**
   * Get schema attribute
   */
  if ((error = Global::hyperspace->AttrGet(tableFile.c_str(), "schema", schemaBuf)) != Error::OK) {
    errMsg = "Problem getting attribute 'schema' for table file '" + tableFile + "'";
    goto abort;
  }

  /**
   * Send back response
   */
  cbuf = new CommBuf(mbuilder.HeaderLength() + 6 + CommBuf::EncodedLength((char *)schemaBuf.buf));
  cbuf->PrependString((char *)schemaBuf.buf);
  cbuf->PrependShort(MasterProtocol::COMMAND_GET_SCHEMA);
  cbuf->PrependInt(error);
  mbuilder.LoadFromMessage(mEvent.header);
  mbuilder.Encapsulate(cbuf);
  if ((error = Global::comm->SendResponse(mEvent.addr, cbuf)) != Error::OK) {
    LOG_VA_ERROR("Comm.SendResponse returned '%s'", Error::GetText(error));
  }
  delete cbuf;

  if (Global::verbose) {
    LOG_VA_INFO("Successfully fetched schema (length=%d) for table '%s'", strlen((char *)schemaBuf.buf), tableName);
  }

 abort:
  if (error != Error::OK) {
    if (Global::verbose) {
      LOG_VA_ERROR("%s '%s'", Error::GetText(error), errMsg.c_str());
    }
    cbuf = Global::protocol->CreateErrorMessage(MasterProtocol::COMMAND_GET_SCHEMA, error,
						errMsg.c_str(), mbuilder.HeaderLength());
    mbuilder.LoadFromMessage(mEvent.header);
    mbuilder.Encapsulate(cbuf);
    if ((error = Global::comm->SendResponse(mEvent.addr, cbuf)) != Error::OK) {
      LOG_VA_ERROR("Comm.SendResponse returned '%s'", Error::GetText(error));
    }
    delete cbuf;
  }
  return;
}
