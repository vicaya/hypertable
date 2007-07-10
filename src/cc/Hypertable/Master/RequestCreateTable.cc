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
#include "RequestCreateTable.h"

using namespace hypertable;
using namespace hypertable;


void RequestCreateTable::run() {
  const char *tableName;
  const char *schemaString;
  size_t skip;
  size_t remaining = mEvent.messageLen - sizeof(int16_t);
  uint8_t *msgPtr = mEvent.message + sizeof(int16_t);
  CommBuf *cbuf = 0;
  MessageBuilderSimple mbuilder;
  int error = Error::OK;
  std::string finalSchema = "";
  std::string tableFile;
  std::string errMsg;
  string tableBaseDir;
  string lgDir;
  Schema *schema = 0;
  list<Schema::AccessGroup *> *lgList;

  if ((skip = CommBuf::DecodeString(msgPtr, remaining, &tableName)) == 0) {
    error = Error::PROTOCOL_ERROR;
    errMsg = "Table name not encoded properly in request packet";
    goto abort;
  }

  tableFile = (std::string)"/hypertable/tables/" + tableName;
  msgPtr += skip;

  if ((skip = CommBuf::DecodeString(msgPtr, remaining, &schemaString)) == 0) {
    error = Error::PROTOCOL_ERROR;
    errMsg = "Schema not encoded properly in request packet";
    goto abort;
  }

  /**
   * Check for table existence
   */
  if ((error = Global::hyperspace->Exists(tableFile.c_str())) == Error::OK) {
    error = Error::BTMASTER_TABLE_EXISTS;
    errMsg = tableName;
    goto abort;
  }
  else if (error != Error::HYPERTABLEFS_FILE_NOT_FOUND) {
    errMsg = (std::string)"Problem checking for existence of table file '" + tableFile + "'";
    goto abort;
  }

  /**
   *  Parse Schema and assign Generation number and Column ids
   */
  schema = Schema::NewInstance(schemaString, strlen(schemaString));
  if (!schema->IsValid()) {
    errMsg = schema->GetErrorString();
    goto abort;
  }
  schema->AssignIds();
  schema->Render(finalSchema);

  /**
   * Create table file
   */
  if ((error = Global::hyperspace->Create(tableFile.c_str())) != Error::OK) {
    errMsg = "Problem creating table file '" + tableFile + "'";
    goto abort;
  }


  /**
   * Write 'table_id' attribute of table file and 'last_table_id' attribute of /hypertable/meta
   */
  {
    char buf[32];
    int tableId = atomic_inc_return(&Global::lastTableId);
    sprintf(buf, "%d", tableId);
    if ((error = Global::hyperspace->AttrSet("/hypertable/meta", "last_table_id", buf)) != Error::OK) {
      errMsg = "Problem updating attribute 'last_table_id' of file '/hypertable/meta'";
      goto abort;
    }
    if ((error = Global::hyperspace->AttrSet(tableFile.c_str(), "table_id", buf)) != Error::OK) {
      errMsg = "Problem creating attribute 'table_id' for table file '" + tableFile + "'";
      goto abort;
    }
  }


  /**
   * Write schema attribute
   */
  if ((error = Global::hyperspace->AttrSet(tableFile.c_str(), "schema", finalSchema.c_str())) != Error::OK) {
    errMsg = "Problem creating attribute 'schema' for table file '" + tableFile + "'";
    goto abort;
  }

  /**
   * Create /hypertable/tables/<table>/<localityGroup> directories for this table in HDFS
   */
  tableBaseDir = (string)"/hypertable/tables/" + tableName + "/";
  lgList = schema->GetAccessGroupList();
  for (list<Schema::AccessGroup *>::iterator lgIter = lgList->begin(); lgIter != lgList->end(); lgIter++) {
    lgDir = tableBaseDir + (*lgIter)->name;
    if ((error = Global::hdfsClient->Mkdirs(lgDir.c_str())) != Error::OK) {
      errMsg = (string)"Problem creating table directory '" + lgDir + "'";
      goto abort;
    }
  }

  /**
   * Send back response
   */
  cbuf = new CommBuf(mbuilder.HeaderLength() + 6);
  cbuf->PrependShort(MasterProtocol::COMMAND_CREATE_TABLE);
  cbuf->PrependInt(error);
  mbuilder.LoadFromMessage(mEvent.header);
  mbuilder.Encapsulate(cbuf);
  if ((error = Global::comm->SendResponse(mEvent.addr, cbuf)) != Error::OK) {
    LOG_VA_ERROR("Comm.SendResponse returned '%s'", Error::GetText(error));
  }
  delete cbuf;

  if (Global::verbose) {
    LOG_VA_INFO("Successfully created table '%s'", tableName);
  }

 abort:
  delete schema;
  if (error != Error::OK) {
    if (Global::verbose) {
      LOG_VA_ERROR("%s '%s'", Error::GetText(error), errMsg.c_str());
    }
    cbuf = Global::protocol->CreateErrorMessage(MasterProtocol::COMMAND_CREATE_TABLE, error,
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
