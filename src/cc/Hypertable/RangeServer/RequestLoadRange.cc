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

#include <iostream>
#include <string>

#include <boost/shared_array.hpp>

#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/md5.h"

#include "AsyncComm/Event.h"
#include "AsyncComm/MessageBuilderSimple.h"
using namespace hypertable;

#include "Hypertable/Schema.h"
#include "Global.h"
#include "RequestLoadRange.h"
#include "VerifySchema.h"

using namespace hypertable;
using namespace std;

/**
 * - Decode parameters (tableName, generation, startRow, endRow)
 * - Make sure we have up-to-date table information (e.g. schema), if not, reload
 * - Check to see if range is already loaded, if so return error
 * - Fetch metadata for range
 * - Create Range
 * - Replay commit log for range (insided Range.cc) (at what point does commit log get deleted?)
 *
 * Level 1 METADATA entries:
 * "Webtable http://www.foo.com/"
 * "UserSession 2348234ASSFD345DFS"
 *
 * Level 0 METADATA entreis:
 * " Webtable http://www.foo.com/
 * " UserSession 2348234ASSFD345DFS"
 * ...
 * "!"
 *
 *  3. If we're the root range, then the end row will be "0 0 " so we either read the
 *     Range info from a well-known location in Chubby, or just 'readdir' in the
 *     Range directory /hypertable/tables/METADATA/...
 */
void RequestLoadRange::run() {
  std::string tableName;
  int32_t generation;
  size_t skip;
  size_t remaining = mEvent.messageLen - sizeof(int16_t);
  uint8_t *msgPtr = mEvent.message + sizeof(int16_t);
  DynamicBuffer endRowBuffer(0);
  std::string errMsg;
  int error = Error::OK;
  CommBuf *cbuf;
  MessageBuilderSimple mbuilder;
  SchemaPtr schemaPtr;
  TableInfoPtr tableInfoPtr;
  char *ptr;
  string startRow;
  string endRow;
  RangePtr rangePtr;
  RangeInfoPtr rangeInfoPtr;
  string tableHdfsDir;
  string rangeHdfsDir;
  char md5DigestStr[33];
  bool registerTable = false;

  /**
   * Decode parameters
   */

  // generation
  if (remaining < sizeof(int32_t)) {
    error = Error::PROTOCOL_ERROR;
    errMsg = "Unable to decode table generation, short request";
    goto abort;
  }
  memcpy(&generation, msgPtr, sizeof(int32_t));
  msgPtr += sizeof(int32_t);
  remaining -= sizeof(int32_t);

  // table name
  if ((skip = CommBuf::DecodeString(msgPtr, remaining, tableName)) == 0) {
    error = Error::PROTOCOL_ERROR;
    errMsg = "Table name not encoded properly in request packet";
    goto abort;
  }
  msgPtr += skip;
  remaining -= skip;

  // Start row
  if ((skip = CommBuf::DecodeString(msgPtr, remaining, (const char **)&ptr)) == 0) {
    error = Error::PROTOCOL_ERROR;
    errMsg = "Range start row not encoded properly in request packet";
    goto abort;
  }
  msgPtr += skip;
  remaining -= skip;
  if (*ptr == 0)
    startRow = "";
  else
    startRow = ptr;

  // End row
  if ((skip = CommBuf::DecodeString(msgPtr, remaining, (const char **)&ptr)) == 0) {
    error = Error::PROTOCOL_ERROR;
    errMsg = "Range end row not encoded properly in request packet";
    goto abort;
  }
  msgPtr += skip;
  remaining -= skip;
  if (*ptr == 0)
    endRow = "";
  else
    endRow = ptr;

  //cout << "Table file = " << tableFile << endl;
  cout << "Table generation = " << generation << endl;
  cout << "Start row = \"" << startRow << "\"" << endl;
  cout << "End row = \"" << endRow << "\"" << endl;

  if (!Global::GetTableInfo(tableName, tableInfoPtr)) {
    tableInfoPtr.reset( new TableInfo(tableName, schemaPtr) );
    registerTable = true;
  }

  if ((error = VerifySchema(tableInfoPtr, generation, errMsg)) != Error::OK)
    goto abort;

  if (registerTable)
    Global::SetTableInfo(tableName, tableInfoPtr);

  schemaPtr = tableInfoPtr->GetSchema();

  /**
   * Check for existence of and create, if necessary, range directory (md5 of endrow)
   * under all locality group directories for this table
   */
  {
    if (startRow == "")
      memset(md5DigestStr, '0', 24);
    else
      md5_string(startRow.c_str(), md5DigestStr);
    md5DigestStr[24] = 0;
    tableHdfsDir = (string)"/hypertable/tables/" + tableName;
    list<Schema::AccessGroup *> *lgList = schemaPtr->GetAccessGroupList();
    for (list<Schema::AccessGroup *>::iterator lgIter = lgList->begin(); lgIter != lgList->end(); lgIter++) {
      // notice the below variables are different "range" vs. "table"
      rangeHdfsDir = tableHdfsDir + "/" + (*lgIter)->name + "/" + md5DigestStr;
      if ((error = Global::hdfsClient->Mkdirs(rangeHdfsDir.c_str())) != Error::OK) {
	errMsg = (string)"Problem creating range directory '" + rangeHdfsDir + "'";
	goto abort;
      }
    }
  }

  if (tableInfoPtr->GetRange(startRow, rangePtr)) {
    error = Error::RANGESERVER_RANGE_ALREADY_LOADED;
    errMsg = startRow;
    goto abort;
  }

  if ((error = Global::metadata->GetRangeInfo(tableInfoPtr->GetName(), startRow, endRow, rangeInfoPtr)) != Error::OK) {
    errMsg = startRow;
    goto abort;
  }

  tableInfoPtr->AddRange(rangeInfoPtr);

  rangeInfoPtr->SetLogDir(Global::log->GetLogDir());
  Global::metadata->Sync();

  error = Error::OK;

  /**
   * Send back response
   */
  cbuf = new CommBuf(mbuilder.HeaderLength() + 6);
  cbuf->PrependShort(RangeServerProtocol::COMMAND_LOAD_RANGE);
  cbuf->PrependInt(error);
  mbuilder.LoadFromMessage(mEvent.header);
  mbuilder.Encapsulate(cbuf);
  if ((error = Global::comm->SendResponse(mEvent.addr, cbuf)) != Error::OK) {
    LOG_VA_ERROR("Comm.SendResponse returned '%s'", Error::GetText(error));
  }

  if (Global::verbose) {
    LOG_VA_INFO("Successfully loaded range table='%s' startRow='%s' endRow='%s'", 
		tableName.c_str(), startRow.c_str(), endRow.c_str());
  }

 abort:
  if (error != Error::OK) {
    if (Global::verbose) {
      LOG_VA_ERROR("%s '%s'", Error::GetText(error), errMsg.c_str());
    }
    cbuf = Global::protocol->CreateErrorMessage(RangeServerProtocol::COMMAND_LOAD_RANGE, error,
						errMsg.c_str(), mbuilder.HeaderLength());
    mbuilder.LoadFromMessage(mEvent.header);
    mbuilder.Encapsulate(cbuf);
    if ((error = Global::comm->SendResponse(mEvent.addr, cbuf)) != Error::OK) {
      LOG_VA_ERROR("Comm.SendResponse returned '%s'", Error::GetText(error));
    }
  }
  return;
}
