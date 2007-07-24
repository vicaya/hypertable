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
#include <set>

#include <boost/shared_array.hpp>

#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/StringExt.h"

#include "AsyncComm/Event.h"
#include "AsyncComm/MessageBuilderSimple.h"
using namespace hypertable;

#include "Hypertable/Schema.h"
#include "FillScanBlock.h"
#include "Global.h"
#include "AccessGroup.h"
#include "MergeScanner.h"
#include "RequestCreateScanner.h"

using namespace hypertable;
using namespace std;

namespace {
  const int DEFAULT_SCANBUF_SIZE = 32768;
}

/**
 *
 */
void RequestCreateScanner::run() {
  string tableName;
  int32_t generation;
  size_t skip;
  size_t remaining = mEvent.messageLen - sizeof(int16_t);
  uint8_t *msgPtr = mEvent.message + sizeof(int16_t);
  string errMsg;
  int error = Error::OK;
  CommBuf *cbuf;
  MessageBuilderSimple mbuilder;
  TableInfoPtr tableInfoPtr;
  RangePtr rangePtr;
  KeyT          *startKey = 0;
  KeyT          *endKey = 0;
  uint64_t       startTime, endTime;
  string rowkey;
  string startRow;
  string endRow;
  const char *ptr;
  const uint8_t *columns;
  int32_t  columnCount;
  std::set<uint8_t> columnFamilies;
  uint8_t *kvBuffer = 0;
  uint32_t *kvLenp = 0;
  bool more = true;
  uint32_t id;
  CellListScannerPtr scannerPtr;

  /**
   * Decode parameters
   */

  /** Generation **/
  if (remaining < sizeof(int32_t)) {
    error = Error::PROTOCOL_ERROR;
    errMsg = "Unable to decode table generation, short request";
    goto abort;
  }
  memcpy(&generation, msgPtr, sizeof(int32_t));
  msgPtr += sizeof(int32_t);
  remaining -= sizeof(int32_t);

  /** Table name **/
  if ((skip = CommBuf::DecodeString(msgPtr, remaining, tableName)) == 0) {
    error = Error::PROTOCOL_ERROR;
    errMsg = "Table name not encoded properly in request packet";
    goto abort;
  }
  msgPtr += skip;
  remaining -= skip;

  /** Start row **/
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

  /** End row **/
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

  /** Columns **/
  if ((skip = CommBuf::DecodeByteArray(msgPtr, remaining, &columns, &columnCount)) == 0) {
    error = Error::PROTOCOL_ERROR;
    errMsg = "Column vector not encoded properly in CREATE SCANNER packet";
    goto abort;
  }
  msgPtr += skip;
  remaining -= skip;
  for (int32_t i=0; i<columnCount; i++)
    columnFamilies.insert(columns[i]);

  /** Start Key **/
  if ((skip = CommBuf::DecodeString(msgPtr, remaining, (const char **)&ptr)) == 0) {
    error = Error::PROTOCOL_ERROR;
    errMsg = "Range start row not encoded properly in request packet";
    goto abort;
  }
  if (*ptr != 0) {
    startKey = (KeyT *)new uint8_t [ sizeof(int32_t) + strlen(ptr) ];
    startKey->len = strlen(ptr);
    memcpy(startKey->data, ptr, startKey->len);
  }
  msgPtr += skip;
  remaining -= skip;

  /** End Key **/
  if ((skip = CommBuf::DecodeString(msgPtr, remaining, (const char **)&ptr)) == 0) {
    error = Error::PROTOCOL_ERROR;
    errMsg = "Range end row not encoded properly in request packet";
    goto abort;
  }
  if (*ptr != 0) {
    endKey = (KeyT *)new uint8_t [ sizeof(int32_t) + strlen(ptr) ];
    endKey->len = strlen(ptr);
    memcpy(endKey->data, ptr, endKey->len);
  }
  msgPtr += skip;
  remaining -= skip;

  /** Start time / End time **/
  if (remaining < 2*sizeof(uint64_t)) {
    error = Error::PROTOCOL_ERROR;
    errMsg = "Time interval not encoded properly in request packet";
    goto abort;
  }

  memcpy(&startTime, msgPtr, sizeof(int64_t));
  msgPtr += sizeof(int64_t);
  memcpy(&endTime, msgPtr, sizeof(int64_t));

  cout << "Table name = " << tableName << endl;
  cout << "Start row  = " << startRow << endl;
  cout << "End row  = " << endRow << endl;
  cout << "Table generation = " << generation << endl;
  for (int32_t i=0; i<columnCount; i++) {
    cout << "Column = " << (int)columns[i] << endl;
  }
  if (startKey != 0) {
    rowkey = string((const char *)startKey->data, startKey->len);
    cout << "Start Key = " << rowkey << endl;
  }
  else
    cout << "Start Key = \"\"" << endl;

  if (endKey != 0) {
    rowkey = string((const char *)endKey->data, endKey->len);
    cout << "End Key = " << rowkey << endl;
  }
  else
    cout << "End Key = \"\"" << endl;
  cout << "Start Time = " << startTime << endl;
  cout << "End Time = " << endTime << endl;

  if (!Global::GetTableInfo(tableName, tableInfoPtr)) {
    error = Error::RANGESERVER_RANGE_NOT_FOUND;
    errMsg = "No ranges for table '" + tableName + "' loaded";
    goto abort;
  }

  if (!tableInfoPtr->GetRange(startRow, rangePtr)) {
    error = Error::RANGESERVER_RANGE_NOT_FOUND;
    errMsg = startRow;
    goto abort;
  }

  kvBuffer = new uint8_t [ sizeof(int32_t) + DEFAULT_SCANBUF_SIZE ];
  kvLenp = (uint32_t *)kvBuffer;

  cerr << "About to scan ..." << endl;

  rangePtr->LockShareable();
  if (columnFamilies.size() > 0) 
    scannerPtr.reset( rangePtr->CreateScanner(columnFamilies, true) );
  else
    scannerPtr.reset( rangePtr->CreateScanner(true) );
  scannerPtr->RestrictRange(startKey, endKey);
  if (columnCount > 0)
    scannerPtr->RestrictColumns(columns, columnCount);
  scannerPtr->Reset();
  more = FillScanBlock(scannerPtr, kvBuffer+sizeof(int32_t), DEFAULT_SCANBUF_SIZE, kvLenp);
  if (more)
    id = Global::scannerMap.Put(scannerPtr, rangePtr);

  rangePtr->UnlockShareable();

  cerr << "Finished to scan ..." << endl;

  /**
   *  Send back data
   */

  cbuf = new CommBuf(mbuilder.HeaderLength() + 12);
  cbuf->SetExt(kvBuffer, sizeof(int32_t) + *kvLenp);
  cbuf->PrependInt(id);   // scanner ID
  if (more)
    cbuf->PrependShort(0);
  else
    cbuf->PrependShort(1);
  cbuf->PrependShort(RangeServerProtocol::COMMAND_CREATE_SCANNER);
  cbuf->PrependInt(Error::OK);
  mbuilder.LoadFromMessage(mEvent.header);
  mbuilder.Encapsulate(cbuf);
  if ((error = Global::comm->SendResponse(mEvent.addr, cbuf)) != Error::OK) {
    LOG_VA_ERROR("Comm.SendResponse returned '%s'", Error::GetText(error));
  }

  error = Error::OK;

  if (Global::verbose) {
    LOG_VA_INFO("Successfully created scanner on table '%s'", tableName.c_str());
  }

 abort:
  if (error != Error::OK) {
    if (Global::verbose) {
      LOG_VA_ERROR("%s '%s'", Error::GetText(error), errMsg.c_str());
    }
    cbuf = Global::protocol->CreateErrorMessage(RangeServerProtocol::COMMAND_CREATE_SCANNER, error,
						errMsg.c_str(), mbuilder.HeaderLength());
    mbuilder.LoadFromMessage(mEvent.header);
    mbuilder.Encapsulate(cbuf);
    if ((error = Global::comm->SendResponse(mEvent.addr, cbuf)) != Error::OK) {
      LOG_VA_ERROR("Comm.SendResponse returned '%s'", Error::GetText(error));
    }
  }
  return;
}

