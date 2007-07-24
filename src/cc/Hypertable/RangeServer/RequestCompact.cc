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

#include <boost/shared_array.hpp>

#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/md5.h"

#include "AsyncComm/Event.h"
#include "AsyncComm/MessageBuilderSimple.h"
using namespace hypertable;

#include "Hypertable/Schema.h"
#include "MaintenanceThread.h"
#include "Global.h"
#include "RequestCompact.h"

using namespace hypertable;
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
void RequestCompact::run() {
  int32_t generation;
  std::string tableName;
  std::string tableFile;
  size_t skip;
  size_t remaining = mEvent.messageLen - sizeof(int16_t);
  uint8_t *msgPtr = mEvent.message + sizeof(int16_t);
  DynamicBuffer endRowBuffer(0);
  std::string errMsg;
  int error = Error::OK;
  CommBuf *cbuf;
  MessageBuilderSimple mbuilder;
  DynamicBuffer valueBuf(0);
  TableInfoPtr tableInfoPtr;
  char *ptr;
  string startRow;
  string endRow;
  string localityGroup;
  RangePtr rangePtr;
  RangeInfoPtr rangeInfoPtr;
  string tableHdfsDir;
  string rangeHdfsDir;
  uint64_t commitTime = 0;
  MaintenanceThread::WorkType compactionType = MaintenanceThread::COMPACTION_MINOR;
  bool major = false;

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
  tableFile = (std::string)"/hypertable/tables/" + tableName;
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

  // compaction type
  if (remaining == 0) {
    error = Error::PROTOCOL_ERROR;
    errMsg = "Compact packet truncated";
    goto abort;
  }
  if (*msgPtr == 1)
    major = true;
  msgPtr++;
  remaining--;

  // Locality Group
  if ((skip = CommBuf::DecodeString(msgPtr, remaining, (const char **)&ptr)) == 0) {
    error = Error::PROTOCOL_ERROR;
    errMsg = "Locality group not encoded properly in request packet";
    goto abort;
  }
  msgPtr += skip;
  remaining -= skip;
  if (*ptr == 0)
    localityGroup = "";
  else
    localityGroup = ptr;

  cout << "Table file = " << tableFile << endl;
  cout << "Compaction type = " << (major ? "major" : "minor") << endl;
  cout << "Locality group = \"" << localityGroup << "\"" << endl;
  cout << "Start row = \"" << startRow << "\"" << endl;
  cout << "End row = \"" << endRow << "\"" << endl;

  /**
   * Fetch table info
   */
  if (!Global::GetTableInfo(tableName, tableInfoPtr)) {
    error = Error::RANGESERVER_RANGE_NOT_FOUND;
    errMsg = "No ranges loaded for table '" + tableName + "'";
    goto abort;
  }

  /**
   * Fetch range info
   */
  if (!tableInfoPtr->GetRange(startRow, rangePtr)) {
    error = Error::RANGESERVER_RANGE_NOT_FOUND;
    errMsg = "No range with start row '" + startRow + "' found for table '" + tableName + "'";
    goto abort;
  }

  {
    boost::read_write_mutex::scoped_write_lock lock(Global::log->ReadWriteMutex());
    commitTime = Global::log->GetLastTimestamp();
  }

  if (major)
    compactionType = MaintenanceThread::COMPACTION_MAJOR;

  MaintenanceThread::ScheduleCompaction(rangePtr.get(), compactionType, commitTime, localityGroup);

#if 0
  if (localityGroup != "") {
    MaintenanceThread::ScheduleCompaction(lg, commitTime, major);
    lg = rangePtr->GetAccessGroup(localityGroup);
    MaintenanceThread::ScheduleCompaction(lg, commitTime, major);
  }
  else {
    vector<AccessGroup *> localityGroups = rangePtr->AccessGroupVector();
    for (size_t i=0; i< localityGroups.size(); i++)
      MaintenanceThread::ScheduleCompaction(localityGroups[i], commitTime, major);
  }
#endif

  error = Error::OK;

  /**
   * Send back response
   */
  cbuf = new CommBuf(mbuilder.HeaderLength() + 6);
  cbuf->PrependShort(RangeServerProtocol::COMMAND_COMPACT);
  cbuf->PrependInt(error);
  mbuilder.LoadFromMessage(mEvent.header);
  mbuilder.Encapsulate(cbuf);
  if ((error = Global::comm->SendResponse(mEvent.addr, cbuf)) != Error::OK) {
    LOG_VA_ERROR("Comm.SendResponse returned '%s'", Error::GetText(error));
  }

  if (Global::verbose) {
    if (localityGroup == "") {
      LOG_VA_INFO("Compaction (%s) scheduled for table '%s' start row '%s', all locality groups", 
		  (major ? "major" : "minor"), tableName.c_str(), startRow.c_str());
    }
    else {
      LOG_VA_INFO("Compaction (%s) scheduled for table '%s' start row '%s' locality group '%s'",
		  (major ? "major" : "minor"), tableName.c_str(), startRow.c_str(), localityGroup.c_str());
    }
  }

 abort:
  if (error != Error::OK) {
    if (Global::verbose) {
      LOG_VA_ERROR("%s '%s'", Error::GetText(error), errMsg.c_str());
    }
    cbuf = Global::protocol->CreateErrorMessage(RangeServerProtocol::COMMAND_COMPACT, error,
						errMsg.c_str(), mbuilder.HeaderLength());
    mbuilder.LoadFromMessage(mEvent.header);
    mbuilder.Encapsulate(cbuf);
    if ((error = Global::comm->SendResponse(mEvent.addr, cbuf)) != Error::OK) {
      LOG_VA_ERROR("Comm.SendResponse returned '%s'", Error::GetText(error));
    }
  }
  return;
}
