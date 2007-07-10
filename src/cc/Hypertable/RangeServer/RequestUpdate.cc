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

#include <boost/shared_array.hpp>

#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/StringExt.h"

#include "AsyncComm/Event.h"
#include "AsyncComm/MessageBuilderSimple.h"

#include "Hypertable/Schema.h"
#include "MaintenanceThread.h"
#include "Global.h"
#include "AccessGroup.h"
#include "RequestUpdate.h"
#include "SplitLogMap.h"

using namespace hypertable;
using namespace std;

namespace {

  typedef struct {
    const uint8_t *base;
    size_t len;
    Range *range;
  } UpdateRecT;

}

/**
 *
 * NOTE: Add start row to request
 *
 *  1. Make sure we have up-to-date table information (table Id, schema, etc.), if not, reload
 *  2. Read Range info from the METADATA table (report split if detected)
 *  3. If we're the root range, then the end row will be "0 0 " so we either read the
 *     Range info from a well-known location in Chubby, or just 'readdir' in the
 *     Range directory /hypertable/tables/METADATA/...
 *  4. Create Range by passing to the constructor the following:
 *    - Schema
 *    - Range files
 *  6. Return
 */
void RequestUpdate::run() {
  string tableName;
  int32_t generation;
  size_t skip;
  size_t remaining = mEvent.messageLen - sizeof(int16_t);
  uint8_t *msgPtr = mEvent.message + sizeof(int16_t);
  const uint8_t *modData, *modEnd;
  int32_t modLen;
  string errMsg;
  int error = Error::OK;
  CommBuf *cbuf;
  MessageBuilderSimple mbuilder;
  string tableFile;
  TableInfoPtr tableInfoPtr;
  TableInfo *tableInfo = 0;
  const uint8_t *modPtr;
  char *ptr;
  string startRow;
  string endRow;
  string rowkey;
  vector<UpdateRecT> goMods;
  vector<UpdateRecT> stopMods;
  vector<UpdateRecT> splitMods;
  RangePtr rangePtr;
  UpdateRecT update;
  size_t goSize = 0;
  size_t stopSize = 0;
  size_t splitSize = 0;
  set<Range *> rangeSet;
  KeyComponentsT keyComps;
  uint64_t commitTime = 0;
  SplitLogMap::SplitLogInfoPtr splitLogInfoPtr;

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
  tableFile = (string)"/hypertable/tables/" + tableName;
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

  // mods
  if ((skip = CommBuf::DecodeByteArray(msgPtr, remaining, &modData, &modLen)) == 0) {
    error = Error::PROTOCOL_ERROR;
    errMsg = "Updates not encoded properly";
    goto abort;
  }

  // TODO: Sanity check mod data (checksum validation)

  cout << "Table file = " << tableFile << endl;
  cout << "Table generation = " << generation << endl;
  cout << "Start row = \"" << startRow << "\"" << endl;
  cout << "End row = \"" << endRow << "\"" << endl;

  /**
   * Fetch table info
   */
  if (Global::GetTableInfo(tableName, tableInfoPtr))
    tableInfo = tableInfoPtr.get();
  else {
    SendBackInvalidUpdates(modData, modLen);
    LOG_VA_ERROR("Unable to find table info for table '%s'", tableName.c_str());
    return;
  }

  /**
   * Fetch range
   */
  if (!tableInfoPtr->GetRange(startRow, rangePtr)) {
    error = Error::RANGESERVER_RANGE_NOT_FOUND;
    errMsg = startRow;
    goto abort;
  }

  /**
   * We're locking the commit log here because we don't want the set of ranges
   * to change between when we figure out a destination range for each mutation
   * and when we actually apply the mutations to the ranges.
   *
   * Why is it a read lock?  Because ranges can't split while the commit log is read locked.
   * But writing to the commit log needs a write lock, so let's just do a write lock here
   * But we can change the CommitLog to be thread safe, so we should do so
   */
  {
    boost::read_write_mutex::scoped_write_lock lock(Global::log->ReadWriteMutex());

    /**
     * Figure out destination for each mutations
     */
    modEnd = modData + modLen;
    modPtr = modData;
    while (modPtr < modEnd) {
      if (!Load((KeyT *)modPtr, keyComps)) {
	error = Error::PROTOCOL_ERROR;
	errMsg = "Problem de-serializing key/value pair";
	goto abort;
      }
      if (commitTime < keyComps.timestamp)
	commitTime = keyComps.timestamp;
      rowkey = keyComps.rowKey;
      update.base = modPtr;
      modPtr = keyComps.endPtr + Length((const ByteString32T *)keyComps.endPtr);
      update.len = modPtr - update.base;

      if (Global::splitLogMap.Lookup(rowkey, splitLogInfoPtr)) {
	splitMods.push_back(update);
	splitSize += update.len;
      }
      else if (rowkey > rangePtr->StartRow() && (rangePtr->EndRow() == "" || rowkey <= rangePtr->EndRow())) {
	update.range = rangePtr.get();
	rangeSet.insert(update.range);
	goMods.push_back(update);
	goSize += update.len;
      }
      else {
	update.range = 0;
	stopMods.push_back(update);
	stopSize += update.len;
      }

      //cout << "KeyLen = " << keyLen << ", ValueLen = " << valueLen << endl;
    }

    /**
    cout << "goMods.size() = " << goMods.size() << endl;
    cout << "stopMods.size() = " << stopMods.size() << endl;
    **/

    if (splitSize > 0) {
      boost::shared_array<uint8_t> bufPtr( new uint8_t [ splitSize ] );
      uint8_t *base = bufPtr.get();
      uint8_t *ptr = base;
      
      for (size_t i=0; i<splitMods.size(); i++) {
	memcpy(ptr, splitMods[i].base, splitMods[i].len);
	ptr += splitMods[i].len;
      }

      if ((error = splitLogInfoPtr->splitLog->Write(tableName.c_str(), base, ptr-base, commitTime)) != Error::OK) {
	errMsg = (string)"Problem writing " + (int)(ptr-base) + " bytes to split log";
	goto abort;
      }
      //exit(1);
    }

    /**
     * Commit mutations that are destined for this range server
     */
    if (goSize > 0) {
      // free base!!!
      uint8_t *base = new uint8_t [ goSize ];
      uint8_t *ptr = base;
      for (size_t i=0; i<goMods.size(); i++) {
	memcpy(ptr, goMods[i].base, goMods[i].len);
	ptr += goMods[i].len;
      }
      if ((error = Global::log->Write(tableName.c_str(), base, ptr-base, commitTime)) != Error::OK) {
	errMsg = (string)"Problem writing " + (int)(ptr-base) + " bytes to commit log";
	goto abort;
      }
    }

    /**
     * Apply the updates
     */
    for (size_t i=0; i<goMods.size(); i++) {
      KeyT *key = (KeyT *)goMods[i].base;
      ByteString32T *value = (ByteString32T *)(goMods[i].base + Length(key));
      goMods[i].range->Add(key, value);
    }

  }

  /**
   * Split and Compaction processing
   */
  for (set<Range *>::iterator iter = rangeSet.begin(); iter != rangeSet.end(); iter++) {
    if ((*iter)->CheckAndSetMaintenance()) {
      MaintenanceThread::ScheduleMaintenance(*iter, commitTime);
    }
  }


  /**
   * Send back response
   */
  if (stopSize > 0) {
    uint8_t *base = new uint8_t [ stopSize ];
    uint8_t *ptr = base;
    for (size_t i=0; i<stopMods.size(); i++) {
      memcpy(ptr, stopMods[i].base, stopMods[i].len);
      ptr += stopMods[i].len;
    }
    SendBackInvalidUpdates(base, ptr-base);
  }
  else {
    cbuf = new CommBuf(mbuilder.HeaderLength() + 6);
    cbuf->PrependShort(RangeServerProtocol::COMMAND_UPDATE);
    cbuf->PrependInt(Error::OK);
    mbuilder.LoadFromMessage(mEvent.header);
    mbuilder.Encapsulate(cbuf);
    if ((error = Global::comm->SendResponse(mEvent.addr, cbuf)) != Error::OK) {
      LOG_VA_ERROR("Comm.SendResponse returned '%s'", Error::GetText(error));
    }
  }

  // Get table info
  // For each K/V pair
  //  - Get range
  //  - If OK, add to goBuf
  //  - If no range, add to stopBuf
  // If goBuf non-empty, add goBuf to commit log
  // If stopBuf non-empty, return Error::PARTIAL_UPDATE with stopBuf
  // otherwise, return Error::OK
  // Add goBuf K/V pairs to CellCaches
  // If range size exceeds threshold, schedule split
  // else if CellCache exceeds threshold, schedule compaction

  error = Error::OK;

  if (Global::verbose) {
    LOG_VA_INFO("Added %d (%d split off) and dropped %d updates to '%s'",
		goMods.size() + splitMods.size(), splitMods.size(), stopMods.size(), tableName.c_str());
  }

 abort:

  if (error != Error::OK) {
    if (Global::verbose) {
      LOG_VA_ERROR("%s '%s'", Error::GetText(error), errMsg.c_str());
    }
    cbuf = Global::protocol->CreateErrorMessage(RangeServerProtocol::COMMAND_UPDATE, error,
						errMsg.c_str(), mbuilder.HeaderLength());
    mbuilder.LoadFromMessage(mEvent.header);
    mbuilder.Encapsulate(cbuf);
    if ((error = Global::comm->SendResponse(mEvent.addr, cbuf)) != Error::OK) {
      LOG_VA_ERROR("Comm.SendResponse returned '%s'", Error::GetText(error));
    }
  }
  return;
}


void RequestUpdate::SendBackInvalidUpdates(const uint8_t *modData, int32_t modLen) {
  int error;
  MessageBuilderSimple mbuilder;
  CommBuf *cbuf = new CommBuf(mbuilder.HeaderLength() + 10 + modLen);
  cbuf->PrependByteArray(modData, modLen);
  cbuf->PrependShort(RangeServerProtocol::COMMAND_UPDATE);
  cbuf->PrependInt(Error::RANGESERVER_PARTIAL_UPDATE);
  mbuilder.LoadFromMessage(mEvent.header);
  mbuilder.Encapsulate(cbuf);
  if ((error = Global::comm->SendResponse(mEvent.addr, cbuf)) != Error::OK) {
    LOG_VA_ERROR("Comm.SendResponse returned '%s'", Error::GetText(error));
  }
  return;
}




