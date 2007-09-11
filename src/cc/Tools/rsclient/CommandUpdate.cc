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

#include <iostream>

extern "C" {
#include <sys/types.h>
#include <sys/stat.h>
}

#include "Common/Error.h"
#include "Common/Usage.h"

#include "AsyncComm/DispatchHandlerSynchronizer.h"
#include "AsyncComm/Event.h"

#include "Hypertable/Lib/TestData.h"
#include "Hypertable/Lib/TestSource.h"

#include "CommandUpdate.h"
#include "ParseRangeSpec.h"
#include "FetchSchema.h"
#include "Global.h"

#define BUFFER_SIZE 65536

using namespace hypertable;
using namespace std;

const char *CommandUpdate::msUsage[] = {
  "update <range> <datafile>",
  "",
  "  This command issues an UPDATE command to the range server.  The data",
  "  to load is contained, one update per line, in <datafile>.  Lines can",
  "  take one of the following forms:",
  "",
  "  <timestamp> '\t' <rowKey> '\t' <qualifiedColumn> '\t' <value>",
  "  <timestamp> '\t' <rowKey> '\t' <qualifiedColumn> '\t' DELETE",
  "  <timestamp> '\t' DELETE",
  "",
  "  The <timestamp> field can either contain a timestamp that is microseconds",
  "  since the epoch, or it can contain the word AUTO which will cause the",
  "  system to automatically assign a timestamp to each update.",
  "",
  (const char *)0
};


int CommandUpdate::run() {
  DispatchHandlerSynchronizer syncHandler;
  off_t len;
  const char *schema = 0;
  int error;
  std::string tableName;
  std::string startRow;
  std::string endRow;
  RangeSpecificationT rangeSpec;
  SchemaPtr schemaPtr;
  TestData tdata;
  TestSource  *tsource = 0;
  uint8_t *bufs[2];
  bool outstanding = false;
  struct stat statbuf;

  if (mArgs.size() != 2) {
    cerr << "Wrong number of arguments.  Type 'help' for usage." << endl;
    return -1;
  }

  if (mArgs[0].second != "" || mArgs[1].second != "")
    Usage::DumpAndExit(msUsage);

  if (!ParseRangeSpec(mArgs[0].first, tableName, startRow, endRow)) {
    cerr << "error:  Invalid range specification." << endl;
  }

  schemaPtr = Global::schemaMap[tableName];

  if (!schemaPtr) {
    cerr << "error: Table schema not cached, try loading range first." << endl;
    return Error::OK;
  }

  rangeSpec.tableName = tableName.c_str();
  rangeSpec.startRow = startRow.c_str();
  rangeSpec.endRow = endRow.c_str();
  rangeSpec.generation = schemaPtr->GetGeneration();

  // verify data file
  if (stat(mArgs[1].first.c_str(), &statbuf) != 0) {
    LOG_VA_ERROR("Unable to stat data file '%s' : %s", mArgs[1].second.c_str(), strerror(errno));
    return false;
  }

  tsource = new TestSource(mArgs[1].first, schemaPtr.get());

  uint8_t *buf = new uint8_t [ BUFFER_SIZE ];
  uint8_t *ptr = buf;
  uint8_t *endPtr = buf + BUFFER_SIZE;
  int pendingAmount;
  ByteString32T *key, *value;
  EventPtr eventPtr;

  while (true) {

    pendingAmount = 0;

    while (tsource->Next(&key, &value)) {
      pendingAmount = Length(key) + Length(value);
      if (pendingAmount <= endPtr-ptr || ptr == buf) {
	if (pendingAmount > endPtr-ptr) {
	  delete [] buf;
	  buf = new uint8_t [ pendingAmount ];
	  ptr = buf;
	  endPtr = buf + pendingAmount;
	}
	memcpy(ptr, key, Length(key));
	ptr += Length(key);
	memcpy(ptr, value, Length(value));
	ptr += Length(value);
      }
      else
	break;
    }

    if (outstanding) {
      if (!syncHandler.WaitForReply(eventPtr)) {
	error = Protocol::ResponseCode(eventPtr);
	if (error == Error::RANGESERVER_PARTIAL_UPDATE)
	  cout << "partial update" << endl;
	else {
	  LOG_VA_ERROR("update error : %s", (Protocol::StringFormatMessage(eventPtr)).c_str());
	  return error;
	}
      }
      else
	cout << "success" << endl;
    }

    outstanding = false;

    if (ptr > buf) {
      if ((error = Global::rangeServer->Update(mAddr, rangeSpec, buf, ptr-buf, &syncHandler)) != Error::OK) {
	LOG_VA_ERROR("Problem sending updates - %s", Error::GetText(error));
	return error;
      }
      outstanding = true;
      if (pendingAmount > 0) {
	if (pendingAmount > BUFFER_SIZE) {
	  ptr = buf = new uint8_t [ pendingAmount ];
	  endPtr = buf + pendingAmount;
	}
	else {
	  ptr = buf = new uint8_t [ BUFFER_SIZE ];
	  endPtr = buf + BUFFER_SIZE;
	}
	memcpy(ptr, key, Length(key));
	ptr += Length(key);
	memcpy(ptr, value, Length(value));
	ptr += Length(value);
      }
      else
	break;
    }
    else
      break;
  }

  if (outstanding) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      error = Protocol::ResponseCode(eventPtr);
      if (error == Error::RANGESERVER_PARTIAL_UPDATE)
	cout << "partial update" << endl;
      else {
	LOG_VA_ERROR("update error : %s", (Protocol::StringFormatMessage(eventPtr)).c_str());
	return error;
      }
    }
    else
      cout << "success" << endl;
  }

  return Error::OK;
}
