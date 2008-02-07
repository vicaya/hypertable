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

#include <boost/algorithm/string.hpp>

#include "Common/Error.h"
#include "Common/Usage.h"

#include "AsyncComm/DispatchHandlerSynchronizer.h"
#include "AsyncComm/Event.h"

#include "Hypertable/Lib/TestData.h"
#include "Hypertable/Lib/TestSource.h"

#include "CommandUpdate.h"
#include "ParseRangeSpec.h"
#include "TableInfo.h"

#define BUFFER_SIZE 65536

using namespace Hypertable;
using namespace std;

const char *CommandUpdate::ms_usage[] = {
  "update <table> <datafile>",
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
  int error;
  TableIdentifierT *table;
  std::string tableName;
  SchemaPtr schema_ptr;
  TestData tdata;
  TestSource  *tsource = 0;
  bool outstanding = false;
  struct stat statbuf;
  TableInfo *table_info;

  if (m_args.size() != 2) {
    cerr << "Wrong number of arguments.  Type 'help' for usage." << endl;
    return -1;
  }

  if (m_args[0].second != "" || m_args[1].second != "")
    Usage::dump_and_exit(ms_usage);

  tableName = m_args[0].first;

#if defined(__APPLE__)
  boost::to_upper(tableName);
#endif

  table_info = TableInfo::map[tableName];

  if (table_info == 0) {
    table_info = new TableInfo(tableName);
    if ((error = table_info->load(m_hyperspace_ptr)) != Error::OK)
      return error;
    TableInfo::map[tableName] = table_info;
  }

  table = table_info->get_table_identifier();
  table_info->get_schema_ptr(schema_ptr);

  // verify data file
  if (stat(m_args[1].first.c_str(), &statbuf) != 0) {
    HT_ERRORF("Unable to stat data file '%s' : %s", m_args[1].second.c_str(), strerror(errno));
    return false;
  }

  tsource = new TestSource(m_args[1].first, schema_ptr.get());

  uint8_t *send_buf = 0;
  size_t   send_buf_len = 0;
  DynamicBuffer buf(BUFFER_SIZE);
  ByteString32T *key, *value;
  EventPtr eventPtr;

  while (true) {

    while (tsource->next(&key, &value)) {
      buf.ensure(Length(key) + Length(value));
      buf.addNoCheck(key, Length(key));
      buf.addNoCheck(value, Length(value));
      if (buf.fill() > BUFFER_SIZE)
	break;
    }

    /**
     * Sort the keys
     */
    if (buf.fill()) {
      std::vector<ByteString32T *> keys;
      struct ltByteString32 ltbs32;
      uint8_t *ptr;
      size_t len;

      ptr = buf.buf;

      while (ptr < buf.ptr) {
	key = (ByteString32T *)ptr;
	keys.push_back(key);
	ptr += Length(key) + Length(Skip(key));
      }

      std::sort(keys.begin(), keys.end(), ltbs32);

      send_buf = new uint8_t [ buf.fill() ];
      ptr = send_buf;
      for (size_t i=0; i<keys.size(); i++) {
	len = Length(keys[i]) + Length(Skip(keys[i]));
	memcpy(ptr, keys[i], len);
	ptr += len;
      }
      send_buf_len = ptr - send_buf;
      buf.clear();
    }
    else
      send_buf_len = 0;

    if (outstanding) {
      if (!syncHandler.wait_for_reply(eventPtr)) {
	error = Protocol::response_code(eventPtr);
	if (error == Error::RANGESERVER_PARTIAL_UPDATE)
	  cout << "partial update" << endl;
	else {
	  HT_ERRORF("update error : %s", (Protocol::string_format_message(eventPtr)).c_str());
	  return error;
	}
      }
      else
	cout << "success" << endl;
    }

    outstanding = false;

    if (send_buf_len > 0) {
      if ((error = m_range_server_ptr->update(m_addr, *table, send_buf, send_buf_len, &syncHandler)) != Error::OK) {
	HT_ERRORF("Problem sending updates - %s", Error::get_text(error));
	return error;
      }
      outstanding = true;
    }
    else
      break;
  }

  if (outstanding) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      error = Protocol::response_code(eventPtr);
      if (error == Error::RANGESERVER_PARTIAL_UPDATE)
	cout << "partial update" << endl;
      else {
	HT_ERRORF("update error : %s", (Protocol::string_format_message(eventPtr)).c_str());
	return error;
      }
    }
    else
      cout << "success" << endl;
  }

  return Error::OK;
}
