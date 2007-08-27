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

#include <cassert>

#include "Error.h"

using namespace std;
using namespace hypertable;

namespace {
  typedef struct {
    uint32_t     code;
    const char  *text;
  } ErrorInfoT;

  ErrorInfoT errorInfo[] = {
    { Error::OK,                          "HYPERTABLE ok" },
    { Error::PROTOCOL_ERROR,              "HYPERTABLE protocol error" },
    { Error::REQUEST_TRUNCATED,           "HYPERTABLE request truncated" },
    { Error::RESPONSE_TRUNCATED,          "HYPERTABLE response truncated" },
    { Error::REQUEST_TIMEOUT,             "HYPERTABLE request timeout" },
    { Error::LOCAL_IO_ERROR,              "HYPERTABLE local i/o error" },
    { Error::COMM_NOT_CONNECTED,          "COMM not connected" },
    { Error::COMM_BROKEN_CONNECTION,      "COMM broken connection" },
    { Error::COMM_CONNECT_ERROR,          "COMM connect error" },
    { Error::COMM_ALREADY_CONNECTED,      "COMM already connected" },
    { Error::COMM_REQUEST_TIMEOUT,        "COMM request timeout" },
    { Error::DFSBROKER_BAD_FILE_HANDLE,   "DFS BROKER bad file handle" },
    { Error::DFSBROKER_IO_ERROR,          "DFS BROKER i/o error" },
    { Error::DFSBROKER_FILE_NOT_FOUND,    "DFS BROKER file not found" },
    { Error::DFSBROKER_BAD_FILENAME,      "DFS BROKER bad filename" },
    { Error::DFSBROKER_PERMISSION_DENIED, "DFS BROKER permission denied" },
    { Error::DFSBROKER_INVALID_ARGUMENT,  "DFS BROKER invalid argument" },
    { Error::HYPERSPACE_IO_ERROR,         "HYPERSPACE i/o error" },
    { Error::HYPERSPACE_CREATE_FAILED,    "HYPERSPACE create failed" },
    { Error::HYPERSPACE_FILE_NOT_FOUND,   "HYPERSPACE file not found" },
    { Error::HYPERSPACE_ATTR_NOT_FOUND,   "HYPERSPACE attribute not found" },
    { Error::HYPERSPACE_DELETE_ERROR,     "HYPERSPACE delete error" },
    { Error::MASTER_TABLE_EXISTS,         "HYPERTABLE MASTER table exists" },
    { Error::MASTER_BAD_SCHEMA,           "HYPERTABLE MASTER bad schema" },
    { Error::RANGESERVER_GENERATION_MISMATCH,  "RANGE SERVER generation mismatch" },
    { Error::RANGESERVER_RANGE_ALREADY_LOADED, "RANGE SERVER range already loaded" },
    { Error::RANGESERVER_RANGE_MISMATCH,       "RANGE SERVER range mismatch" },
    { Error::RANGESERVER_NONEXISTENT_RANGE,    "RANGE SERVER non-existent range" },
    { Error::RANGESERVER_PARTIAL_UPDATE,       "RANGE SERVER partial update" },
    { Error::RANGESERVER_RANGE_NOT_FOUND,      "RANGE SERVER range not found" },
    { Error::RANGESERVER_INVALID_SCANNER_ID,   "RANGE SERVER invalid scanner id" },
    { Error::RANGESERVER_SCHEMA_PARSE_ERROR,   "RANGE SERVER schema parse error" },
    { Error::RANGESERVER_SCHEMA_INVALID_CFID,  "RANGE SERVER invalid column family id" },
    { 0, 0 }
  };

  typedef __gnu_cxx::hash_map<uint32_t, const char *>  TextMapT;

  TextMapT &buildTextMap() {
    TextMapT *map = new TextMapT();
    for (int i=0; errorInfo[i].text != 0; i++)
      (*map)[errorInfo[i].code] = errorInfo[i].text;
    return *map;
  }

  TextMapT &textMap = buildTextMap();
}


const int Error::OK;
const int Error::PROTOCOL_ERROR;
const int Error::REQUEST_TRUNCATED;
const int Error::RESPONSE_TRUNCATED;
const int Error::REQUEST_TIMEOUT;
const int Error::LOCAL_IO_ERROR;

const int Error::COMM_NOT_CONNECTED;
const int Error::COMM_BROKEN_CONNECTION;
const int Error::COMM_CONNECT_ERROR;
const int Error::COMM_ALREADY_CONNECTED;
const int Error::COMM_REQUEST_TIMEOUT;

const int Error::DFSBROKER_BAD_FILE_HANDLE;
const int Error::DFSBROKER_IO_ERROR;
const int Error::DFSBROKER_FILE_NOT_FOUND;
const int Error::DFSBROKER_BAD_FILENAME;
const int Error::DFSBROKER_PERMISSION_DENIED;
const int Error::DFSBROKER_INVALID_ARGUMENT;

const int Error::HYPERSPACE_IO_ERROR;
const int Error::HYPERSPACE_CREATE_FAILED;
const int Error::HYPERSPACE_FILE_NOT_FOUND;
const int Error::HYPERSPACE_ATTR_NOT_FOUND;
const int Error::HYPERSPACE_DELETE_ERROR;

const int Error::MASTER_TABLE_EXISTS;
const int Error::MASTER_BAD_SCHEMA;

const int Error::RANGESERVER_GENERATION_MISMATCH;
const int Error::RANGESERVER_RANGE_ALREADY_LOADED;
const int Error::RANGESERVER_RANGE_MISMATCH;
const int Error::RANGESERVER_NONEXISTENT_RANGE;
const int Error::RANGESERVER_PARTIAL_UPDATE;
const int Error::RANGESERVER_RANGE_NOT_FOUND;
const int Error::RANGESERVER_INVALID_SCANNER_ID;
const int Error::RANGESERVER_SCHEMA_PARSE_ERROR;
const int Error::RANGESERVER_SCHEMA_INVALID_CFID;

const char *Error::GetText(int error) {
  const char *text = textMap[error];
  if (text == 0)
    return "ERROR NOT REGISTERED";
  return text;
}
