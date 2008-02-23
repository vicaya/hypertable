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
using namespace Hypertable;

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
    { Error::BAD_ROOT_LOCATION,           "HYPERTABLE bad root location" },
    { Error::BAD_SCHEMA,                  "HYPERTABLE bad schema" },
    { Error::INVALID_METADATA,            "HYPERTABLE invalid metadata" },
    { Error::BAD_KEY,                     "HYPERTABLE bad key" },
    { Error::METADATA_NOT_FOUND,          "HYPERTABLE metadata not found" },
    { Error::HQL_PARSE_ERROR,             "HYPERTABLE HQL parse error" },
    { Error::FILE_NOT_FOUND,              "HYPERTABLE file not found" },
    { Error::BLOCK_COMPRESSOR_UNSUPPORTED_TYPE,  "HYPERTABLE block compressor unsupported type" },
    { Error::BLOCK_COMPRESSOR_INVALID_ARG,       "HYPERTABLE block compressor invalid arg" },
    { Error::BLOCK_COMPRESSOR_TRUNCATED,         "HYPERTABLE block compressor block truncated" },
    { Error::BLOCK_COMPRESSOR_BAD_HEADER,        "HYPERTABLE block compressor bad block header" },
    { Error::BLOCK_COMPRESSOR_BAD_MAGIC,         "HYPERTABLE block compressor bad magic string" },
    { Error::BLOCK_COMPRESSOR_CHECKSUM_MISMATCH, "HYPERTABLE block compressor block checksum mismatch" },
    { Error::BLOCK_COMPRESSOR_INFLATE_ERROR,     "HYPERTABLE block compressor inflate error" },
    { Error::BLOCK_COMPRESSOR_INIT_ERROR,        "HYPERTABLE block compressor initialization error" },
    { Error::TABLE_DOES_NOT_EXIST,               "HYPERTABLE table does not exist" },
    { Error::FAILED_EXPECTATION,          "HYPERTABLE failed expectation" },
    { Error::COMM_NOT_CONNECTED,          "COMM not connected" },
    { Error::COMM_BROKEN_CONNECTION,      "COMM broken connection" },
    { Error::COMM_CONNECT_ERROR,          "COMM connect error" },
    { Error::COMM_ALREADY_CONNECTED,      "COMM already connected" },
    { Error::COMM_REQUEST_TIMEOUT,        "COMM request timeout" },
    { Error::COMM_SEND_ERROR,             "COMM send error" },
    { Error::COMM_RECEIVE_ERROR,          "COMM receive error" },
    { Error::COMM_POLL_ERROR,             "COMM poll error" },
    { Error::COMM_CONFLICTING_ADDRESS,    "COMM conflicting address" },
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
    { Error::HYPERSPACE_BAD_PATHNAME,     "HYPERSPACE bad pathname" },
    { Error::HYPERSPACE_PERMISSION_DENIED,"HYPERSPACE permission denied" },
    { Error::HYPERSPACE_EXPIRED_SESSION,  "HYPERSPACE expired session" },
    { Error::HYPERSPACE_FILE_EXISTS,      "HYPERSPACE file exists" },
    { Error::HYPERSPACE_IS_DIRECTORY,     "HYPERSPACE is directory" },
    { Error::HYPERSPACE_INVALID_HANDLE,   "HYPERSPACE invalid handle" },
    { Error::HYPERSPACE_REQUEST_CANCELLED,"HYPERSPACE request cancelled" },
    { Error::HYPERSPACE_MODE_RESTRICTION, "HYPERSPACE mode restriction" },
    { Error::HYPERSPACE_ALREADY_LOCKED,   "HYPERSPACE already locked" },
    { Error::HYPERSPACE_LOCK_CONFLICT,    "HYPERSPACE lock conflict" },
    { Error::HYPERSPACE_NOT_LOCKED,       "HYPERSPACE not locked" },
    { Error::HYPERSPACE_BAD_ATTRIBUTE,    "HYPERSPACE bad attribute" },
    { Error::MASTER_TABLE_EXISTS,         "MASTER table exists" },
    { Error::MASTER_BAD_SCHEMA,           "MASTER bad schema" },
    { Error::MASTER_NOT_RUNNING,          "MASTER not running" },
    { Error::RANGESERVER_GENERATION_MISMATCH,  "RANGE SERVER generation mismatch" },
    { Error::RANGESERVER_RANGE_ALREADY_LOADED, "RANGE SERVER range already loaded" },
    { Error::RANGESERVER_RANGE_MISMATCH,       "RANGE SERVER range mismatch" },
    { Error::RANGESERVER_NONEXISTENT_RANGE,    "RANGE SERVER non-existent range" },
    { Error::RANGESERVER_PARTIAL_UPDATE,       "RANGE SERVER partial update" },
    { Error::RANGESERVER_RANGE_NOT_FOUND,      "RANGE SERVER range not found" },
    { Error::RANGESERVER_INVALID_SCANNER_ID,   "RANGE SERVER invalid scanner id" },
    { Error::RANGESERVER_SCHEMA_PARSE_ERROR,   "RANGE SERVER schema parse error" },
    { Error::RANGESERVER_SCHEMA_INVALID_CFID,  "RANGE SERVER invalid column family id" },
    { Error::RANGESERVER_INVALID_COLUMNFAMILY, "RANGE SERVER invalid column family" },
    { Error::RANGESERVER_TRUNCATED_COMMIT_LOG, "RANGE SERVER truncated commit log" },
    { Error::RANGESERVER_NO_METADATA_FOR_RANGE, "RANGE SERVER no metadata for range" },
    { Error::RANGESERVER_SHUTTING_DOWN,        "RANGE SERVER shutting down" },
    { Error::RANGESERVER_CORRUPT_COMMIT_LOG,   "RANGE SERVER corrupt commit log" },
    { Error::RANGESERVER_UNAVAILABLE,          "RANGE SERVER unavailable" },
    { Error::RANGESERVER_TIMESTAMP_ORDER_ERROR,"RANGE SERVER supplied timestamp is not strictly increasing" },
    { Error::HQL_BAD_LOAD_FILE_FORMAT,         "HQL bad load file format" },
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
const int Error::BAD_ROOT_LOCATION;
const int Error::BAD_SCHEMA;
const int Error::INVALID_METADATA;
const int Error::BAD_KEY;
const int Error::METADATA_NOT_FOUND;
const int Error::HQL_PARSE_ERROR;
const int Error::FILE_NOT_FOUND;
const int Error::BLOCK_COMPRESSOR_UNSUPPORTED_TYPE;
const int Error::BLOCK_COMPRESSOR_INVALID_ARG;
const int Error::BLOCK_COMPRESSOR_TRUNCATED;
const int Error::BLOCK_COMPRESSOR_BAD_HEADER;
const int Error::BLOCK_COMPRESSOR_BAD_MAGIC;
const int Error::BLOCK_COMPRESSOR_CHECKSUM_MISMATCH;
const int Error::BLOCK_COMPRESSOR_INFLATE_ERROR;
const int Error::BLOCK_COMPRESSOR_INIT_ERROR;
const int Error::TABLE_DOES_NOT_EXIST;
const int Error::FAILED_EXPECTATION;

const int Error::COMM_NOT_CONNECTED;
const int Error::COMM_BROKEN_CONNECTION;
const int Error::COMM_CONNECT_ERROR;
const int Error::COMM_ALREADY_CONNECTED;
const int Error::COMM_REQUEST_TIMEOUT;
const int Error::COMM_SEND_ERROR;
const int Error::COMM_RECEIVE_ERROR;
const int Error::COMM_POLL_ERROR;

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
const int Error::HYPERSPACE_BAD_PATHNAME;
const int Error::HYPERSPACE_PERMISSION_DENIED;
const int Error::HYPERSPACE_EXPIRED_SESSION;
const int Error::HYPERSPACE_FILE_EXISTS;
const int Error::HYPERSPACE_IS_DIRECTORY;
const int Error::HYPERSPACE_INVALID_HANDLE;
const int Error::HYPERSPACE_MODE_RESTRICTION;
const int Error::HYPERSPACE_ALREADY_LOCKED;
const int Error::HYPERSPACE_LOCK_CONFLICT;
const int Error::HYPERSPACE_NOT_LOCKED;
const int Error::HYPERSPACE_BAD_ATTRIBUTE;

const int Error::MASTER_TABLE_EXISTS;
const int Error::MASTER_BAD_SCHEMA;
const int Error::MASTER_NOT_RUNNING;

const int Error::RANGESERVER_GENERATION_MISMATCH;
const int Error::RANGESERVER_RANGE_ALREADY_LOADED;
const int Error::RANGESERVER_RANGE_MISMATCH;
const int Error::RANGESERVER_NONEXISTENT_RANGE;
const int Error::RANGESERVER_PARTIAL_UPDATE;
const int Error::RANGESERVER_RANGE_NOT_FOUND;
const int Error::RANGESERVER_INVALID_SCANNER_ID;
const int Error::RANGESERVER_SCHEMA_PARSE_ERROR;
const int Error::RANGESERVER_SCHEMA_INVALID_CFID;
const int Error::RANGESERVER_TRUNCATED_COMMIT_LOG;
const int Error::RANGESERVER_NO_METADATA_FOR_RANGE;
const int Error::RANGESERVER_SHUTTING_DOWN;
const int Error::RANGESERVER_CORRUPT_COMMIT_LOG;
const int Error::RANGESERVER_UNAVAILABLE;
const int Error::RANGESERVER_TIMESTAMP_ORDER_ERROR;

const int Error::HQL_BAD_LOAD_FILE_FORMAT;


const char *Error::get_text(int error) {
  const char *text = textMap[error];
  if (text == 0)
    return "ERROR NOT REGISTERED";
  return text;
}
