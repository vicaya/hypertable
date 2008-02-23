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


package org.hypertable.Common;

import java.util.HashMap;

public class Error {

    static public final int OK                 = 0;
    static public final int PROTOCOL_ERROR     = 1;
    static public final int REQUEST_TRUNCATED  = 2;
    static public final int RESPONSE_TRUNCATED = 3;
    static public final int REQUEST_TIMEOUT    = 4;
    static public final int LOCAL_IO_ERROR     = 5;
    static public final int BAD_ROOT_LOCATION  = 6;
    static public final int BAD_SCHEMA         = 7;
    static public final int INVALID_METADATA   = 8;
    static public final int BAD_KEY            = 9;
    static public final int METADATA_NOT_FOUND = 10;
    static public final int HQL_PARSE_ERROR    = 11;
    static public final int FILE_NOT_FOUND     = 12;
    static public final int BLOCK_COMPRESSOR_UNSUPPORTED_TYPE  = 13;
    static public final int BLOCK_COMPRESSOR_INVALID_ARG       = 14;
    static public final int BLOCK_COMPRESSOR_TRUNCATED         = 15;
    static public final int BLOCK_COMPRESSOR_BAD_HEADER        = 16;
    static public final int BLOCK_COMPRESSOR_BAD_MAGIC         = 17;
    static public final int BLOCK_COMPRESSOR_CHECKSUM_MISMATCH = 18;
    static public final int BLOCK_COMPRESSOR_DEFLATE_ERROR     = 19;
    static public final int BLOCK_COMPRESSOR_INFLATE_ERROR     = 20;
    static public final int BLOCK_COMPRESSOR_INIT_ERROR        = 21;
    static public final int TABLE_DOES_NOT_EXIST               = 22;
    static public final int FAILED_EXPECTATION                 = 23;

    static public final int COMM_NOT_CONNECTED       = 0x00010001;
    static public final int COMM_BROKEN_CONNECTION   = 0x00010002;
    static public final int COMM_CONNECT_ERROR       = 0x00010003;
    static public final int COMM_ALREADY_CONNECTED   = 0x00010004;
    static public final int COMM_REQUEST_TIMEOUT     = 0x00010005;
    static public final int COMM_SEND_ERROR          = 0x00010006;
    static public final int COMM_RECEIVE_ERROR       = 0x00010007;
    static public final int COMM_POLL_ERROR          = 0x00010008;
    static public final int COMM_CONFLICTING_ADDRESS = 0x00010009;

    static public final int DFSBROKER_BAD_FILE_HANDLE   = 0x00020001;
    static public final int DFSBROKER_IO_ERROR          = 0x00020002;
    static public final int DFSBROKER_FILE_NOT_FOUND    = 0x00020003;
    static public final int DFSBROKER_BAD_FILENAME      = 0x00020004;
    static public final int DFSBROKER_PERMISSION_DENIED = 0x00020005;
    static public final int DFSBROKER_INVALID_ARGUMENT  = 0x00020006;

    static public final int HYPERSPACE_IO_ERROR          = 0x00030001;
    static public final int HYPERSPACE_CREATE_FAILED     = 0x00030002;
    static public final int HYPERSPACE_FILE_NOT_FOUND    = 0x00030003;
    static public final int HYPERSPACE_ATTR_NOT_FOUND    = 0x00030004;
    static public final int HYPERSPACE_DELETE_ERROR      = 0x00030005;
    static public final int HYPERSPACE_BAD_PATHNAME      = 0x00030006;
    static public final int HYPERSPACE_PERMISSION_DENIED = 0x00030007;
    static public final int HYPERSPACE_EXPIRED_SESSION   = 0x00030008;
    static public final int HYPERSPACE_FILE_EXISTS       = 0x00030009;
    static public final int HYPERSPACE_IS_DIRECTORY      = 0x0003000A;
    static public final int HYPERSPACE_INVALID_HANDLE    = 0x0003000B;
    static public final int HYPERSPACE_REQUEST_CANCELLED = 0x0003000C;
    static public final int HYPERSPACE_MODE_RESTRICTION  = 0x0003000D;
    static public final int HYPERSPACE_ALREADY_LOCKED    = 0x0003000E;
    static public final int HYPERSPACE_LOCK_CONFLICT     = 0x0003000F;
    static public final int HYPERSPACE_NOT_LOCKED        = 0x00030010;
    static public final int HYPERSPACE_BAD_ATTRIBUTE     = 0x00030011;

    static public final int MASTER_TABLE_EXISTS  = 0x00040001;
    static public final int MASTER_BAD_SCHEMA    = 0x00040002;
    static public final int MASTER_NOT_RUNNING   = 0x00040003;

    static public final int RANGESERVER_GENERATION_MISMATCH    = 0x00050001;
    static public final int RANGESERVER_RANGE_ALREADY_LOADED   = 0x00050002;
    static public final int RANGESERVER_RANGE_MISMATCH         = 0x00050003;
    static public final int RANGESERVER_NONEXISTENT_RANGE      = 0x00050004;
    static public final int RANGESERVER_PARTIAL_UPDATE         = 0x00050005;
    static public final int RANGESERVER_RANGE_NOT_FOUND        = 0x00050006;
    static public final int RANGESERVER_INVALID_SCANNER_ID     = 0x00050007;
    static public final int RANGESERVER_SCHEMA_PARSE_ERROR     = 0x00050008;
    static public final int RANGESERVER_SCHEMA_INVALID_CFID    = 0x00050009;
    static public final int RANGESERVER_INVALID_COLUMNFAMILY   = 0x0005000A;
    static public final int RANGESERVER_TRUNCATED_COMMIT_LOG   = 0x0005000B;
    static public final int RANGESERVER_NO_METADATA_FOR_RANGE  = 0x0005000C;
    static public final int RANGESERVER_SHUTTING_DOWN          = 0x0005000D;
    static public final int RANGESERVER_CORRUPT_COMMIT_LOG     = 0x0005000E;
    static public final int RANGESERVER_UNAVAILABLE            = 0x0005000F;
    static public final int RANGESERVER_TIMESTAMP_ORDER_ERROR  = 0x00050010;

    static public final int HQL_BAD_LOAD_FILE_FORMAT = 0x00060001;

    static public String GetText(int lcode) {
	return mTextMap.get(lcode);
    }

    static private HashMap<Integer, String> mTextMap = new HashMap<Integer, String>();

    static {
	mTextMap.put(OK,                         "HYPERTABLE ok");
	mTextMap.put(PROTOCOL_ERROR,             "HYPERTABLE protocol error");
	mTextMap.put(REQUEST_TRUNCATED,          "HYPERTABLE request truncated");
	mTextMap.put(RESPONSE_TRUNCATED,         "HYPERTABLE response truncated");
	mTextMap.put(REQUEST_TIMEOUT,            "HYPERTABLE request timeout");
	mTextMap.put(LOCAL_IO_ERROR,             "HYPERTABLE local i/o error");
	mTextMap.put(BAD_ROOT_LOCATION,          "HYPERTABLE bad root location");
	mTextMap.put(BAD_SCHEMA,                 "HYPERTABLE bad schema");
	mTextMap.put(INVALID_METADATA,           "HYPERTABLE invalid metadata");
	mTextMap.put(BAD_KEY,                    "HYPERTABLE bad key");
	mTextMap.put(METADATA_NOT_FOUND,         "HYPERTABLE metadata not found");
	mTextMap.put(HQL_PARSE_ERROR,            "HYPERTABLE HQL parse error");
	mTextMap.put(FILE_NOT_FOUND,             "HYPERTABLE file not found");
	mTextMap.put(BLOCK_COMPRESSOR_UNSUPPORTED_TYPE,  "HYPERTABLE block compressor unsupported type");
	mTextMap.put(BLOCK_COMPRESSOR_INVALID_ARG,       "HYPERTABLE block compressor invalid arg");
	mTextMap.put(BLOCK_COMPRESSOR_TRUNCATED,         "HYPERTABLE block compressor block truncated");
	mTextMap.put(BLOCK_COMPRESSOR_BAD_HEADER,        "HYPERTABLE block compressor bad block header");
	mTextMap.put(BLOCK_COMPRESSOR_BAD_MAGIC,         "HYPERTABLE block compressor bad magic string");
	mTextMap.put(BLOCK_COMPRESSOR_CHECKSUM_MISMATCH, "HYPERTABLE block compressor block checksum mismatch");
	mTextMap.put(BLOCK_COMPRESSOR_DEFLATE_ERROR,     "HYPERTABLE block compressor deflate error");
	mTextMap.put(BLOCK_COMPRESSOR_INFLATE_ERROR,     "HYPERTABLE block compressor inflate error");
	mTextMap.put(BLOCK_COMPRESSOR_INIT_ERROR,        "HYPERTABLE block compressor initialization error");
	mTextMap.put(TABLE_DOES_NOT_EXIST,       "HYPERTABLE table does not exist");
	mTextMap.put(FAILED_EXPECTATION,         "HYPERTABLE failed expectation");
	mTextMap.put(COMM_NOT_CONNECTED,         "COMM not connected");
	mTextMap.put(COMM_BROKEN_CONNECTION,     "COMM broken connection");
	mTextMap.put(COMM_CONNECT_ERROR,         "COMM connect error");
	mTextMap.put(COMM_ALREADY_CONNECTED,     "COMM already connected");
	mTextMap.put(COMM_REQUEST_TIMEOUT,       "COMM request timeout");
	mTextMap.put(COMM_SEND_ERROR,            "COMM send error");
	mTextMap.put(COMM_RECEIVE_ERROR,         "COMM receive error");
	mTextMap.put(COMM_POLL_ERROR,            "COMM poll error");
	mTextMap.put(DFSBROKER_BAD_FILE_HANDLE,  "DFS BROKER bad file handle");
	mTextMap.put(DFSBROKER_IO_ERROR,         "DFS BROKER i/o error");
	mTextMap.put(DFSBROKER_FILE_NOT_FOUND,   "DFS BROKER file not found");
	mTextMap.put(DFSBROKER_BAD_FILENAME,     "DFS BROKER bad filename");
	mTextMap.put(DFSBROKER_PERMISSION_DENIED,"DFS BROKER permission denied");
	mTextMap.put(DFSBROKER_INVALID_ARGUMENT, "DFS BROKER invalid argument");
	mTextMap.put(HYPERSPACE_IO_ERROR,        "HYPERSPACE i/o error");
	mTextMap.put(HYPERSPACE_CREATE_FAILED,   "HYPERSPACE create failed");
	mTextMap.put(HYPERSPACE_FILE_NOT_FOUND,  "HYPERSPACE file not found");
	mTextMap.put(HYPERSPACE_ATTR_NOT_FOUND,  "HYPERSPACE attribute not found");
	mTextMap.put(HYPERSPACE_DELETE_ERROR,    "HYPERSPACE delete error");
	mTextMap.put(HYPERSPACE_BAD_PATHNAME,    "HYPERSPACE bad pathname");
	mTextMap.put(HYPERSPACE_PERMISSION_DENIED, "HYPERSPACE permission denied");
	mTextMap.put(HYPERSPACE_EXPIRED_SESSION, "HYPERSPACE expired session");
	mTextMap.put(HYPERSPACE_FILE_EXISTS,     "HYPERSPACE file exists");
	mTextMap.put(HYPERSPACE_IS_DIRECTORY,    "HYPERSPACE is directory");
	mTextMap.put(HYPERSPACE_INVALID_HANDLE,  "HYPERSPACE invalid handle");
	mTextMap.put(HYPERSPACE_REQUEST_CANCELLED,"HYPERSPACE request cancelled");
	mTextMap.put(HYPERSPACE_MODE_RESTRICTION,"HYPERSPACE mode restriction");
	mTextMap.put(HYPERSPACE_ALREADY_LOCKED,  "HYPERSPACE already locked");
	mTextMap.put(HYPERSPACE_LOCK_CONFLICT,   "HYPERSPACE lock conflict");
	mTextMap.put(HYPERSPACE_NOT_LOCKED,      "HYPERSPACE not locked");
	mTextMap.put(HYPERSPACE_BAD_ATTRIBUTE,   "HYPERSPACE bad attribute");
	mTextMap.put(MASTER_TABLE_EXISTS,        "MASTER table exists");
	mTextMap.put(MASTER_TABLE_EXISTS,        "MASTER bad schema");
	mTextMap.put(MASTER_NOT_RUNNING,         "MASTER not running");
	mTextMap.put(RANGESERVER_GENERATION_MISMATCH,   "RANGE SERVER generation mismatch");
	mTextMap.put(RANGESERVER_RANGE_ALREADY_LOADED,  "RANGE SERVER range already loaded");
	mTextMap.put(RANGESERVER_RANGE_MISMATCH,        "RANGE SERVER range mismatch");
	mTextMap.put(RANGESERVER_NONEXISTENT_RANGE,     "RANGE SERVER non-existent range");
	mTextMap.put(RANGESERVER_PARTIAL_UPDATE,        "RANGE SERVER partial update");
	mTextMap.put(RANGESERVER_RANGE_NOT_FOUND,       "RANGE SERVER range not found");
	mTextMap.put(RANGESERVER_INVALID_SCANNER_ID,    "RANGE SERVER invalid scanner id");
	mTextMap.put(RANGESERVER_SCHEMA_PARSE_ERROR,    "RANGE SERVER schema parse error");
	mTextMap.put(RANGESERVER_SCHEMA_INVALID_CFID,   "RANGE SERVER invalid column family id");
	mTextMap.put(RANGESERVER_INVALID_COLUMNFAMILY,  "RANGE SERVER invalid column family");
	mTextMap.put(RANGESERVER_TRUNCATED_COMMIT_LOG,  "RANGE SERVER truncated commit log");
	mTextMap.put(RANGESERVER_NO_METADATA_FOR_RANGE, "RANGE SERVER no metadata for range");
	mTextMap.put(RANGESERVER_CORRUPT_COMMIT_LOG,    "RANGE SERVER corrupt commit log");
	mTextMap.put(RANGESERVER_SHUTTING_DOWN,         "RANGE SERVER shutting down");
	mTextMap.put(RANGESERVER_UNAVAILABLE,           "RANGE SERVER unavailable");
	mTextMap.put(RANGESERVER_TIMESTAMP_ORDER_ERROR, "RANGE SERVER supplied timestamp is not strictly increasing");
	mTextMap.put(HQL_BAD_LOAD_FILE_FORMAT,    "HQL bad load file format");
    }
}
