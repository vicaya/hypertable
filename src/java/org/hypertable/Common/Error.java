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


package org.hypertable.Common;

import java.util.HashMap;

public class Error {

    static public final int OK                 = 0;
    static public final int PROTOCOL_ERROR     = 1;
    static public final int REQUEST_TRUNCATED  = 2;
    static public final int RESPONSE_TRUNCATED = 3;
    static public final int REQUEST_TIMEOUT    = 4;
    static public final int LOCAL_IO_ERROR     = 5;

    static public final int COMM_NOT_CONNECTED      = 0x00010001;
    static public final int COMM_BROKEN_CONNECTION  = 0x00010002;
    static public final int COMM_CONNECT_ERROR      = 0x00010003;
    static public final int COMM_ALREADY_CONNECTED  = 0x00010004;
    static public final int COMM_REQUEST_TIMEOUT    = 0x00010005;

    static public final int HDFSBROKER_BAD_FILE_HANDLE  = 0x00020001;
    static public final int HDFSBROKER_IO_ERROR         = 0x00020002;
    static public final int HDFSBROKER_FILE_NOT_FOUND   = 0x00020003;

    static public final int HYPERTABLEFS_IO_ERROR       = 0x00030001;
    static public final int HYPERTABLEFS_CREATE_FAILED  = 0x00030002;
    static public final int HYPERTABLEFS_FILE_NOT_FOUND = 0x00030003;
    static public final int HYPERTABLEFS_ATTR_NOT_FOUND = 0x00030004;

    static public final int BTMASTER_TABLE_EXISTS = 0x00040001;

    static public final int RANGESERVER_GENERATION_MISMATCH  = 0x00050001;
    static public final int RANGESERVER_RANGE_ALREADY_LOADED = 0x00050002;
    static public final int RANGESERVER_RANGE_MISMATCH       = 0x00050003;
    static public final int RANGESERVER_NONEXISTENT_RANGE    = 0x00050004;
    static public final int RANGESERVER_PARTIAL_UPDATE       = 0x00050005;
    static public final int RANGESERVER_RANGE_NOT_FOUND      = 0x00050006;
    static public final int RANGESERVER_INVALID_SCANNER_ID   = 0x00050007;
    static public final int RANGESERVER_SCHEMA_PARSE_ERROR   = 0x00050008;

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
	mTextMap.put(COMM_NOT_CONNECTED,         "COMM not connected");
	mTextMap.put(COMM_BROKEN_CONNECTION,     "COMM broken connection");
	mTextMap.put(COMM_CONNECT_ERROR,         "COMM connect error");
	mTextMap.put(COMM_ALREADY_CONNECTED,     "COMM already connected");
	mTextMap.put(COMM_REQUEST_TIMEOUT,       "COMM request timeout");
	mTextMap.put(HDFSBROKER_BAD_FILE_HANDLE, "HDFS BROKER bad file handle");
	mTextMap.put(HDFSBROKER_IO_ERROR,        "HDFS BROKER i/o error");
	mTextMap.put(HDFSBROKER_FILE_NOT_FOUND,  "HDFS BROKER file not found");
	mTextMap.put(HYPERTABLEFS_IO_ERROR,       "HYPERTABLE FS i/o error");
	mTextMap.put(HYPERTABLEFS_CREATE_FAILED,  "HYPERTABLE FS create failed");
	mTextMap.put(HYPERTABLEFS_FILE_NOT_FOUND, "HYPERTABLE FS file not found");
	mTextMap.put(HYPERTABLEFS_ATTR_NOT_FOUND, "HYPERTABLE FS attribute not found");
	mTextMap.put(BTMASTER_TABLE_EXISTS,       "BIGTABLE MASTER table exists");
	mTextMap.put(RANGESERVER_GENERATION_MISMATCH,  "RANGE SERVER generation mismatch");
	mTextMap.put(RANGESERVER_RANGE_ALREADY_LOADED, "RANGE SERVER tablet already loaded");
	mTextMap.put(RANGESERVER_RANGE_MISMATCH,       "RANGE SERVER range mismatch");
	mTextMap.put(RANGESERVER_NONEXISTENT_RANGE,    "RANGE SERVER non-existent tablet");
	mTextMap.put(RANGESERVER_PARTIAL_UPDATE,       "RANGE SERVER partial update");
	mTextMap.put(RANGESERVER_RANGE_NOT_FOUND,      "RANGE SERVER tablet not found");
	mTextMap.put(RANGESERVER_INVALID_SCANNER_ID,   "RANGE SERVER invalid scanner id");
	mTextMap.put(RANGESERVER_SCHEMA_PARSE_ERROR,   "RANGE SERVER schema parse error");
    }
}
