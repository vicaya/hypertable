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


#ifndef HYPERTABLE_ERROR_H
#define HYPERTABLE_ERROR_H

#include <ext/hash_map>
#include <string>
using namespace std;

namespace hypertable {

  class Error {

  public:

    static const int OK                 = 0;
    static const int PROTOCOL_ERROR     = 1;
    static const int REQUEST_TRUNCATED  = 2;
    static const int RESPONSE_TRUNCATED = 3;
    static const int REQUEST_TIMEOUT    = 4;
    static const int LOCAL_IO_ERROR     = 5;

    static const int COMM_NOT_CONNECTED     = 0x00010001;
    static const int COMM_BROKEN_CONNECTION = 0x00010002;
    static const int COMM_CONNECT_ERROR     = 0x00010003;
    static const int COMM_ALREADY_CONNECTED = 0x00010004;
    static const int COMM_REQUEST_TIMEOUT   = 0x00010005;

    static const int HDFSBROKER_BAD_FILE_HANDLE = 0x00020001;
    static const int HDFSBROKER_IO_ERROR        = 0x00020002;
    static const int HDFSBROKER_FILE_NOT_FOUND  = 0x00020003;

    static const int HYPERTABLEFS_IO_ERROR       = 0x00030001;
    static const int HYPERTABLEFS_CREATE_FAILED  = 0x00030002;
    static const int HYPERTABLEFS_FILE_NOT_FOUND = 0x00030003;
    static const int HYPERTABLEFS_ATTR_NOT_FOUND = 0x00030004;

    static const int BTMASTER_TABLE_EXISTS = 0x00040001;

    static const int RANGESERVER_GENERATION_MISMATCH  = 0x00050001;
    static const int RANGESERVER_RANGE_ALREADY_LOADED = 0x00050002;
    static const int RANGESERVER_RANGE_MISMATCH       = 0x00050003;
    static const int RANGESERVER_NONEXISTENT_RANGE    = 0x00050004;
    static const int RANGESERVER_PARTIAL_UPDATE       = 0x00050005;
    static const int RANGESERVER_RANGE_NOT_FOUND      = 0x00050006;
    static const int RANGESERVER_INVALID_SCANNER_ID   = 0x00050007;
    static const int RANGESERVER_SCHEMA_PARSE_ERROR   = 0x00050008;

    static const char *GetText(int error);

  };
}

#endif // HYPERTABLE_ERROR_H
