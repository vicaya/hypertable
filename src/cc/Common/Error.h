/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERTABLE_ERROR_H
#define HYPERTABLE_ERROR_H

#include <stdexcept>
#include <ext/hash_map>
#include <string>

namespace Hypertable {

  class Error {

  public:

    static const int OK                 = 0;
    static const int PROTOCOL_ERROR     = 1;
    static const int REQUEST_TRUNCATED  = 2;
    static const int RESPONSE_TRUNCATED = 3;
    static const int REQUEST_TIMEOUT    = 4;
    static const int LOCAL_IO_ERROR     = 5;
    static const int BAD_ROOT_LOCATION  = 6;
    static const int BAD_SCHEMA         = 7;
    static const int INVALID_METADATA   = 8;
    static const int BAD_KEY            = 9;
    static const int METADATA_NOT_FOUND = 10;
    static const int HQL_PARSE_ERROR    = 11;
    static const int FILE_NOT_FOUND     = 12;
    static const int BLOCK_COMPRESSOR_UNSUPPORTED_TYPE  = 13;
    static const int BLOCK_COMPRESSOR_INVALID_ARG       = 14;
    static const int BLOCK_COMPRESSOR_TRUNCATED         = 15;
    static const int BLOCK_COMPRESSOR_BAD_HEADER        = 16;
    static const int BLOCK_COMPRESSOR_BAD_MAGIC         = 17;
    static const int BLOCK_COMPRESSOR_CHECKSUM_MISMATCH = 18;
    static const int BLOCK_COMPRESSOR_DEFLATE_ERROR     = 19;
    static const int BLOCK_COMPRESSOR_INFLATE_ERROR     = 20;
    static const int BLOCK_COMPRESSOR_INIT_ERROR        = 21;
    static const int TABLE_DOES_NOT_EXIST               = 22;
    static const int FAILED_EXPECTATION                 = 23;

    static const int COMM_NOT_CONNECTED       = 0x00010001;
    static const int COMM_BROKEN_CONNECTION   = 0x00010002;
    static const int COMM_CONNECT_ERROR       = 0x00010003;
    static const int COMM_ALREADY_CONNECTED   = 0x00010004;
    static const int COMM_REQUEST_TIMEOUT     = 0x00010005;
    static const int COMM_SEND_ERROR          = 0x00010006;
    static const int COMM_RECEIVE_ERROR       = 0x00010007;
    static const int COMM_POLL_ERROR          = 0x00010008;
    static const int COMM_CONFLICTING_ADDRESS = 0x00010009;

    static const int DFSBROKER_BAD_FILE_HANDLE   = 0x00020001;
    static const int DFSBROKER_IO_ERROR          = 0x00020002;
    static const int DFSBROKER_FILE_NOT_FOUND    = 0x00020003;
    static const int DFSBROKER_BAD_FILENAME      = 0x00020004;
    static const int DFSBROKER_PERMISSION_DENIED = 0x00020005;
    static const int DFSBROKER_INVALID_ARGUMENT  = 0x00020006;

    static const int HYPERSPACE_IO_ERROR          = 0x00030001;
    static const int HYPERSPACE_CREATE_FAILED     = 0x00030002;
    static const int HYPERSPACE_FILE_NOT_FOUND    = 0x00030003;
    static const int HYPERSPACE_ATTR_NOT_FOUND    = 0x00030004;
    static const int HYPERSPACE_DELETE_ERROR      = 0x00030005;
    static const int HYPERSPACE_BAD_PATHNAME      = 0x00030006;
    static const int HYPERSPACE_PERMISSION_DENIED = 0x00030007;
    static const int HYPERSPACE_EXPIRED_SESSION   = 0x00030008;
    static const int HYPERSPACE_FILE_EXISTS       = 0x00030009;
    static const int HYPERSPACE_IS_DIRECTORY      = 0x0003000A;
    static const int HYPERSPACE_INVALID_HANDLE    = 0x0003000B;
    static const int HYPERSPACE_REQUEST_CANCELLED = 0x0003000C;
    static const int HYPERSPACE_MODE_RESTRICTION  = 0x0003000D;
    static const int HYPERSPACE_ALREADY_LOCKED    = 0x0003000E;
    static const int HYPERSPACE_LOCK_CONFLICT     = 0x0003000F;
    static const int HYPERSPACE_NOT_LOCKED        = 0x00030010;
    static const int HYPERSPACE_BAD_ATTRIBUTE     = 0x00030011;

    static const int MASTER_TABLE_EXISTS   = 0x00040001;
    static const int MASTER_BAD_SCHEMA     = 0x00040002;
    static const int MASTER_NOT_RUNNING    = 0x00040003;

    static const int RANGESERVER_GENERATION_MISMATCH    = 0x00050001;
    static const int RANGESERVER_RANGE_ALREADY_LOADED   = 0x00050002;
    static const int RANGESERVER_RANGE_MISMATCH         = 0x00050003;
    static const int RANGESERVER_NONEXISTENT_RANGE      = 0x00050004;
    static const int RANGESERVER_PARTIAL_UPDATE         = 0x00050005;
    static const int RANGESERVER_RANGE_NOT_FOUND        = 0x00050006;
    static const int RANGESERVER_INVALID_SCANNER_ID     = 0x00050007;
    static const int RANGESERVER_SCHEMA_PARSE_ERROR     = 0x00050008;
    static const int RANGESERVER_SCHEMA_INVALID_CFID    = 0x00050009;
    static const int RANGESERVER_INVALID_COLUMNFAMILY   = 0x0005000A;
    static const int RANGESERVER_TRUNCATED_COMMIT_LOG   = 0x0005000B;
    static const int RANGESERVER_NO_METADATA_FOR_RANGE  = 0x0005000C;
    static const int RANGESERVER_SHUTTING_DOWN          = 0x0005000D;
    static const int RANGESERVER_CORRUPT_COMMIT_LOG     = 0x0005000E;
    static const int RANGESERVER_UNAVAILABLE            = 0x0005000F;
    static const int RANGESERVER_TIMESTAMP_ORDER_ERROR  = 0x00050010;

    static const int HQL_BAD_LOAD_FILE_FORMAT  = 0x00060001;

    static const char *get_text(int error);
    static std::string get_text_string(int error) { return get_text(error); }
  };

  /**
   * This is a generic exception class for Hypertable.  It takes an error code
   * as a constructor argument and translates it into an error message.
   */
  class Exception : public std::runtime_error {
  public:
    Exception(int error) : std::runtime_error(Error::get_text_string(error)), m_error(error) { return; }
    Exception(int error, const std::string &msg) : std::runtime_error(msg), m_error(error) { return; }
    int code() { return m_error; }
  private:
    int m_error;
  };

} // namespace Hypertable

#endif // HYPERTABLE_ERROR_H
