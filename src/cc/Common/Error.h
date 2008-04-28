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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#ifndef HYPERTABLE_ERROR_H
#define HYPERTABLE_ERROR_H

#include "Common/String.h"
#include <stdexcept>

namespace Hypertable {
  namespace Error {
    enum Code {
      OK                 = 0,
      PROTOCOL_ERROR     = 1,
      REQUEST_TRUNCATED  = 2,
      RESPONSE_TRUNCATED = 3,
      REQUEST_TIMEOUT    = 4,
      LOCAL_IO_ERROR     = 5,
      BAD_ROOT_LOCATION  = 6,
      BAD_SCHEMA         = 7,
      INVALID_METADATA   = 8,
      BAD_KEY            = 9,
      METADATA_NOT_FOUND = 10,
      HQL_PARSE_ERROR    = 11,
      FILE_NOT_FOUND     = 12,
      BLOCK_COMPRESSOR_UNSUPPORTED_TYPE  = 13,
      BLOCK_COMPRESSOR_INVALID_ARG       = 14,
      BLOCK_COMPRESSOR_TRUNCATED         = 15,
      BLOCK_COMPRESSOR_BAD_HEADER        = 16,
      BLOCK_COMPRESSOR_BAD_MAGIC         = 17,
      BLOCK_COMPRESSOR_CHECKSUM_MISMATCH = 18,
      BLOCK_COMPRESSOR_DEFLATE_ERROR     = 19,
      BLOCK_COMPRESSOR_INFLATE_ERROR     = 20,
      BLOCK_COMPRESSOR_INIT_ERROR        = 21,
      TABLE_DOES_NOT_EXIST               = 22,
      FAILED_EXPECTATION                 = 23,

      COMM_NOT_CONNECTED       = 0x00010001,
      COMM_BROKEN_CONNECTION   = 0x00010002,
      COMM_CONNECT_ERROR       = 0x00010003,
      COMM_ALREADY_CONNECTED   = 0x00010004,
      COMM_REQUEST_TIMEOUT     = 0x00010005,
      COMM_SEND_ERROR          = 0x00010006,
      COMM_RECEIVE_ERROR       = 0x00010007,
      COMM_POLL_ERROR          = 0x00010008,
      COMM_CONFLICTING_ADDRESS = 0x00010009,

      DFSBROKER_BAD_FILE_HANDLE   = 0x00020001,
      DFSBROKER_IO_ERROR          = 0x00020002,
      DFSBROKER_FILE_NOT_FOUND    = 0x00020003,
      DFSBROKER_BAD_FILENAME      = 0x00020004,
      DFSBROKER_PERMISSION_DENIED = 0x00020005,
      DFSBROKER_INVALID_ARGUMENT  = 0x00020006,
      DFSBROKER_INVALID_CONFIG    = 0x00020007,

      HYPERSPACE_IO_ERROR          = 0x00030001,
      HYPERSPACE_CREATE_FAILED     = 0x00030002,
      HYPERSPACE_FILE_NOT_FOUND    = 0x00030003,
      HYPERSPACE_ATTR_NOT_FOUND    = 0x00030004,
      HYPERSPACE_DELETE_ERROR      = 0x00030005,
      HYPERSPACE_BAD_PATHNAME      = 0x00030006,
      HYPERSPACE_PERMISSION_DENIED = 0x00030007,
      HYPERSPACE_EXPIRED_SESSION   = 0x00030008,
      HYPERSPACE_FILE_EXISTS       = 0x00030009,
      HYPERSPACE_IS_DIRECTORY      = 0x0003000A,
      HYPERSPACE_INVALID_HANDLE    = 0x0003000B,
      HYPERSPACE_REQUEST_CANCELLED = 0x0003000C,
      HYPERSPACE_MODE_RESTRICTION  = 0x0003000D,
      HYPERSPACE_ALREADY_LOCKED    = 0x0003000E,
      HYPERSPACE_LOCK_CONFLICT     = 0x0003000F,
      HYPERSPACE_NOT_LOCKED        = 0x00030010,
      HYPERSPACE_BAD_ATTRIBUTE     = 0x00030011,

      MASTER_TABLE_EXISTS   = 0x00040001,
      MASTER_BAD_SCHEMA     = 0x00040002,
      MASTER_NOT_RUNNING    = 0x00040003,

      RANGESERVER_GENERATION_MISMATCH    = 0x00050001,
      RANGESERVER_RANGE_ALREADY_LOADED   = 0x00050002,
      RANGESERVER_RANGE_MISMATCH         = 0x00050003,
      RANGESERVER_NONEXISTENT_RANGE      = 0x00050004,
      RANGESERVER_PARTIAL_UPDATE         = 0x00050005,
      RANGESERVER_RANGE_NOT_FOUND        = 0x00050006,
      RANGESERVER_INVALID_SCANNER_ID     = 0x00050007,
      RANGESERVER_SCHEMA_PARSE_ERROR     = 0x00050008,
      RANGESERVER_SCHEMA_INVALID_CFID    = 0x00050009,
      RANGESERVER_INVALID_COLUMNFAMILY   = 0x0005000A,
      RANGESERVER_TRUNCATED_COMMIT_LOG   = 0x0005000B,
      RANGESERVER_NO_METADATA_FOR_RANGE  = 0x0005000C,
      RANGESERVER_SHUTTING_DOWN          = 0x0005000D,
      RANGESERVER_CORRUPT_COMMIT_LOG     = 0x0005000E,
      RANGESERVER_UNAVAILABLE            = 0x0005000F,
      RANGESERVER_TIMESTAMP_ORDER_ERROR  = 0x00050010,
      RANGESERVER_SPLIT_ERROR            = 0x00050011,

      HQL_BAD_LOAD_FILE_FORMAT  = 0x00060001
    };

    const char *get_text(int error);

  } // namespace Error

  /**
   * This is a generic exception class for Hypertable.  It takes an error code
   * as a constructor argument and translates it into an error message.
   */
  class Exception : public std::runtime_error {
  public:
    Exception(int error) :
              std::runtime_error(Error::get_text(error)),
              m_error(error) { return; }
    Exception(int error, const String &msg) :
              std::runtime_error(msg), m_error(error) { return; }
    int code() { return m_error; }
  private:
    int m_error;
  };

} // namespace Hypertable

#endif // HYPERTABLE_ERROR_H
