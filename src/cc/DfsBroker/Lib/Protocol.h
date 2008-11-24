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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#ifndef HYPERTABLE_DFSBROKER_PROTOCOL_H
#define HYPERTABLE_DFSBROKER_PROTOCOL_H

#include "AsyncComm/CommBuf.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/Protocol.h"

#include "Common/StaticBuffer.h"
#include "Common/String.h"

namespace Hypertable {

  namespace DfsBroker {

    class Protocol : public Hypertable::Protocol {

    public:

      static CommBuf *create_open_request(const String &fname,
                                          uint32_t bufsz=0);

      static CommBuf *create_create_request(const String &fname,
                                            bool overwrite,
                                            int32_t bufsz,
                                            int32_t replication,
                                            int64_t blksz);

      static CommBuf *create_close_request(int32_t fd);

      static CommBuf *create_read_request(int32_t fd, uint32_t amount);

      static CommBuf *create_append_request(int32_t fd, StaticBuffer &buffer,
                                            bool flush = false);

      static CommBuf *create_seek_request(int32_t fd, uint64_t offset);

      static CommBuf *create_remove_request(const String &fname);

      static CommBuf *create_length_request(const String &fname);

      static CommBuf *create_position_read_request(int32_t fd, uint64_t offset,
                                                   uint32_t amount);

      static CommBuf *create_mkdirs_request(const String &fname);

      static CommBuf *create_rmdir_request(const String &fname);

      static CommBuf *create_readdir_request(const String &fname);

      static CommBuf *create_flush_request(int32_t fd);

      static CommBuf *create_status_request();

      static CommBuf *create_shutdown_request(uint16_t flags);

      static CommBuf *create_exists_request(const String &fname);

      static CommBuf *create_rename_request(const String &src,
                                            const String &dst);

      virtual const char *command_text(uint64_t command);

      static const uint64_t COMMAND_OPEN     = 0;
      static const uint64_t COMMAND_CREATE   = 1;
      static const uint64_t COMMAND_CLOSE    = 2;
      static const uint64_t COMMAND_READ     = 3;
      static const uint64_t COMMAND_APPEND   = 4;
      static const uint64_t COMMAND_SEEK     = 5;
      static const uint64_t COMMAND_REMOVE   = 6;
      static const uint64_t COMMAND_SHUTDOWN = 7;
      static const uint64_t COMMAND_LENGTH   = 8;
      static const uint64_t COMMAND_PREAD    = 9;
      static const uint64_t COMMAND_MKDIRS   = 10;
      static const uint64_t COMMAND_STATUS   = 11;
      static const uint64_t COMMAND_FLUSH    = 12;
      static const uint64_t COMMAND_RMDIR    = 13;
      static const uint64_t COMMAND_READDIR  = 14;
      static const uint64_t COMMAND_EXISTS   = 15;
      static const uint64_t COMMAND_RENAME   = 16;
      static const uint64_t COMMAND_MAX      = 17;

      static const uint16_t SHUTDOWN_FLAG_IMMEDIATE = 0x0001;

      static const char * ms_command_strings[COMMAND_MAX];

    };

  }

}

#endif // HYPERTABLE_DFSBROKER_PROTOCOL_H
