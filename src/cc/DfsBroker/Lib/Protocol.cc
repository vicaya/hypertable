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

#include "Common/Compat.h"

#include <cassert>
#include <iostream>

#include "Common/Serialization.h"
#include "Common/Logger.h"

#include "AsyncComm/CommHeader.h"


#include "Protocol.h"

using namespace std;
using namespace Hypertable;
using namespace DfsBroker;
using namespace Serialization;


namespace Hypertable {

  namespace DfsBroker {

    const char *Protocol::ms_command_strings[COMMAND_MAX] = {
      "open",
      "create",
      "close",
      "read",
      "append",
      "seek",
      "remove",
      "shutdown",
      "length",
      "pread",
      "mkdirs",
      "status",
      "flush",
      "rmdir",
      "readdir",
      "exists",
      "rename"
    };


    /**
     *
     */
    CommBuf *
    Protocol::create_open_request(const String &fname, uint32_t bufsz) {
      CommHeader header(COMMAND_OPEN);
      CommBuf *cbuf = new CommBuf(header, 4 + encoded_length_str16(fname));
      cbuf->append_i32(bufsz);
      cbuf->append_str16(fname);
      return cbuf;
    }


    /**
     */
    CommBuf *
    Protocol::create_create_request(const String &fname, bool overwrite,
        int32_t bufsz, int32_t replication, int64_t blksz) {
      CommHeader header(COMMAND_CREATE);
      CommBuf *cbuf = new CommBuf(header, 18 + encoded_length_str16(fname));
      cbuf->append_i16((overwrite) ? 1 : 0);
      cbuf->append_i32(replication);
      cbuf->append_i32(bufsz);
      cbuf->append_i64(blksz);
      cbuf->append_str16(fname);
      return cbuf;
    }


    /**
     */
    CommBuf *Protocol::create_close_request(int32_t fd) {
      CommHeader header(COMMAND_CLOSE);
      header.gid = fd;
      CommBuf *cbuf = new CommBuf(header, 4);
      cbuf->append_i32(fd);
      return cbuf;
    }



    CommBuf *Protocol::create_read_request(int32_t fd, uint32_t amount) {
      CommHeader header(COMMAND_READ);
      header.gid = fd;
      CommBuf *cbuf = new CommBuf(header, 8);
      cbuf->append_i32(fd);
      cbuf->append_i32(amount);
      return cbuf;
    }


    CommBuf *Protocol::create_append_request(int32_t fd, StaticBuffer &buffer,
                                             bool flush) {
      CommHeader header(COMMAND_APPEND);
      header.gid = fd;
      CommBuf *cbuf = new CommBuf(header, 9, buffer);
      cbuf->append_i32(fd);
      cbuf->append_i32(buffer.size);
      cbuf->append_bool(flush);
      return cbuf;
    }


    CommBuf *Protocol::create_seek_request(int32_t fd, uint64_t offset) {
      CommHeader header(COMMAND_SEEK);
      header.gid = fd;
      CommBuf *cbuf = new CommBuf(header, 12);
      cbuf->append_i32(fd);
      cbuf->append_i64(offset);
      return cbuf;
    }

    /**
     */
    CommBuf *Protocol::create_remove_request(const String &fname) {
      CommHeader header(COMMAND_REMOVE);
      CommBuf *cbuf = new CommBuf(header, encoded_length_str16(fname));
      cbuf->append_str16(fname);
      return cbuf;
    }


    /**
     */
    CommBuf *Protocol::create_length_request(const String &fname) {
      CommHeader header(COMMAND_LENGTH);
      CommBuf *cbuf = new CommBuf(header, encoded_length_str16(fname));
      cbuf->append_str16(fname);
      return cbuf;
    }


    /**
     */
    CommBuf *
    Protocol::create_position_read_request(int32_t fd, uint64_t offset,
                                           uint32_t amount) {
      CommHeader header(COMMAND_PREAD);
      header.gid = fd;
      CommBuf *cbuf = new CommBuf(header, 16);
      cbuf->append_i32(fd);
      cbuf->append_i64(offset);
      cbuf->append_i32(amount);
      return cbuf;
    }

    /**
     */
    CommBuf *Protocol::create_mkdirs_request(const String &fname) {
      CommHeader header(COMMAND_MKDIRS);
      CommBuf *cbuf = new CommBuf(header, encoded_length_str16(fname));
      cbuf->append_str16(fname);
      return cbuf;
    }

    /**
     */
    CommBuf *Protocol::create_flush_request(int32_t fd) {
      CommHeader header(COMMAND_FLUSH);
      header.gid = fd;
      CommBuf *cbuf = new CommBuf(header, 4);
      cbuf->append_i32(fd);
      return cbuf;
    }

    /**
     */
    CommBuf *Protocol::create_rmdir_request(const String &fname) {
      CommHeader header(COMMAND_RMDIR);
      CommBuf *cbuf = new CommBuf(header, encoded_length_str16(fname));
      cbuf->append_str16(fname);
      return cbuf;
    }

    /**
     */
    CommBuf *Protocol::create_readdir_request(const String &fname) {
      CommHeader header(COMMAND_READDIR);
      CommBuf *cbuf = new CommBuf(header, encoded_length_str16(fname));
      cbuf->append_str16(fname);
      return cbuf;
    }

    /**
     *
     */
    CommBuf *Protocol::create_shutdown_request(uint16_t flags) {
      CommHeader header(COMMAND_SHUTDOWN);
      CommBuf *cbuf = new CommBuf(header, 2);
      cbuf->append_i16(flags);
      return cbuf;
    }


    /**
     *
     */
    CommBuf *Protocol::create_status_request() {
      CommHeader header(COMMAND_STATUS);
      CommBuf *cbuf = new CommBuf(header);
      return cbuf;
    }


    /**
     */
    CommBuf *Protocol::create_exists_request(const String &fname) {
      CommHeader header(COMMAND_EXISTS);
      CommBuf *cbuf = new CommBuf(header, encoded_length_str16(fname));
      cbuf->append_str16(fname);
      return cbuf;
    }


    CommBuf *
    Protocol::create_rename_request(const String &src, const String &dst) {
      CommHeader header(COMMAND_RENAME);
      CommBuf *cbuf = new CommBuf(header, encoded_length_str16(src) +
                                  encoded_length_str16(dst));
      cbuf->append_str16(src);
      cbuf->append_str16(dst);
      return cbuf;
    }


    const char *Protocol::command_text(uint64_t command) {
      if (command < 0 || command >= COMMAND_MAX)
        return "UNKNOWN";
      return ms_command_strings[command];
    }

  }
}
