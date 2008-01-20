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
#include <iostream>

#include "AsyncComm/HeaderBuilder.h"
#include "AsyncComm/Serialization.h"

#include "Protocol.h"

using namespace Hypertable;
using namespace Hypertable::DfsBroker;
using namespace std;


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
      "readdir"
    };


    /**
     *
     */
    CommBuf *Protocol::create_open_request(std::string &fname, uint32_t bufferSize) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER);
      CommBuf *cbuf = new CommBuf(hbuilder, 6 + Serialization::encoded_length_string(fname));
      cbuf->append_short(COMMAND_OPEN);
      cbuf->append_int(bufferSize);
      cbuf->append_string(fname);
      return cbuf;
    }


    /**
     */
    CommBuf *Protocol::create_create_request(std::string &fname, bool overwrite, int32_t bufferSize,
					   int32_t replication, int64_t blockSize) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER);
      CommBuf *cbuf = new CommBuf(hbuilder, 20 + Serialization::encoded_length_string(fname));
      cbuf->append_short(COMMAND_CREATE);
      cbuf->append_short( (overwrite) ? 1 : 0 );
      cbuf->append_int(replication);
      cbuf->append_int(bufferSize);
      cbuf->append_long(blockSize);
      cbuf->append_string(fname);
      return cbuf;
    }


    /**
     */
    CommBuf *Protocol::create_close_request(int32_t fd) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER, fd);
      CommBuf *cbuf = new CommBuf(hbuilder, 6);
      cbuf->append_short(COMMAND_CLOSE);
      cbuf->append_int(fd);
      return cbuf;
    }



    CommBuf *Protocol::create_read_request(int32_t fd, uint32_t amount) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER, fd);
      CommBuf *cbuf = new CommBuf(hbuilder, 10);
      cbuf->append_short(COMMAND_READ);
      cbuf->append_int(fd);
      cbuf->append_int(amount);
      return cbuf;
    }


    CommBuf *Protocol::create_append_request(int32_t fd, const void *buf, uint32_t amount) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER, fd);
      CommBuf *cbuf = new CommBuf(hbuilder, 10, buf, amount);
      cbuf->append_short(COMMAND_APPEND);
      cbuf->append_int(fd);
      cbuf->append_int(amount);
      return cbuf;
    }


    CommBuf *Protocol::create_seek_request(int32_t fd, uint64_t offset) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER, fd);
      CommBuf *cbuf = new CommBuf(hbuilder, 14);
      cbuf->append_short(COMMAND_SEEK);
      cbuf->append_int(fd);
      cbuf->append_long(offset);
      return cbuf;
    }

    /**
     */
    CommBuf *Protocol::create_remove_request(std::string &fname) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER);
      CommBuf *cbuf = new CommBuf(hbuilder, 2 + Serialization::encoded_length_string(fname));
      cbuf->append_short(COMMAND_REMOVE);
      cbuf->append_string(fname);
      return cbuf;
    }


    /**
     */
    CommBuf *Protocol::create_length_request(std::string &fname) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER);
      CommBuf *cbuf = new CommBuf(hbuilder, 2 + Serialization::encoded_length_string(fname));
      cbuf->append_short(COMMAND_LENGTH);
      cbuf->append_string(fname);
      return cbuf;
    }


    /**
     */
    CommBuf *Protocol::create_position_read_request(int32_t fd, uint64_t offset, uint32_t amount) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER, fd);
      CommBuf *cbuf = new CommBuf(hbuilder, 18);
      cbuf->append_short(COMMAND_PREAD);
      cbuf->append_int(fd);
      cbuf->append_long(offset);
      cbuf->append_int(amount);
      return cbuf;
    }

    /**
     */
    CommBuf *Protocol::create_mkdirs_request(std::string &fname) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER);
      CommBuf *cbuf = new CommBuf(hbuilder, 2 + Serialization::encoded_length_string(fname));
      cbuf->append_short(COMMAND_MKDIRS);
      cbuf->append_string(fname);
      return cbuf;
    }

    /**
     */
    CommBuf *Protocol::create_flush_request(int32_t fd) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER, fd);
      CommBuf *cbuf = new CommBuf(hbuilder, 6);
      cbuf->append_short(COMMAND_FLUSH);
      cbuf->append_int(fd);
      return cbuf;
    }

    /**
     */
    CommBuf *Protocol::create_rmdir_request(std::string &fname) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER);
      CommBuf *cbuf = new CommBuf(hbuilder, 2 + Serialization::encoded_length_string(fname));
      cbuf->append_short(COMMAND_RMDIR);
      cbuf->append_string(fname);
      return cbuf;
    }

    /**
     */
    CommBuf *Protocol::create_readdir_request(std::string &fname) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER);
      CommBuf *cbuf = new CommBuf(hbuilder, 2 + Serialization::encoded_length_string(fname));
      cbuf->append_short(COMMAND_READDIR);
      cbuf->append_string(fname);
      return cbuf;
    }

    /**
     *
     */
    CommBuf *Protocol::create_shutdown_request(uint16_t flags) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER);
      CommBuf *cbuf = new CommBuf(hbuilder, 4);
      cbuf->append_short(COMMAND_SHUTDOWN);
      cbuf->append_short(flags);
      return cbuf;
    }


    /**
     *
     */
    CommBuf *Protocol::create_status_request() {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER);
      CommBuf *cbuf = new CommBuf(hbuilder, 2);
      cbuf->append_short(COMMAND_STATUS);
      return cbuf;
    }

    const char *Protocol::command_text(short command) {
      if (command < 0 || command >= COMMAND_MAX)
	return "UNKNOWN";
      return ms_command_strings[command];
    }

  }
}
