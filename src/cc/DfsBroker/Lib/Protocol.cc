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
      CommBuf *cbuf = new CommBuf(hbuilder, sizeof(RequestHeaderOpenT) + Serialization::encoded_length_string(fname));

      RequestHeaderOpenT *openHeader = (RequestHeaderOpenT *)cbuf->get_data_ptr();
      openHeader->hdr.command = COMMAND_OPEN;
      openHeader->bufferSize = bufferSize;
      cbuf->advance_data_ptr(sizeof(RequestHeaderOpenT));
      cbuf->append_string(fname);
      return cbuf;
    }


    /**
     */
    CommBuf *Protocol::create_create_request(std::string &fname, bool overwrite, int32_t bufferSize,
					   int32_t replication, int64_t blockSize) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER);
      CommBuf *cbuf = new CommBuf(hbuilder, sizeof(RequestHeaderCreateT) + Serialization::encoded_length_string(fname));

      RequestHeaderCreateT *createHeader = (RequestHeaderCreateT *)cbuf->get_data_ptr();
      createHeader->hdr.command = COMMAND_CREATE;
      createHeader->overwrite = (overwrite) ? 1 : 0;
      createHeader->replication = replication;
      createHeader->bufferSize = bufferSize;
      createHeader->blockSize = blockSize;
      cbuf->advance_data_ptr(sizeof(RequestHeaderCreateT));
      cbuf->append_string(fname);
      return cbuf;
    }



    /**
     *
     */
    CommBuf *Protocol::create_close_request(int32_t fd) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER, fd);
      CommBuf *cbuf = new CommBuf(hbuilder, sizeof(RequestHeaderCloseT));
      RequestHeaderCloseT *closeHeader = (RequestHeaderCloseT *)cbuf->get_data_ptr();
      closeHeader->hdr.command = COMMAND_CLOSE;
      closeHeader->fd = fd;
      return cbuf;
    }



    CommBuf *Protocol::create_read_request(int32_t fd, uint32_t amount) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER, fd);
      CommBuf *cbuf = new CommBuf(hbuilder, sizeof(RequestHeaderReadT));
      RequestHeaderReadT *readHeader = (RequestHeaderReadT *)cbuf->get_data_ptr();
      readHeader->hdr.command = COMMAND_READ;
      readHeader->fd = fd;
      readHeader->amount = amount;
      return cbuf;
    }


    CommBuf *Protocol::create_append_request(int32_t fd, const void *buf, uint32_t amount) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER, fd);
      CommBuf *cbuf = new CommBuf(hbuilder, sizeof(RequestHeaderAppendT), buf, amount);
      RequestHeaderAppendT *appendHeader = (RequestHeaderAppendT *)cbuf->get_data_ptr();
      appendHeader->hdr.command = COMMAND_APPEND;
      appendHeader->fd = fd;
      appendHeader->amount = amount;
      return cbuf;
    }


    CommBuf *Protocol::create_seek_request(int32_t fd, uint64_t offset) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER, fd);
      CommBuf *cbuf = new CommBuf(hbuilder, sizeof(RequestHeaderSeekT));
      RequestHeaderSeekT *seekHeader = (RequestHeaderSeekT *)cbuf->get_data_ptr();
      seekHeader->hdr.command = COMMAND_SEEK;
      seekHeader->fd = fd;
      seekHeader->offset = offset;
      return cbuf;
    }

    /**
     *
     */
    CommBuf *Protocol::create_remove_request(std::string &fname) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER);
      CommBuf *cbuf = new CommBuf(hbuilder, sizeof(RequestHeaderRemoveT) + Serialization::encoded_length_string(fname));

      RequestHeaderRemoveT *removeHeader = (RequestHeaderRemoveT *)cbuf->get_data_ptr();
      removeHeader->hdr.command = COMMAND_REMOVE;
      cbuf->advance_data_ptr(sizeof(RequestHeaderRemoveT));
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


    /**
     * 
     */
    CommBuf *Protocol::create_length_request(std::string &fname) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER);
      CommBuf *cbuf = new CommBuf(hbuilder, sizeof(RequestHeaderLengthT) + Serialization::encoded_length_string(fname));
      RequestHeaderLengthT *lengthHeader = (RequestHeaderLengthT *)cbuf->get_data_ptr();
      lengthHeader->hdr.command = COMMAND_LENGTH;
      cbuf->advance_data_ptr(sizeof(RequestHeaderLengthT));
      cbuf->append_string(fname);
      return cbuf;
    }


    /**
     * 
     */
    CommBuf *Protocol::create_position_read_request(int32_t fd, uint64_t offset, uint32_t amount) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER, fd);
      CommBuf *cbuf = new CommBuf(hbuilder, sizeof(RequestHeaderPositionReadT));
      RequestHeaderPositionReadT *preadHeader = (RequestHeaderPositionReadT *)cbuf->get_data_ptr();
      preadHeader->hdr.command = COMMAND_PREAD;
      preadHeader->fd = fd;
      preadHeader->offset = offset;
      preadHeader->amount = amount;
      return cbuf;
    }

    /**
     *
     */
    CommBuf *Protocol::create_mkdirs_request(std::string &fname) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER);
      CommBuf *cbuf = new CommBuf(hbuilder, sizeof(RequestHeaderMkdirsT) + Serialization::encoded_length_string(fname));
      RequestHeaderMkdirsT *removeHeader = (RequestHeaderMkdirsT *)cbuf->get_data_ptr();
      removeHeader->hdr.command = COMMAND_MKDIRS;
      cbuf->advance_data_ptr(sizeof(RequestHeaderMkdirsT));
      cbuf->append_string(fname);
      return cbuf;
    }

    /**
     *
     */
    CommBuf *Protocol::create_flush_request(int32_t fd) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER, fd);
      CommBuf *cbuf = new CommBuf(hbuilder, sizeof(RequestHeaderFlushT));
      RequestHeaderFlushT *flushHeader = (RequestHeaderFlushT *)cbuf->get_data_ptr();
      flushHeader->hdr.command = COMMAND_FLUSH;
      flushHeader->fd = fd;
      return cbuf;
    }

    /**
     *
     */
    CommBuf *Protocol::create_rmdir_request(std::string &fname) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER);
      CommBuf *cbuf = new CommBuf(hbuilder, sizeof(RequestHeaderRmdirT) + Serialization::encoded_length_string(fname));
      RequestHeaderRmdirT *removeHeader = (RequestHeaderRmdirT *)cbuf->get_data_ptr();
      removeHeader->hdr.command = COMMAND_RMDIR;
      cbuf->advance_data_ptr(sizeof(RequestHeaderRmdirT));
      cbuf->append_string(fname);
      return cbuf;
    }

    /**
     *
     */
    CommBuf *Protocol::create_readdir_request(std::string &fname) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER);
      CommBuf *cbuf = new CommBuf(hbuilder, sizeof(RequestHeaderReaddirT) + Serialization::encoded_length_string(fname));
      RequestHeaderReaddirT *readdirHeader = (RequestHeaderReaddirT *)cbuf->get_data_ptr();
      readdirHeader->hdr.command = COMMAND_READDIR;
      cbuf->advance_data_ptr(sizeof(RequestHeaderReaddirT));
      cbuf->append_string(fname);
      return cbuf;
    }


    const char *Protocol::command_text(short command) {
      if (command < 0 || command >= COMMAND_MAX)
	return "UNKNOWN";
      return ms_command_strings[command];
    }

  }
}
