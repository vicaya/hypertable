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

using namespace hypertable;
using namespace hypertable::DfsBroker;
using namespace std;


namespace hypertable {

  namespace DfsBroker {

    const char *Protocol::msCommandStrings[COMMAND_MAX] = {
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
      "rmdir"
    };


    /**
     *
     */
    CommBuf *Protocol::CreateOpenRequest(std::string &fname, uint32_t bufferSize) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER);
      hbuilder.AssignUniqueId();
      CommBuf *cbuf = new CommBuf(hbuilder, sizeof(RequestHeaderOpenT) + Serialization::EncodedLengthString(fname));

      RequestHeaderOpenT *openHeader = (RequestHeaderOpenT *)cbuf->GetDataPtr();
      openHeader->hdr.command = COMMAND_OPEN;
      openHeader->bufferSize = bufferSize;
      cbuf->AdvanceDataPtr(sizeof(RequestHeaderOpenT));
      cbuf->AppendString(fname);
      return cbuf;
    }


    /**
     */
    CommBuf *Protocol::CreateCreateRequest(std::string &fname, bool overwrite, int32_t bufferSize,
					   int32_t replication, int64_t blockSize) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER);
      hbuilder.AssignUniqueId();
      CommBuf *cbuf = new CommBuf(hbuilder, sizeof(RequestHeaderCreateT) + Serialization::EncodedLengthString(fname));

      RequestHeaderCreateT *createHeader = (RequestHeaderCreateT *)cbuf->GetDataPtr();
      createHeader->hdr.command = COMMAND_CREATE;
      createHeader->overwrite = (overwrite) ? 1 : 0;
      createHeader->replication = replication;
      createHeader->bufferSize = bufferSize;
      createHeader->blockSize = blockSize;
      cbuf->AdvanceDataPtr(sizeof(RequestHeaderCreateT));
      cbuf->AppendString(fname);
      return cbuf;
    }



    /**
     *
     */
    CommBuf *Protocol::CreateCloseRequest(int32_t fd) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER, fd);
      hbuilder.AssignUniqueId();
      CommBuf *cbuf = new CommBuf(hbuilder, sizeof(RequestHeaderCloseT));
      RequestHeaderCloseT *closeHeader = (RequestHeaderCloseT *)cbuf->GetDataPtr();
      closeHeader->hdr.command = COMMAND_CLOSE;
      closeHeader->fd = fd;
      return cbuf;
    }



    CommBuf *Protocol::CreateReadRequest(int32_t fd, uint32_t amount) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER, fd);
      hbuilder.AssignUniqueId();
      CommBuf *cbuf = new CommBuf(hbuilder, sizeof(RequestHeaderReadT));
      RequestHeaderReadT *readHeader = (RequestHeaderReadT *)cbuf->GetDataPtr();
      readHeader->hdr.command = COMMAND_READ;
      readHeader->fd = fd;
      readHeader->amount = amount;
      return cbuf;
    }


    CommBuf *Protocol::CreateAppendRequest(int32_t fd, const void *buf, uint32_t amount) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER, fd);
      hbuilder.AssignUniqueId();
      CommBuf *cbuf = new CommBuf(hbuilder, sizeof(RequestHeaderAppendT), buf, amount);
      RequestHeaderAppendT *appendHeader = (RequestHeaderAppendT *)cbuf->GetDataPtr();
      appendHeader->hdr.command = COMMAND_APPEND;
      appendHeader->fd = fd;
      appendHeader->amount = amount;
      return cbuf;
    }


    CommBuf *Protocol::CreateSeekRequest(int32_t fd, uint64_t offset) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER, fd);
      hbuilder.AssignUniqueId();
      CommBuf *cbuf = new CommBuf(hbuilder, sizeof(RequestHeaderSeekT));
      RequestHeaderSeekT *seekHeader = (RequestHeaderSeekT *)cbuf->GetDataPtr();
      seekHeader->hdr.command = COMMAND_SEEK;
      seekHeader->fd = fd;
      seekHeader->offset = offset;
      return cbuf;
    }

    /**
     *
     */
    CommBuf *Protocol::CreateRemoveRequest(std::string &fname) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER);
      hbuilder.AssignUniqueId();
      CommBuf *cbuf = new CommBuf(hbuilder, sizeof(RequestHeaderRemoveT) + Serialization::EncodedLengthString(fname));

      RequestHeaderRemoveT *removeHeader = (RequestHeaderRemoveT *)cbuf->GetDataPtr();
      removeHeader->hdr.command = COMMAND_REMOVE;
      cbuf->AdvanceDataPtr(sizeof(RequestHeaderRemoveT));
      cbuf->AppendString(fname);
      return cbuf;
    }


    /**
     *
     */
    CommBuf *Protocol::CreateShutdownRequest(uint16_t flags) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER);
      hbuilder.AssignUniqueId();
      CommBuf *cbuf = new CommBuf(hbuilder, 4);
      cbuf->AppendShort(COMMAND_SHUTDOWN);
      cbuf->AppendShort(flags);
      return cbuf;
    }


    /**
     *
     */
    CommBuf *Protocol::CreateStatusRequest() {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER);
      hbuilder.AssignUniqueId();
      CommBuf *cbuf = new CommBuf(hbuilder, 2);
      cbuf->AppendShort(COMMAND_STATUS);
      return cbuf;
    }


    /**
     * 
     */
    CommBuf *Protocol::CreateLengthRequest(std::string &fname) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER);
      hbuilder.AssignUniqueId();
      CommBuf *cbuf = new CommBuf(hbuilder, sizeof(RequestHeaderLengthT) + Serialization::EncodedLengthString(fname));
      RequestHeaderLengthT *lengthHeader = (RequestHeaderLengthT *)cbuf->GetDataPtr();
      lengthHeader->hdr.command = COMMAND_LENGTH;
      cbuf->AdvanceDataPtr(sizeof(RequestHeaderLengthT));
      cbuf->AppendString(fname);
      return cbuf;
    }


    /**
     * 
     */
    CommBuf *Protocol::CreatePositionReadRequest(int32_t fd, uint64_t offset, uint32_t amount) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER, fd);
      hbuilder.AssignUniqueId();
      CommBuf *cbuf = new CommBuf(hbuilder, sizeof(RequestHeaderPositionReadT));
      RequestHeaderPositionReadT *preadHeader = (RequestHeaderPositionReadT *)cbuf->GetDataPtr();
      preadHeader->hdr.command = COMMAND_PREAD;
      preadHeader->fd = fd;
      preadHeader->offset = offset;
      preadHeader->amount = amount;
      return cbuf;
    }

    /**
     *
     */
    CommBuf *Protocol::CreateMkdirsRequest(std::string &fname) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER);
      hbuilder.AssignUniqueId();
      CommBuf *cbuf = new CommBuf(hbuilder, sizeof(RequestHeaderMkdirsT) + Serialization::EncodedLengthString(fname));
      RequestHeaderMkdirsT *removeHeader = (RequestHeaderMkdirsT *)cbuf->GetDataPtr();
      removeHeader->hdr.command = COMMAND_MKDIRS;
      cbuf->AdvanceDataPtr(sizeof(RequestHeaderMkdirsT));
      cbuf->AppendString(fname);
      return cbuf;
    }

    /**
     *
     */
    CommBuf *Protocol::CreateFlushRequest(int32_t fd) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER, fd);
      hbuilder.AssignUniqueId();
      CommBuf *cbuf = new CommBuf(hbuilder, sizeof(RequestHeaderFlushT));
      RequestHeaderFlushT *flushHeader = (RequestHeaderFlushT *)cbuf->GetDataPtr();
      flushHeader->hdr.command = COMMAND_FLUSH;
      flushHeader->fd = fd;
      return cbuf;
    }

    /**
     *
     */
    CommBuf *Protocol::CreateRmdirRequest(std::string &fname) {
      HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER);
      hbuilder.AssignUniqueId();
      CommBuf *cbuf = new CommBuf(hbuilder, sizeof(RequestHeaderRmdirT) + Serialization::EncodedLengthString(fname));
      RequestHeaderRmdirT *removeHeader = (RequestHeaderRmdirT *)cbuf->GetDataPtr();
      removeHeader->hdr.command = COMMAND_RMDIR;
      cbuf->AdvanceDataPtr(sizeof(RequestHeaderRmdirT));
      cbuf->AppendString(fname);
      return cbuf;
    }


    const char *Protocol::CommandText(short command) {
      if (command < 0 || command >= COMMAND_MAX)
	return "UNKNOWN";
      return msCommandStrings[command];
    }

  }
}
