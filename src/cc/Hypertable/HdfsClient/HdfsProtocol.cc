/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * This program is distributed in the hope that it will be useful,
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
#include "Common/Error.h"

#include "HdfsProtocol.h"

using namespace hypertable;
using namespace std;

namespace hypertable {

  const char *HdfsProtocol::msCommandStrings[COMMAND_MAX] = {
    "open",
    "create",
    "close",
    "read",
    "write",
    "seek",
    "remove",
    "shutdown",
    "length",
    "pread",
    "mkdirs"
  };


  /**
   *
   */
  CommBuf *HdfsProtocol::CreateOpenRequest(const char *fname, uint32_t bufferSize) {
    HeaderBuilder hbuilder;
    CommBuf *cbuf = new CommBuf(hbuilder.HeaderLength() + sizeof(RequestHeaderOpenT) + CommBuf::EncodedLength(fname));

    cbuf->PrependString(fname);

    RequestHeaderOpenT *openHeader = (RequestHeaderOpenT *)cbuf->AllocateSpace(sizeof(RequestHeaderOpenT));
    openHeader->hdr.command = COMMAND_OPEN;
    openHeader->bufferSize = bufferSize;

    hbuilder.Reset(Header::PROTOCOL_HDFS);
    hbuilder.Encapsulate(cbuf);

    return cbuf;
  }


  /**
   */
  CommBuf *HdfsProtocol::CreateCreateRequest(const char *fname, bool overwrite, int32_t bufferSize,
					 int32_t replication, int64_t blockSize) {
    HeaderBuilder hbuilder;
    CommBuf *cbuf = new CommBuf(hbuilder.HeaderLength() + sizeof(RequestHeaderCreateT) + strlen(fname));

    cbuf->PrependData(fname, strlen(fname));

    RequestHeaderCreateT *createHeader = (RequestHeaderCreateT *)cbuf->AllocateSpace(sizeof(RequestHeaderCreateT));
    createHeader->hdr.command = COMMAND_CREATE;
    createHeader->overwrite = (overwrite) ? 1 : 0;
    createHeader->replication = replication;
    createHeader->bufferSize = bufferSize;
    createHeader->blockSize = blockSize;

    hbuilder.Reset(Header::PROTOCOL_HDFS);
    hbuilder.Encapsulate(cbuf);

    return cbuf;
  }



  /**
   *
   */
  CommBuf *HdfsProtocol::CreateCloseRequest(int32_t fd) {
    HeaderBuilder hbuilder;
    CommBuf *cbuf = new CommBuf(hbuilder.HeaderLength() + sizeof(RequestHeaderCloseT));
  
    RequestHeaderCloseT *closeHeader = (RequestHeaderCloseT *)cbuf->AllocateSpace(sizeof(RequestHeaderCloseT));
    closeHeader->hdr.command = COMMAND_CLOSE;
    closeHeader->fd = fd;

    hbuilder.Reset(Header::PROTOCOL_HDFS);
    hbuilder.SetGroupId(fd);
    hbuilder.Encapsulate(cbuf);

    return cbuf;
  }



  CommBuf *HdfsProtocol::CreateReadRequest(int32_t fd, uint32_t amount) {
    HeaderBuilder hbuilder;
    CommBuf *cbuf = new CommBuf(hbuilder.HeaderLength() + sizeof(RequestHeaderReadT));
  
    RequestHeaderReadT *readHeader = (RequestHeaderReadT *)cbuf->AllocateSpace(sizeof(RequestHeaderReadT));
    readHeader->hdr.command = COMMAND_READ;
    readHeader->fd = fd;
    readHeader->amount = amount;

    hbuilder.Reset(Header::PROTOCOL_HDFS);
    hbuilder.SetGroupId(fd);
    hbuilder.Encapsulate(cbuf);

    return cbuf;
  }


  CommBuf *HdfsProtocol::CreateWriteRequest(int32_t fd, uint8_t *buf, uint32_t amount) {
    HeaderBuilder hbuilder;
    CommBuf *cbuf = new CommBuf(hbuilder.HeaderLength() + sizeof(RequestHeaderWriteT));

    cbuf->SetExt(buf, amount);
  
    RequestHeaderWriteT *writeHeader = (RequestHeaderWriteT *)cbuf->AllocateSpace(sizeof(RequestHeaderWriteT));
    writeHeader->hdr.command = COMMAND_WRITE;
    writeHeader->fd = fd;
    writeHeader->amount = amount;

    hbuilder.Reset(Header::PROTOCOL_HDFS);
    hbuilder.SetGroupId(fd);
    hbuilder.Encapsulate(cbuf);

    return cbuf;
  }


  CommBuf *HdfsProtocol::CreateSeekRequest(int32_t fd, uint64_t offset) {
    HeaderBuilder hbuilder;
    CommBuf *cbuf = new CommBuf(hbuilder.HeaderLength() + sizeof(RequestHeaderSeekT));
  
    RequestHeaderSeekT *seekHeader = (RequestHeaderSeekT *)cbuf->AllocateSpace(sizeof(RequestHeaderSeekT));
    seekHeader->hdr.command = COMMAND_SEEK;
    seekHeader->fd = fd;
    seekHeader->offset = offset;

    hbuilder.Reset(Header::PROTOCOL_HDFS);
    hbuilder.SetGroupId(fd);
    hbuilder.Encapsulate(cbuf);

    return cbuf;
  }

  /**
   *
   */
  CommBuf *HdfsProtocol::CreateRemoveRequest(const char *fname) {
    HeaderBuilder hbuilder;
    CommBuf *cbuf = new CommBuf(hbuilder.HeaderLength() + sizeof(RequestHeaderRemoveT) + CommBuf::EncodedLength(fname));

    cbuf->PrependString(fname);

    RequestHeaderRemoveT *removeHeader = (RequestHeaderRemoveT *)cbuf->AllocateSpace(sizeof(RequestHeaderRemoveT));
    removeHeader->hdr.command = COMMAND_REMOVE;

    hbuilder.Reset(Header::PROTOCOL_HDFS);
    hbuilder.Encapsulate(cbuf);

    return cbuf;
  }


  /**
   *
   */
  CommBuf *HdfsProtocol::CreateShutdownRequest(uint16_t flags) {
    HeaderBuilder hbuilder;
    CommBuf *cbuf = new CommBuf(hbuilder.HeaderLength() + 4);

    cbuf->PrependShort(flags);
    cbuf->PrependShort(COMMAND_SHUTDOWN);

    hbuilder.Reset(Header::PROTOCOL_HDFS);
    hbuilder.Encapsulate(cbuf);

    return cbuf;
  }


  /**
   *
   */
  CommBuf *HdfsProtocol::CreateLengthRequest(const char *fname) {
    HeaderBuilder hbuilder;
    CommBuf *cbuf = new CommBuf(hbuilder.HeaderLength() + sizeof(RequestHeaderLengthT) + CommBuf::EncodedLength(fname));

    cbuf->PrependString(fname);

    RequestHeaderLengthT *lengthHeader = (RequestHeaderLengthT *)cbuf->AllocateSpace(sizeof(RequestHeaderLengthT));
    lengthHeader->hdr.command = COMMAND_LENGTH;

    hbuilder.Reset(Header::PROTOCOL_HDFS);
    hbuilder.Encapsulate(cbuf);

    return cbuf;
  }

  CommBuf *HdfsProtocol::CreatePositionReadRequest(int32_t fd, uint64_t offset, uint32_t amount) {
    HeaderBuilder hbuilder;
    CommBuf *cbuf = new CommBuf(hbuilder.HeaderLength() + sizeof(RequestHeaderPositionReadT));
  
    RequestHeaderPositionReadT *preadHeader = (RequestHeaderPositionReadT *)cbuf->AllocateSpace(sizeof(RequestHeaderPositionReadT));
    preadHeader->hdr.command = COMMAND_PREAD;
    preadHeader->fd = fd;
    preadHeader->offset = offset;
    preadHeader->amount = amount;

    hbuilder.Reset(Header::PROTOCOL_HDFS);
    hbuilder.Encapsulate(cbuf);

    return cbuf;
  }

  /**
   *
   */
  CommBuf *HdfsProtocol::CreateMkdirsRequest(const char *fname) {
    HeaderBuilder hbuilder;
    CommBuf *cbuf = new CommBuf(hbuilder.HeaderLength() + sizeof(RequestHeaderMkdirsT) + CommBuf::EncodedLength(fname));

    cbuf->PrependString(fname);

    RequestHeaderMkdirsT *removeHeader = (RequestHeaderMkdirsT *)cbuf->AllocateSpace(sizeof(RequestHeaderMkdirsT));
    removeHeader->hdr.command = COMMAND_MKDIRS;

    hbuilder.Reset(Header::PROTOCOL_HDFS);
    hbuilder.Encapsulate(cbuf);

    return cbuf;
  }


  const char *HdfsProtocol::CommandText(short command) {
    if (command < 0 || command >= COMMAND_MAX)
      return "UNKNOWN";
    return msCommandStrings[command];
  }

}
