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


#include <cassert>
#include <iostream>

#include "AsyncComm/MessageBuilderSimple.h"
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
    MessageBuilderSimple mbuilder;
    CommBuf *cbuf = new CommBuf(mbuilder.HeaderLength() + sizeof(RequestHeaderOpenT) + CommBuf::EncodedLength(fname));

    cbuf->PrependString(fname);

    RequestHeaderOpenT *openHeader = (RequestHeaderOpenT *)cbuf->AllocateSpace(sizeof(RequestHeaderOpenT));
    openHeader->hdr.command = COMMAND_OPEN;
    openHeader->bufferSize = bufferSize;

    mbuilder.Reset(Message::PROTOCOL_HDFS);
    mbuilder.Encapsulate(cbuf);

    return cbuf;
  }


  /**
   */
  CommBuf *HdfsProtocol::CreateCreateRequest(const char *fname, bool overwrite, int32_t bufferSize,
					 int32_t replication, int64_t blockSize) {
    MessageBuilderSimple mbuilder;
    CommBuf *cbuf = new CommBuf(mbuilder.HeaderLength() + sizeof(RequestHeaderCreateT) + strlen(fname));

    cbuf->PrependData(fname, strlen(fname));

    RequestHeaderCreateT *createHeader = (RequestHeaderCreateT *)cbuf->AllocateSpace(sizeof(RequestHeaderCreateT));
    createHeader->hdr.command = COMMAND_CREATE;
    createHeader->overwrite = (overwrite) ? 1 : 0;
    createHeader->replication = replication;
    createHeader->bufferSize = bufferSize;
    createHeader->blockSize = blockSize;

    mbuilder.Reset(Message::PROTOCOL_HDFS);
    mbuilder.Encapsulate(cbuf);

    return cbuf;
  }



  /**
   *
   */
  CommBuf *HdfsProtocol::CreateCloseRequest(int32_t fd) {
    MessageBuilderSimple mbuilder;
    CommBuf *cbuf = new CommBuf(mbuilder.HeaderLength() + sizeof(RequestHeaderCloseT));
  
    RequestHeaderCloseT *closeHeader = (RequestHeaderCloseT *)cbuf->AllocateSpace(sizeof(RequestHeaderCloseT));
    closeHeader->hdr.command = COMMAND_CLOSE;
    closeHeader->fd = fd;

    mbuilder.Reset(Message::PROTOCOL_HDFS);
    mbuilder.Encapsulate(cbuf);

    return cbuf;
  }



  CommBuf *HdfsProtocol::CreateReadRequest(int32_t fd, uint32_t amount) {
    MessageBuilderSimple mbuilder;
    CommBuf *cbuf = new CommBuf(mbuilder.HeaderLength() + sizeof(RequestHeaderReadT));
  
    RequestHeaderReadT *readHeader = (RequestHeaderReadT *)cbuf->AllocateSpace(sizeof(RequestHeaderReadT));
    readHeader->hdr.command = COMMAND_READ;
    readHeader->fd = fd;
    readHeader->amount = amount;

    mbuilder.Reset(Message::PROTOCOL_HDFS);
    mbuilder.Encapsulate(cbuf);

    return cbuf;
  }


  CommBuf *HdfsProtocol::CreateWriteRequest(int32_t fd, uint8_t *buf, uint32_t amount) {
    MessageBuilderSimple mbuilder;
    CommBuf *cbuf = new CommBuf(mbuilder.HeaderLength() + sizeof(RequestHeaderWriteT));

    cbuf->SetExt(buf, amount);
  
    RequestHeaderWriteT *writeHeader = (RequestHeaderWriteT *)cbuf->AllocateSpace(sizeof(RequestHeaderWriteT));
    writeHeader->hdr.command = COMMAND_WRITE;
    writeHeader->fd = fd;
    writeHeader->amount = amount;

    mbuilder.Reset(Message::PROTOCOL_HDFS);
    mbuilder.Encapsulate(cbuf);

    return cbuf;
  }


  CommBuf *HdfsProtocol::CreateSeekRequest(int32_t fd, uint64_t offset) {
    MessageBuilderSimple mbuilder;
    CommBuf *cbuf = new CommBuf(mbuilder.HeaderLength() + sizeof(RequestHeaderSeekT));
  
    RequestHeaderSeekT *seekHeader = (RequestHeaderSeekT *)cbuf->AllocateSpace(sizeof(RequestHeaderSeekT));
    seekHeader->hdr.command = COMMAND_SEEK;
    seekHeader->fd = fd;
    seekHeader->offset = offset;

    mbuilder.Reset(Message::PROTOCOL_HDFS);
    mbuilder.Encapsulate(cbuf);

    return cbuf;
  }

  /**
   *
   */
  CommBuf *HdfsProtocol::CreateRemoveRequest(const char *fname) {
    MessageBuilderSimple mbuilder;
    CommBuf *cbuf = new CommBuf(mbuilder.HeaderLength() + sizeof(RequestHeaderRemoveT) + CommBuf::EncodedLength(fname));

    cbuf->PrependString(fname);

    RequestHeaderRemoveT *removeHeader = (RequestHeaderRemoveT *)cbuf->AllocateSpace(sizeof(RequestHeaderRemoveT));
    removeHeader->hdr.command = COMMAND_REMOVE;

    mbuilder.Reset(Message::PROTOCOL_HDFS);
    mbuilder.Encapsulate(cbuf);

    return cbuf;
  }


  /**
   *
   */
  CommBuf *HdfsProtocol::CreateShutdownRequest(uint16_t flags) {
    MessageBuilderSimple mbuilder;
    CommBuf *cbuf = new CommBuf(mbuilder.HeaderLength() + 4);

    cbuf->PrependShort(flags);
    cbuf->PrependShort(COMMAND_SHUTDOWN);

    mbuilder.Reset(Message::PROTOCOL_HDFS);
    mbuilder.Encapsulate(cbuf);

    return cbuf;
  }


  /**
   *
   */
  CommBuf *HdfsProtocol::CreateLengthRequest(const char *fname) {
    MessageBuilderSimple mbuilder;
    CommBuf *cbuf = new CommBuf(mbuilder.HeaderLength() + sizeof(RequestHeaderLengthT) + CommBuf::EncodedLength(fname));

    cbuf->PrependString(fname);

    RequestHeaderLengthT *lengthHeader = (RequestHeaderLengthT *)cbuf->AllocateSpace(sizeof(RequestHeaderLengthT));
    lengthHeader->hdr.command = COMMAND_LENGTH;

    mbuilder.Reset(Message::PROTOCOL_HDFS);
    mbuilder.Encapsulate(cbuf);

    return cbuf;
  }

  CommBuf *HdfsProtocol::CreatePositionReadRequest(int32_t fd, uint64_t offset, uint32_t amount) {
    MessageBuilderSimple mbuilder;
    CommBuf *cbuf = new CommBuf(mbuilder.HeaderLength() + sizeof(RequestHeaderPositionReadT));
  
    RequestHeaderPositionReadT *preadHeader = (RequestHeaderPositionReadT *)cbuf->AllocateSpace(sizeof(RequestHeaderPositionReadT));
    preadHeader->hdr.command = COMMAND_PREAD;
    preadHeader->fd = fd;
    preadHeader->offset = offset;
    preadHeader->amount = amount;

    mbuilder.Reset(Message::PROTOCOL_HDFS);
    mbuilder.Encapsulate(cbuf);

    return cbuf;
  }

  /**
   *
   */
  CommBuf *HdfsProtocol::CreateMkdirsRequest(const char *fname) {
    MessageBuilderSimple mbuilder;
    CommBuf *cbuf = new CommBuf(mbuilder.HeaderLength() + sizeof(RequestHeaderMkdirsT) + CommBuf::EncodedLength(fname));

    cbuf->PrependString(fname);

    RequestHeaderMkdirsT *removeHeader = (RequestHeaderMkdirsT *)cbuf->AllocateSpace(sizeof(RequestHeaderMkdirsT));
    removeHeader->hdr.command = COMMAND_MKDIRS;

    mbuilder.Reset(Message::PROTOCOL_HDFS);
    mbuilder.Encapsulate(cbuf);

    return cbuf;
  }


  const char *HdfsProtocol::CommandText(short command) {
    if (command < 0 || command >= COMMAND_MAX)
      return "UNKNOWN";
    return msCommandStrings[command];
  }

}
