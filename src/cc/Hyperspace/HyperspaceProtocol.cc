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

#include "Common/Error.h"
#include "AsyncComm/MessageBuilderSimple.h"

#include "HyperspaceProtocol.h"

using namespace hypertable;
using namespace std;

const char *HyperspaceProtocol::commandStrings[COMMAND_MAX] = {
  "create",
  "delete",
  "mkdirs",
  "attrset",
  "attrget",
  "attrdel",
  "exists"
};


/**
 */
const char *HyperspaceProtocol::CommandText(short command) {
  if (command < 0 || command >= COMMAND_MAX)
    return "UNKNOWN";
  return commandStrings[command];
}


/**
 *
 */
CommBuf *HyperspaceProtocol::CreateMkdirsRequest(const char *fname) {
  MessageBuilderSimple mbuilder;
  CommBuf *cbuf = new CommBuf(mbuilder.HeaderLength() + sizeof(int16_t) + CommBuf::EncodedLength(fname));

  cbuf->PrependString(fname);
  cbuf->PrependShort(COMMAND_MKDIRS);

  mbuilder.Reset(Message::PROTOCOL_PFS);
  mbuilder.Encapsulate(cbuf);

  return cbuf;
}


/**
 *
 */
CommBuf *HyperspaceProtocol::CreateCreateRequest(const char *fname) {
  MessageBuilderSimple mbuilder;
  CommBuf *cbuf = new CommBuf(mbuilder.HeaderLength() + sizeof(int16_t) + CommBuf::EncodedLength(fname));

  cbuf->PrependString(fname);
  cbuf->PrependShort(COMMAND_CREATE);

  mbuilder.Reset(Message::PROTOCOL_PFS);
  mbuilder.Encapsulate(cbuf);

  return cbuf;
}


/**
 */
CommBuf *HyperspaceProtocol::CreateAttrSetRequest(const char *fname, const char *aname, const char *avalue) {
  MessageBuilderSimple mbuilder;
  CommBuf *cbuf = new CommBuf(mbuilder.HeaderLength() + sizeof(int16_t) + CommBuf::EncodedLength(fname)
			      + CommBuf::EncodedLength(aname) + CommBuf::EncodedLength(avalue));

  cbuf->PrependString(avalue);
  cbuf->PrependString(aname);
  cbuf->PrependString(fname);
  cbuf->PrependShort(COMMAND_ATTRSET);

  mbuilder.Reset(Message::PROTOCOL_PFS);
  mbuilder.Encapsulate(cbuf);

  return cbuf;
}


/**
 */
CommBuf *HyperspaceProtocol::CreateAttrGetRequest(const char *fname, const char *aname) {
  MessageBuilderSimple mbuilder;
  CommBuf *cbuf = new CommBuf(mbuilder.HeaderLength() + sizeof(int16_t) + CommBuf::EncodedLength(fname)
			      + CommBuf::EncodedLength(aname));

  cbuf->PrependString(aname);
  cbuf->PrependString(fname);
  cbuf->PrependShort(COMMAND_ATTRGET);

  mbuilder.Reset(Message::PROTOCOL_PFS);
  mbuilder.Encapsulate(cbuf);

  return cbuf;
}


/**
 */
CommBuf *HyperspaceProtocol::CreateAttrDelRequest(const char *fname, const char *aname) {
  MessageBuilderSimple mbuilder;
  CommBuf *cbuf = new CommBuf(mbuilder.HeaderLength() + sizeof(int16_t) + CommBuf::EncodedLength(fname)
			      + CommBuf::EncodedLength(aname));

  cbuf->PrependString(aname);
  cbuf->PrependString(fname);
  cbuf->PrependShort(COMMAND_ATTRDEL);

  mbuilder.Reset(Message::PROTOCOL_PFS);
  mbuilder.Encapsulate(cbuf);

  return cbuf;
}


/**
 *
 */
CommBuf *HyperspaceProtocol::CreateExistsRequest(const char *fname) {
  MessageBuilderSimple mbuilder;
  CommBuf *cbuf = new CommBuf(mbuilder.HeaderLength() + sizeof(int16_t) + CommBuf::EncodedLength(fname));

  cbuf->PrependString(fname);
  cbuf->PrependShort(COMMAND_EXISTS);

  mbuilder.Reset(Message::PROTOCOL_PFS);
  mbuilder.Encapsulate(cbuf);

  return cbuf;
}
