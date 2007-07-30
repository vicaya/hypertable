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

#include "Common/Error.h"
#include "AsyncComm/HeaderBuilder.h"

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
  HeaderBuilder hbuilder;
  CommBuf *cbuf = new CommBuf(hbuilder.HeaderLength() + sizeof(int16_t) + CommBuf::EncodedLength(fname));

  cbuf->PrependString(fname);
  cbuf->PrependShort(COMMAND_MKDIRS);

  hbuilder.Reset(Header::PROTOCOL_PFS);
  hbuilder.Encapsulate(cbuf);

  return cbuf;
}


/**
 *
 */
CommBuf *HyperspaceProtocol::CreateCreateRequest(const char *fname) {
  HeaderBuilder hbuilder;
  CommBuf *cbuf = new CommBuf(hbuilder.HeaderLength() + sizeof(int16_t) + CommBuf::EncodedLength(fname));

  cbuf->PrependString(fname);
  cbuf->PrependShort(COMMAND_CREATE);

  hbuilder.Reset(Header::PROTOCOL_PFS);
  hbuilder.Encapsulate(cbuf);

  return cbuf;
}


/**
 */
CommBuf *HyperspaceProtocol::CreateAttrSetRequest(const char *fname, const char *aname, const char *avalue) {
  HeaderBuilder hbuilder;
  CommBuf *cbuf = new CommBuf(hbuilder.HeaderLength() + sizeof(int16_t) + CommBuf::EncodedLength(fname)
			      + CommBuf::EncodedLength(aname) + CommBuf::EncodedLength(avalue));

  cbuf->PrependString(avalue);
  cbuf->PrependString(aname);
  cbuf->PrependString(fname);
  cbuf->PrependShort(COMMAND_ATTRSET);

  hbuilder.Reset(Header::PROTOCOL_PFS);
  hbuilder.Encapsulate(cbuf);

  return cbuf;
}


/**
 */
CommBuf *HyperspaceProtocol::CreateAttrGetRequest(const char *fname, const char *aname) {
  HeaderBuilder hbuilder;
  CommBuf *cbuf = new CommBuf(hbuilder.HeaderLength() + sizeof(int16_t) + CommBuf::EncodedLength(fname)
			      + CommBuf::EncodedLength(aname));

  cbuf->PrependString(aname);
  cbuf->PrependString(fname);
  cbuf->PrependShort(COMMAND_ATTRGET);

  hbuilder.Reset(Header::PROTOCOL_PFS);
  hbuilder.Encapsulate(cbuf);

  return cbuf;
}


/**
 */
CommBuf *HyperspaceProtocol::CreateAttrDelRequest(const char *fname, const char *aname) {
  HeaderBuilder hbuilder;
  CommBuf *cbuf = new CommBuf(hbuilder.HeaderLength() + sizeof(int16_t) + CommBuf::EncodedLength(fname)
			      + CommBuf::EncodedLength(aname));

  cbuf->PrependString(aname);
  cbuf->PrependString(fname);
  cbuf->PrependShort(COMMAND_ATTRDEL);

  hbuilder.Reset(Header::PROTOCOL_PFS);
  hbuilder.Encapsulate(cbuf);

  return cbuf;
}


/**
 *
 */
CommBuf *HyperspaceProtocol::CreateExistsRequest(const char *fname) {
  HeaderBuilder hbuilder;
  CommBuf *cbuf = new CommBuf(hbuilder.HeaderLength() + sizeof(int16_t) + CommBuf::EncodedLength(fname));

  cbuf->PrependString(fname);
  cbuf->PrependShort(COMMAND_EXISTS);

  hbuilder.Reset(Header::PROTOCOL_PFS);
  hbuilder.Encapsulate(cbuf);

  return cbuf;
}
