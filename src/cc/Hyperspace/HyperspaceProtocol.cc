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

#include "Common/Error.h"
#include "AsyncComm/HeaderBuilder.h"
#include "AsyncComm/Serialization.h"

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
  "exists",
  "status"
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
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, fileNameToGroupId(fname));
  hbuilder.AssignUniqueId();
  CommBuf *cbuf = new CommBuf(hbuilder, 2 + Serialization::EncodedLengthString(fname));
  cbuf->AppendShort(COMMAND_MKDIRS);
  cbuf->AppendString(fname);
  return cbuf;
}


/**
 *
 */
CommBuf *HyperspaceProtocol::CreateCreateRequest(const char *fname) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, fileNameToGroupId(fname));
  hbuilder.AssignUniqueId();
  CommBuf *cbuf = new CommBuf(hbuilder, 2 + Serialization::EncodedLengthString(fname));
  cbuf->AppendShort(COMMAND_CREATE);
  cbuf->AppendString(fname);
  return cbuf;
}


/**
 */
CommBuf *HyperspaceProtocol::CreateAttrSetRequest(const char *fname, const char *aname, const char *avalue) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, fileNameToGroupId(fname));
  hbuilder.AssignUniqueId();
  CommBuf *cbuf = new CommBuf(hbuilder, + 2 + 
			      Serialization::EncodedLengthString(fname) + 
			      Serialization::EncodedLengthString(aname) + 
			      Serialization::EncodedLengthString(avalue));
  cbuf->AppendShort(COMMAND_ATTRSET);
  cbuf->AppendString(fname);
  cbuf->AppendString(aname);
  cbuf->AppendString(avalue);
  return cbuf;
}


/**
 */
CommBuf *HyperspaceProtocol::CreateAttrGetRequest(const char *fname, const char *aname) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, fileNameToGroupId(fname));
  hbuilder.AssignUniqueId();
  CommBuf *cbuf = new CommBuf(hbuilder, 2 + 
			      Serialization::EncodedLengthString(fname) +
			      Serialization::EncodedLengthString(aname));
  cbuf->AppendShort(COMMAND_ATTRGET);
  cbuf->AppendString(fname);
  cbuf->AppendString(aname);
  return cbuf;
}


/**
 */
CommBuf *HyperspaceProtocol::CreateAttrDelRequest(const char *fname, const char *aname) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, fileNameToGroupId(fname));
  hbuilder.AssignUniqueId();
  CommBuf *cbuf = new CommBuf(hbuilder, 2 + 
			      Serialization::EncodedLengthString(fname) +
			      Serialization::EncodedLengthString(aname));
  cbuf->AppendShort(COMMAND_ATTRDEL);
  cbuf->AppendString(fname);
  cbuf->AppendString(aname);
  return cbuf;
}


/**
 *
 */
CommBuf *HyperspaceProtocol::CreateExistsRequest(const char *fname) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, fileNameToGroupId(fname));
  hbuilder.AssignUniqueId();
  CommBuf *cbuf = new CommBuf(hbuilder, 2 + Serialization::EncodedLengthString(fname));
  cbuf->AppendShort(COMMAND_EXISTS);
  cbuf->AppendString(fname);
  return cbuf;
}


/**
 *
 */
CommBuf *HyperspaceProtocol::CreateDeleteRequest(const char *fname) {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE, fileNameToGroupId(fname));
  hbuilder.AssignUniqueId();
  CommBuf *cbuf = new CommBuf(hbuilder, 2 + Serialization::EncodedLengthString(fname));
  cbuf->AppendShort(COMMAND_DELETE);
  cbuf->AppendString(fname);
  return cbuf;
}

/**
 *
 */
CommBuf *HyperspaceProtocol::CreateStatusRequest() {
  HeaderBuilder hbuilder(Header::PROTOCOL_HYPERSPACE);
  hbuilder.AssignUniqueId();
  CommBuf *cbuf = new CommBuf(hbuilder, 2);
  cbuf->AppendShort(COMMAND_STATUS);
  return cbuf;
}
