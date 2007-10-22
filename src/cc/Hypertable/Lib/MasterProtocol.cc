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

#include "AsyncComm/HeaderBuilder.h"
#include "AsyncComm/Serialization.h"

#include "MasterProtocol.h"

namespace hypertable {

  /**
   *
   */
  CommBuf *MasterProtocol::CreateCreateTableRequest(const char *tableName, const char *schemaString) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_MASTER);
    hbuilder.AssignUniqueId();
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + Serialization::EncodedLengthString(tableName) + Serialization::EncodedLengthString(schemaString));
    cbuf->AppendShort(COMMAND_CREATE_TABLE);
    cbuf->AppendString(tableName);
    cbuf->AppendString(schemaString);
    return cbuf;
  }

  CommBuf *MasterProtocol::CreateGetSchemaRequest(const char *tableName) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_MASTER);
    hbuilder.AssignUniqueId();
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + Serialization::EncodedLengthString(tableName));
    cbuf->AppendShort(COMMAND_GET_SCHEMA);
    cbuf->AppendString(tableName);
    return cbuf;
  }

  CommBuf *MasterProtocol::CreateStatusRequest() {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_MASTER);
    hbuilder.AssignUniqueId();
    CommBuf *cbuf = new CommBuf(hbuilder, 2);
    cbuf->AppendShort(COMMAND_STATUS);
    return cbuf;
  }

  CommBuf *MasterProtocol::CreateRegisterServerRequest(std::string &serverIdStr) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_MASTER);
    hbuilder.AssignUniqueId();
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + Serialization::EncodedLengthString(serverIdStr));
    cbuf->AppendShort(COMMAND_REGISTER_SERVER);
    cbuf->AppendString(serverIdStr);
    return cbuf;
  }

  const char *MasterProtocol::mCommandStrings[] = {
    "create table",
    "get schema",
    "status",
    "register server"
  };

  const char *MasterProtocol::CommandText(short command) {
    if (command < 0 || command >= COMMAND_MAX)
      return "UNKNOWN";
    return mCommandStrings[command];
  }

}

