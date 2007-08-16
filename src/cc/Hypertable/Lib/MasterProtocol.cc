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

#include "MasterProtocol.h"

namespace hypertable {

  /**
   *
   */
  CommBuf *MasterProtocol::CreateCreateTableRequest(const char *tableName, const char *schemaString) {
    HeaderBuilder hbuilder;
    CommBuf *cbuf = new CommBuf(hbuilder.HeaderLength() + sizeof(int16_t) + CommBuf::EncodedLength(tableName) + CommBuf::EncodedLength(schemaString));
    cbuf->PrependString(schemaString);
    cbuf->PrependString(tableName);
    cbuf->PrependShort(COMMAND_CREATE_TABLE);
    hbuilder.Reset(Header::PROTOCOL_HYPERTABLE_MASTER);
    hbuilder.Encapsulate(cbuf);
    return cbuf;
  }

  CommBuf *MasterProtocol::CreateGetSchemaRequest(const char *tableName) {
    HeaderBuilder hbuilder;
    CommBuf *cbuf = new CommBuf(hbuilder.HeaderLength() + sizeof(int16_t) + CommBuf::EncodedLength(tableName));
    cbuf->PrependString(tableName);
    cbuf->PrependShort(COMMAND_GET_SCHEMA);
    hbuilder.Reset(Header::PROTOCOL_HYPERTABLE_MASTER);
    hbuilder.Encapsulate(cbuf);
    return cbuf;
  }

  CommBuf *MasterProtocol::CreateStatusRequest() {
    HeaderBuilder hbuilder;
    CommBuf *cbuf = new CommBuf(hbuilder.HeaderLength() + sizeof(int16_t));
    cbuf->PrependShort(COMMAND_STATUS);
    hbuilder.Reset(Header::PROTOCOL_HYPERTABLE_MASTER);
    hbuilder.Encapsulate(cbuf);
    return cbuf;
  }

  const char *MasterProtocol::mCommandStrings[] = {
    "create table",
    "get schema",
    "status"
  };

  const char *MasterProtocol::CommandText(short command) {
    if (command < 0 || command >= COMMAND_MAX)
      return "UNKNOWN";
    return mCommandStrings[command];
  }

}

