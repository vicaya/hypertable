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
  CommBuf *MasterProtocol::create_create_table_request(const char *tableName, const char *schemaString) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_MASTER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + Serialization::encoded_length_string(tableName) + Serialization::encoded_length_string(schemaString));
    cbuf->append_short(COMMAND_CREATE_TABLE);
    cbuf->append_string(tableName);
    cbuf->append_string(schemaString);
    return cbuf;
  }

  CommBuf *MasterProtocol::create_get_schema_request(const char *tableName) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_MASTER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + Serialization::encoded_length_string(tableName));
    cbuf->append_short(COMMAND_GET_SCHEMA);
    cbuf->append_string(tableName);
    return cbuf;
  }

  CommBuf *MasterProtocol::create_status_request() {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_MASTER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2);
    cbuf->append_short(COMMAND_STATUS);
    return cbuf;
  }

  CommBuf *MasterProtocol::create_register_server_request(std::string &serverIdStr) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_MASTER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + Serialization::encoded_length_string(serverIdStr));
    cbuf->append_short(COMMAND_REGISTER_SERVER);
    cbuf->append_string(serverIdStr);
    return cbuf;
  }

  const char *MasterProtocol::m_command_strings[] = {
    "create table",
    "get schema",
    "status",
    "register server"
  };

  const char *MasterProtocol::command_text(short command) {
    if (command < 0 || command >= COMMAND_MAX)
      return "UNKNOWN";
    return m_command_strings[command];
  }
  
}

