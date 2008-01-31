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

namespace Hypertable {

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

  CommBuf *MasterProtocol::create_register_server_request(std::string &location) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_MASTER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + Serialization::encoded_length_string(location));
    cbuf->append_short(COMMAND_REGISTER_SERVER);
    cbuf->append_string(location);
    return cbuf;
  }

  CommBuf *MasterProtocol::create_report_split_request(TableIdentifierT &table, RangeT &range, uint64_t soft_limit) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_MASTER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + EncodedLengthTableIdentifier(table) + EncodedLengthRange(range) + 8);
    cbuf->append_short(COMMAND_REPORT_SPLIT);
    EncodeTableIdentifier(cbuf->get_data_ptr_address(), table);
    EncodeRange(cbuf->get_data_ptr_address(), range);
    cbuf->append_long(soft_limit);
    return cbuf;
  }

  CommBuf *MasterProtocol::create_drop_table_request(const char *table_name, bool if_exists) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_MASTER);
    CommBuf *cbuf = new CommBuf(hbuilder, 3 + Serialization::encoded_length_string(table_name));
    cbuf->append_short(COMMAND_DROP_TABLE);
    cbuf->append_bool(if_exists);
    cbuf->append_string(table_name);
    return cbuf;
  }

  const char *MasterProtocol::m_command_strings[] = {
    "create table",
    "get schema",
    "status",
    "register server",
    "report split",
    "drop table"
  };

  const char *MasterProtocol::command_text(short command) {
    if (command < 0 || command >= COMMAND_MAX)
      return "UNKNOWN";
    return m_command_strings[command];
  }
  
}

