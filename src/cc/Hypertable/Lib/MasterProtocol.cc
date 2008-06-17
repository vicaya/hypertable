/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
#include "AsyncComm/HeaderBuilder.h"
#include "Common/Serialization.h"

#include "MasterProtocol.h"

namespace Hypertable {
  using namespace Serialization;

  CommBuf *MasterProtocol::create_create_table_request(const char *tablename, const char *schemastr) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_MASTER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + encoded_length_vstr(tablename) + encoded_length_vstr(schemastr));
    cbuf->append_i16(COMMAND_CREATE_TABLE);
    cbuf->append_vstr(tablename);
    cbuf->append_vstr(schemastr);
    return cbuf;
  }

  CommBuf *MasterProtocol::create_get_schema_request(const char *tablename) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_MASTER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + encoded_length_vstr(tablename));
    cbuf->append_i16(COMMAND_GET_SCHEMA);
    cbuf->append_vstr(tablename);
    return cbuf;
  }

  CommBuf *MasterProtocol::create_status_request() {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_MASTER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2);
    cbuf->append_i16(COMMAND_STATUS);
    return cbuf;
  }

  CommBuf *MasterProtocol::create_register_server_request(const String &location) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_MASTER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + encoded_length_vstr(location));
    cbuf->append_i16(COMMAND_REGISTER_SERVER);
    cbuf->append_vstr(location);
    return cbuf;
  }

  CommBuf *MasterProtocol::create_report_split_request(TableIdentifier *table, RangeSpec &range, const char *transfer_log_dir, uint64_t soft_limit) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_MASTER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + table->encoded_length() + range.encoded_length() + encoded_length_vstr(transfer_log_dir) + 8);
    cbuf->append_i16(COMMAND_REPORT_SPLIT);
    table->encode(cbuf->get_data_ptr_address());
    range.encode(cbuf->get_data_ptr_address());
    cbuf->append_vstr(transfer_log_dir);
    cbuf->append_i64(soft_limit);
    return cbuf;
  }

  CommBuf *MasterProtocol::create_drop_table_request(const char *table_name, bool if_exists) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_MASTER);
    CommBuf *cbuf = new CommBuf(hbuilder, 3 + encoded_length_vstr(table_name));
    cbuf->append_i16(COMMAND_DROP_TABLE);
    cbuf->append_bool(if_exists);
    cbuf->append_vstr(table_name);
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

