/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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
#include "Common/Serialization.h"

#include "AsyncComm/CommHeader.h"

#include "MasterProtocol.h"

namespace Hypertable {
  using namespace Serialization;

  CommBuf *
  MasterProtocol::create_create_table_request(const char *tablename,
                                              const char *schemastr) {
    CommHeader header(COMMAND_CREATE_TABLE);
    CommBuf *cbuf = new CommBuf(header, encoded_length_vstr(tablename)
        + encoded_length_vstr(schemastr));
    cbuf->append_vstr(tablename);
    cbuf->append_vstr(schemastr);
    return cbuf;
  }

  CommBuf *MasterProtocol::create_get_schema_request(const char *tablename) {
    CommHeader header(COMMAND_GET_SCHEMA);
    CommBuf *cbuf = new CommBuf(header, encoded_length_vstr(tablename));
    cbuf->append_vstr(tablename);
    return cbuf;
  }

  CommBuf *MasterProtocol::create_status_request() {
    CommHeader header(COMMAND_STATUS);
    CommBuf *cbuf = new CommBuf(header, 0);
    return cbuf;
  }

  CommBuf *
  MasterProtocol::create_register_server_request(const String &location) {
    CommHeader header(COMMAND_REGISTER_SERVER);
    CommBuf *cbuf = new CommBuf(header, encoded_length_vstr(location));
    cbuf->append_vstr(location);
    return cbuf;
  }

  CommBuf *
  MasterProtocol::create_report_split_request(const TableIdentifier *table,
      const RangeSpec &range, const char *transfer_log_dir,
      uint64_t soft_limit) {
    CommHeader header(COMMAND_REPORT_SPLIT);
    CommBuf *cbuf = new CommBuf(header, table->encoded_length()
        + range.encoded_length() + encoded_length_vstr(transfer_log_dir) + 8);
    table->encode(cbuf->get_data_ptr_address());
    range.encode(cbuf->get_data_ptr_address());
    cbuf->append_vstr(transfer_log_dir);
    cbuf->append_i64(soft_limit);
    return cbuf;
  }

  CommBuf *
  MasterProtocol::create_drop_table_request(const char *table_name,
                                            bool if_exists) {
    CommHeader header(COMMAND_DROP_TABLE);
    CommBuf *cbuf = new CommBuf(header, 1 + encoded_length_vstr(table_name));
    cbuf->append_bool(if_exists);
    cbuf->append_vstr(table_name);
    return cbuf;
  }

  CommBuf *MasterProtocol::create_shutdown_request() {
    CommHeader header(COMMAND_SHUTDOWN);
    CommBuf *cbuf = new CommBuf(header, 0);
    return cbuf;
  }

  const char *MasterProtocol::m_command_strings[] = {
    "create table",
    "get schema",
    "status",
    "register server",
    "report split",
    "drop table",
    "shutdown"
  };

  const char *MasterProtocol::command_text(uint64_t command) {
    if (command < 0 || command >= COMMAND_MAX)
      return "UNKNOWN";
    return m_command_strings[command];
  }

}

