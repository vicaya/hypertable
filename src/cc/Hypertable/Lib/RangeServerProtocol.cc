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

#include "AsyncComm/CommBuf.h"
#include "AsyncComm/HeaderBuilder.h"

#include "RangeServerProtocol.h"

namespace Hypertable {

  const char *RangeServerProtocol::m_command_strings[] = {
    "load range",
    "update",
    "create scanner",
    "fetch scanblock",
    "compact",
    "status",
    "shutdown",
    "dump stats",
    "destroy scanner",
    "drop table",
    "replay start",
    "replay update",
    "replay commit",    
    "drop range",
    (const char *)0
  };

  const char *RangeServerProtocol::command_text(short command) {
    if (command < 0 || command >= COMMAND_MAX)
      return "UNKNOWN";
    return m_command_strings[command];
  }

  CommBuf *RangeServerProtocol::create_request_load_range(TableIdentifier &table, RangeSpec &range, const char *transfer_log, RangeState &range_state, uint16_t flags) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + table.encoded_length() + range.encoded_length() + Serialization::encoded_length_string(transfer_log) + range_state.encoded_length() + 2);
    cbuf->append_short(COMMAND_LOAD_RANGE);
    table.encode(cbuf->get_data_ptr_address());
    range.encode(cbuf->get_data_ptr_address());
    Serialization::encode_string(cbuf->get_data_ptr_address(), transfer_log);
    range_state.encode(cbuf->get_data_ptr_address());
    cbuf->append_short(flags);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_update(TableIdentifier &table, uint8_t *data, size_t len) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + table.encoded_length(), data, len);
    cbuf->append_short(COMMAND_UPDATE);
    table.encode(cbuf->get_data_ptr_address());
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_create_scanner(TableIdentifier &table, RangeSpec &range, ScanSpec &scan_spec) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + table.encoded_length() + range.encoded_length() + scan_spec.encoded_length());
    cbuf->append_short(COMMAND_CREATE_SCANNER);
    table.encode(cbuf->get_data_ptr_address());
    range.encode(cbuf->get_data_ptr_address());
    scan_spec.encode(cbuf->get_data_ptr_address());
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_destroy_scanner(int scanner_id) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER, scanner_id);
    CommBuf *cbuf = new CommBuf(hbuilder, 6);
    cbuf->append_short(COMMAND_DESTROY_SCANNER);
    cbuf->append_int(scanner_id);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_fetch_scanblock(int scanner_id) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER, scanner_id);
    CommBuf *cbuf = new CommBuf(hbuilder, 6);
    cbuf->append_short(COMMAND_FETCH_SCANBLOCK);
    cbuf->append_int(scanner_id);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_drop_table(TableIdentifier &table) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + table.encoded_length());
    cbuf->append_short(COMMAND_DROP_TABLE);
    table.encode(cbuf->get_data_ptr_address());
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_status() {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2);
    cbuf->append_short(COMMAND_STATUS);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_shutdown() {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2);
    cbuf->append_short(COMMAND_SHUTDOWN);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_dump_stats() {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2);
    cbuf->append_short(COMMAND_DUMP_STATS);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_replay_start() {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2);
    cbuf->append_short(COMMAND_REPLAY_START);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_replay_update(const uint8_t *data, size_t len) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2, data, len);
    cbuf->append_short(COMMAND_REPLAY_UPDATE);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_replay_commit() {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2);
    cbuf->append_short(COMMAND_REPLAY_COMMIT);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_drop_range(TableIdentifier &table, RangeSpec &range) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + table.encoded_length() + range.encoded_length());
    cbuf->append_short(COMMAND_DROP_RANGE);
    table.encode(cbuf->get_data_ptr_address());
    range.encode(cbuf->get_data_ptr_address());
    return cbuf;
  }

}

