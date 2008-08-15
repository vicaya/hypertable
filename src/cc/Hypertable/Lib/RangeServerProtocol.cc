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
#include "AsyncComm/CommBuf.h"
#include "AsyncComm/HeaderBuilder.h"

#include "RangeServerProtocol.h"

namespace Hypertable {

  using namespace Serialization;

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
    "drop range",
    "replay begin",
    "replay load range",
    "replay update",
    "replay commit",
    "get statistics",
    (const char *)0
  };

  const char *RangeServerProtocol::command_text(short command) {
    if (command < 0 || command >= COMMAND_MAX)
      return "UNKNOWN";
    return m_command_strings[command];
  }

  CommBuf *
  RangeServerProtocol::create_request_load_range(const TableIdentifier &table,
      const RangeSpec &range, const char *transfer_log,
      const RangeState &range_state) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + table.encoded_length()
        + range.encoded_length() + encoded_length_str16(transfer_log)
        + range_state.encoded_length());
    cbuf->append_i16(COMMAND_LOAD_RANGE);
    table.encode(cbuf->get_data_ptr_address());
    range.encode(cbuf->get_data_ptr_address());
    encode_str16(cbuf->get_data_ptr_address(), transfer_log);
    range_state.encode(cbuf->get_data_ptr_address());
    return cbuf;
  }

  CommBuf *
  RangeServerProtocol::create_request_update(const TableIdentifier &table,
      uint32_t count, StaticBuffer &buffer) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 6 + table.encoded_length(), buffer);
    cbuf->append_i16(COMMAND_UPDATE);
    table.encode(cbuf->get_data_ptr_address());
    cbuf->append_i32(count);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::
  create_request_create_scanner(const TableIdentifier &table,
      const RangeSpec &range, const ScanSpec &scan_spec) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + table.encoded_length()
        + range.encoded_length() + scan_spec.encoded_length());
    cbuf->append_i16(COMMAND_CREATE_SCANNER);
    table.encode(cbuf->get_data_ptr_address());
    range.encode(cbuf->get_data_ptr_address());
    scan_spec.encode(cbuf->get_data_ptr_address());
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_destroy_scanner(int scanner_id) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER, scanner_id);
    CommBuf *cbuf = new CommBuf(hbuilder, 6);
    cbuf->append_i16(COMMAND_DESTROY_SCANNER);
    cbuf->append_i32(scanner_id);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_fetch_scanblock(int scanner_id) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER, scanner_id);
    CommBuf *cbuf = new CommBuf(hbuilder, 6);
    cbuf->append_i16(COMMAND_FETCH_SCANBLOCK);
    cbuf->append_i32(scanner_id);
    return cbuf;
  }

  CommBuf *
  RangeServerProtocol::create_request_drop_table(const TableIdentifier &table) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + table.encoded_length());
    cbuf->append_i16(COMMAND_DROP_TABLE);
    table.encode(cbuf->get_data_ptr_address());
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_status() {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2);
    cbuf->append_i16(COMMAND_STATUS);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_shutdown() {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2);
    cbuf->append_i16(COMMAND_SHUTDOWN);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_dump_stats() {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2);
    cbuf->append_i16(COMMAND_DUMP_STATS);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_replay_begin(uint16_t group) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 4);
    cbuf->append_i16(COMMAND_REPLAY_BEGIN);
    cbuf->append_i16(group);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::
  create_request_replay_load_range(const TableIdentifier &table,
      const RangeSpec &range, const RangeState &range_state) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + table.encoded_length()
        + range.encoded_length() + range_state.encoded_length());
    cbuf->append_i16(COMMAND_REPLAY_LOAD_RANGE);
    table.encode(cbuf->get_data_ptr_address());
    range.encode(cbuf->get_data_ptr_address());
    range_state.encode(cbuf->get_data_ptr_address());
    return cbuf;
  }

  CommBuf *
  RangeServerProtocol::create_request_replay_update(StaticBuffer &buffer) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2, buffer);
    cbuf->append_i16(COMMAND_REPLAY_UPDATE);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_replay_commit() {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2);
    cbuf->append_i16(COMMAND_REPLAY_COMMIT);
    return cbuf;
  }

  CommBuf *
  RangeServerProtocol::create_request_drop_range(const TableIdentifier &table,
                                                 const RangeSpec &range) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + table.encoded_length()
                                + range.encoded_length());
    cbuf->append_i16(COMMAND_DROP_RANGE);
    table.encode(cbuf->get_data_ptr_address());
    range.encode(cbuf->get_data_ptr_address());
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_get_statistics() {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2);
    cbuf->append_i16(COMMAND_GET_STATISTICS);
    return cbuf;
  }

} // namespace Hypertable
