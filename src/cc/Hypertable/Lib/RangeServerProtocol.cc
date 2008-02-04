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
    "drop table"
  };

  const char *RangeServerProtocol::command_text(short command) {
    if (command < 0 || command >= COMMAND_MAX)
      return "UNKNOWN";
    return m_command_strings[command];
  }

  CommBuf *RangeServerProtocol::create_request_load_range(TableIdentifierT &table, RangeT &range, uint64_t soft_limit, uint16_t flags) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + EncodedLengthTableIdentifier(table) + EncodedLengthRange(range) + 10);
    cbuf->append_short(COMMAND_LOAD_RANGE);
    EncodeTableIdentifier(cbuf->get_data_ptr_address(), table);
    EncodeRange(cbuf->get_data_ptr_address(), range);
    cbuf->append_long(soft_limit);
    cbuf->append_short(flags);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_update(TableIdentifierT &table, uint8_t *data, size_t len) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + EncodedLengthTableIdentifier(table), data, len);
    cbuf->append_short(COMMAND_UPDATE);
    EncodeTableIdentifier(cbuf->get_data_ptr_address(), table);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_create_scanner(TableIdentifierT &table, RangeT &range, ScanSpecificationT &scan_spec) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + EncodedLengthTableIdentifier(table) + EncodedLengthRange(range) + EncodedLengthScanSpecification(scan_spec));
    cbuf->append_short(COMMAND_CREATE_SCANNER);
    EncodeTableIdentifier(cbuf->get_data_ptr_address(), table);
    EncodeRange(cbuf->get_data_ptr_address(), range);
    EncodeScanSpecification(cbuf->get_data_ptr_address(), scan_spec);
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

  CommBuf *RangeServerProtocol::create_request_drop_table(std::string &table_name) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + Serialization::encoded_length_string(table_name));
    cbuf->append_short(COMMAND_DROP_TABLE);
    cbuf->append_string(table_name);
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

}

