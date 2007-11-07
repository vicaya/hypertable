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

namespace hypertable {

  const char *RangeServerProtocol::m_command_strings[] = {
    "load range",
    "update",
    "create scanner",
    "fetch scanblock",
    "compact",
    "status"
  };

  const char *RangeServerProtocol::command_text(short command) {
    if (command < 0 || command >= COMMAND_MAX)
      return "UNKNOWN";
    return m_command_strings[command];
  }

  CommBuf *RangeServerProtocol::create_request_load_range(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + EncodedLengthRangeSpecification(rangeSpec));
    cbuf->append_short(COMMAND_LOAD_RANGE);
    EncodeRangeSpecification(cbuf->get_data_ptr_address(), rangeSpec);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_update(struct sockaddr_in &addr, std::string &tableName, uint32_t generation, uint8_t *data, size_t len) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 6 + Serialization::encoded_length_string(tableName) + 
				Serialization::encoded_length_byte_array(len));
    cbuf->append_short(COMMAND_UPDATE);
    cbuf->append_int(generation);
    cbuf->append_string(tableName);
    Serialization::encode_byte_array(cbuf->get_data_ptr_address(), data, len);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_create_scanner(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec, ScanSpecificationT &scanSpec) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + EncodedLengthRangeSpecification(rangeSpec) + EncodedLengthScanSpecification(scanSpec));
    cbuf->append_short(COMMAND_CREATE_SCANNER);
    EncodeRangeSpecification(cbuf->get_data_ptr_address(), rangeSpec);
    EncodeScanSpecification(cbuf->get_data_ptr_address(), scanSpec);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_fetch_scanblock(struct sockaddr_in &addr, int scannerId) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 6);
    cbuf->append_short(COMMAND_FETCH_SCANBLOCK);
    cbuf->append_int(scannerId);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_status() {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    CommBuf *cbuf = new CommBuf(hbuilder, 2);
    cbuf->append_short(COMMAND_STATUS);
    return cbuf;
  }


}

