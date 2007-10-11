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

  const char *RangeServerProtocol::mCommandStrings[] = {
    "load range",
    "update",
    "create scanner",
    "fetch scanblock",
    "compact",
    "status"
  };

  const char *RangeServerProtocol::CommandText(short command) {
    if (command < 0 || command >= COMMAND_MAX)
      return "UNKNOWN";
    return mCommandStrings[command];
  }

  CommBuf *RangeServerProtocol::CreateRequestLoadRange(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    hbuilder.AssignUniqueId();
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + EncodedLengthRangeSpecification(rangeSpec));
    cbuf->AppendShort(COMMAND_LOAD_RANGE);
    EncodeRangeSpecification(cbuf->GetDataPtrAddress(), rangeSpec);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::CreateRequestUpdate(struct sockaddr_in &addr, std::string &tableName, uint32_t generation, uint8_t *data, size_t len) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    hbuilder.AssignUniqueId();
    CommBuf *cbuf = new CommBuf(hbuilder, 6 + Serialization::EncodedLengthString(tableName) + 
				Serialization::EncodedLengthByteArray(len));
    cbuf->AppendShort(COMMAND_UPDATE);
    cbuf->AppendInt(generation);
    cbuf->AppendString(tableName);
    Serialization::EncodeByteArray(cbuf->GetDataPtrAddress(), data, len);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::CreateRequestCreateScanner(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec, ScanSpecificationT &scanSpec) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    hbuilder.AssignUniqueId();
    CommBuf *cbuf = new CommBuf(hbuilder, 2 + EncodedLengthRangeSpecification(rangeSpec) + EncodedLengthScanSpecification(scanSpec));
    cbuf->AppendShort(COMMAND_CREATE_SCANNER);
    EncodeRangeSpecification(cbuf->GetDataPtrAddress(), rangeSpec);
    EncodeScanSpecification(cbuf->GetDataPtrAddress(), scanSpec);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::CreateRequestFetchScanblock(struct sockaddr_in &addr, int scannerId) {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    hbuilder.AssignUniqueId();
    CommBuf *cbuf = new CommBuf(hbuilder, 6);
    cbuf->AppendShort(COMMAND_FETCH_SCANBLOCK);
    cbuf->AppendInt(scannerId);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::CreateRequestStatus() {
    HeaderBuilder hbuilder(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    hbuilder.AssignUniqueId();
    CommBuf *cbuf = new CommBuf(hbuilder, 2);
    cbuf->AppendShort(COMMAND_STATUS);
    return cbuf;
  }


}

