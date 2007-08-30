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
#if 0    
    HeaderBuilder hbuilder;
    CommBuf *cbuf = new CommBuf(hbuilder.HeaderLength() + sizeof(int16_t) + CommBuf::EncodedLength(tableName) + CommBuf::EncodedLength(schemaString));
    cbuf->PrependString(schemaString);
    cbuf->PrependString(tableName);
    cbuf->PrependShort(COMMAND_LOAD_RANGE);
    hbuilder.Reset(Header::PROTOCOL_HYPERTABLE_RANGESERVER);
    hbuilder.Encapsulate(cbuf);
    return cbuf;
#endif
    return 0;
  }

  CommBuf *RangeServerProtocol::CreateRequestUpdate(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec, uint8_t *data, size_t len) {
    return 0;
  }

  CommBuf *RangeServerProtocol::CreateRequestCreateScanner(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec, ScanSpecificationT &spec) {
    return 0;
  }

  CommBuf *RangeServerProtocol::CreateRequestFetchScanblock(struct sockaddr_in &addr, int scannerId) {
    return 0;
  }

  CommBuf *RangeServerProtocol::CreateRequestStatus() {
    return 0;
  }


}

