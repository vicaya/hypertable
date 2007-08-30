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
#ifndef HYPERTABLE_RANGESERVERPROTOCOL_H
#define HYPERTABLE_RANGESERVERPROTOCOL_H

#include "AsyncComm/Protocol.h"

#include "Types.h"

namespace hypertable {

  class RangeServerProtocol : public Protocol {

  public:
    static const short COMMAND_LOAD_RANGE       = 0;
    static const short COMMAND_UPDATE           = 1;
    static const short COMMAND_CREATE_SCANNER   = 2;
    static const short COMMAND_FETCH_SCANBLOCK  = 3;
    static const short COMMAND_COMPACT          = 4;
    static const short COMMAND_STATUS           = 5;
    static const short COMMAND_MAX              = 5;

    static const char *mCommandStrings[];

    CommBuf *CreateRequestLoadRange(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec);
    CommBuf *CreateRequestUpdate(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec, uint8_t *data, size_t len);
    CommBuf *CreateRequestCreateScanner(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec, ScanSpecificationT &spec);
    CommBuf *CreateRequestFetchScanblock(struct sockaddr_in &addr, int scannerId);
    CommBuf *CreateRequestStatus();

    virtual const char *CommandText(short command);
  };

}

#endif // HYPERTABLE_RANGESERVERPROTOCOL_H
