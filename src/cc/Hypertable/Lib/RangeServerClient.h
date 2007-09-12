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

#ifndef HYPERTABLE_RANGESERVERCLIENT_H
#define HYPERTABLE_RANGESERVERCLIENT_H

#include <boost/shared_ptr.hpp>

#include "Common/Properties.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/CommBuf.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/DispatchHandler.h"

#include "RangeServerProtocol.h"
#include "Types.h"

using namespace hypertable;

namespace hypertable {

  class RangeServerClient {
  public:

    RangeServerClient(ConnectionManager *connManager);
    ~RangeServerClient();

    int LoadRange(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec, DispatchHandler *handler);
    int LoadRange(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec);

    int Update(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec, uint8_t *data, size_t len, DispatchHandler *handler);
    int Update(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec, uint8_t *data, size_t len);

    int CreateScanner(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec, ScanSpecificationT &spec, DispatchHandler *handler);
    int CreateScanner(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec, ScanSpecificationT &spec);

    int FetchScanblock(struct sockaddr_in &addr, int scannerId, DispatchHandler *handler);
    int FetchScanblock(struct sockaddr_in &addr, int scannerId);

    int Status(struct sockaddr_in &addr);

  private:

    int SendMessage(struct sockaddr_in &addr, CommBufPtr &cbufPtr, DispatchHandler *handler);
  
    Comm                *mComm;
    ConnectionManager   *mConnectionManager;
  };

  typedef boost::shared_ptr<RangeServerClient> RangeServerClientPtr;
  

}

#endif // HYPERTABLE_RANGESERVERCLIENT_H
