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
#include "AsyncComm/DispatchHandler.h"

#include "RangeServerProtocol.h"
#include "Types.h"

using namespace hypertable;

namespace hypertable {

  class ScanResult;

  class RangeServerClient {
  public:

    RangeServerClient(Comm *comm, time_t timeout);
    ~RangeServerClient();

    int load_range(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec, DispatchHandler *handler);
    int load_range(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec);

    int update(struct sockaddr_in &addr, std::string tableName, uint32_t generation, uint8_t *data, size_t len, DispatchHandler *handler);
    int update(struct sockaddr_in &addr, std::string tableName, uint32_t generation, uint8_t *data, size_t len);

    int create_scanner(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec, ScanSpecificationT &spec, DispatchHandler *handler);
    int create_scanner(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec, ScanSpecificationT &spec, ScanResult &result);

    int fetch_scanblock(struct sockaddr_in &addr, int scannerId, DispatchHandler *handler);
    int fetch_scanblock(struct sockaddr_in &addr, int scannerId, ScanResult &result);

    int status(struct sockaddr_in &addr);

  private:

    int send_message(struct sockaddr_in &addr, CommBufPtr &cbufPtr, DispatchHandler *handler);
  
    Comm                *m_comm;
    time_t               m_timeout;
  };

  typedef boost::shared_ptr<RangeServerClient> RangeServerClientPtr;
  

}

#endif // HYPERTABLE_RANGESERVERCLIENT_H
