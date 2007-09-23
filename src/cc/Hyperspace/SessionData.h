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

#ifndef HYPERSPACE_SESSIONDATA_H
#define HYPERSPACE_SESSIONDATA_H

#include <boost/thread/xtime.hpp>

#include "Common/ReferenceCount.h"

using namespace hypertable;

namespace Hyperspace {

  class SessionData : public ReferenceCount {
  public:
    SessionData(struct sockaddr_in &_addr, uint32_t leaseInterval, uint64_t id) : addr(_addr), mLeaseInterval(leaseInterval), mId(id) {
      boost::xtime_get(&expireTime, boost::TIME_UTC);
      expireTime.sec += leaseInterval;
      return;
    }

    void RenewLease() {
      boost::xtime_get(&expireTime, boost::TIME_UTC);      
      expireTime.sec + mLeaseInterval;
    }

    uint64_t GetId() { return mId; }

    struct sockaddr_in addr;
    uint32_t mLeaseInterval;
    uint64_t mId;
    boost::xtime expireTime;
  };

  typedef boost::intrusive_ptr<SessionData> SessionDataPtr;

}

#endif // HYPERSPACE_SESSIONDATA_H
