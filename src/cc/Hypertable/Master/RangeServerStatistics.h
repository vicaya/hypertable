/** -*- c++ -*-
 * Copyright (C) 2010 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
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

#ifndef HYPERTABLE_RANGESERVERSTATISTICS_H
#define HYPERTABLE_RANGESERVERSTATISTICS_H

#include "AsyncComm/CommAddress.h"

#include "Hypertable/Lib/StatsRangeServer.h"

namespace Hypertable {

  /**
   */
  class RangeServerStatistics {
  public:
    RangeServerStatistics() : dropped(false), clock_skew(0), stats_timestamp(TIMESTAMP_MIN), fetch_timestamp(TIMESTAMP_MIN), fetch_error(0) { }
    String location;
    uint32_t id;
    InetAddr addr;
    bool dropped;
    int32_t clock_skew;
    StatsSystemPtr system_info;
    StatsRangeServerPtr stats;
    int64_t stats_timestamp;
    int64_t fetch_timestamp;
    int64_t fetch_duration;
    int fetch_error;
    String fetch_error_msg;
  };

}

#endif // HYPERTABLE_RANGESERVERSTATISTICS_H
