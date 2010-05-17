/** -*- c++ -*-
 * Copyright (C) 2010 Sanjit Jhala (Hypertable, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
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

#ifndef HYPERTABLE_SERVERSTATS_H
#define HYPERTABLE_SERVERSTATS_H

#include "Common/Compat.h"
#include <boost/thread/xtime.hpp>

#include "Common/Time.h"
#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"
#include "Common/String.h"

namespace Hypertable {

  using namespace std;

  class ServerStatsBundle {
  public:
    ServerStatsBundle() : disk_available(0), disk_used(0), disk_read_KBps(0),
        disk_write_KBps(0), disk_read_rate(0), disk_write_rate(0), mem_total(0), mem_used(0),
        vm_size(0), vm_resident(0), net_recv_KBps(0), net_send_KBps(0),
        loadavg_0(0), loadavg_1(0), loadavg_2(0), cpu_pct(0), num_cores(0), clock_mhz(0) {}

    void dump_str(String &str) const;
    ServerStatsBundle& operator += (const ServerStatsBundle&);
    const ServerStatsBundle operator+(const ServerStatsBundle &other) const;
    ServerStatsBundle& operator /= (uint32_t count);
    const ServerStatsBundle operator/(uint32_t count) const;

    // FS stats in KB
    int64_t disk_available;
    int64_t disk_used;
    int32_t disk_read_KBps;
    int32_t disk_write_KBps;
    int32_t disk_read_rate;
    int32_t disk_write_rate;

    // Mem stats in KB
    int32_t mem_total;
    int32_t mem_used;
    int32_t vm_size;
    int32_t vm_resident;

    // Net stats in KB
    int32_t net_recv_KBps;
    int32_t net_send_KBps;

    // Load stats *100
    int16_t loadavg_0;
    int16_t loadavg_1;
    int16_t loadavg_2;

    // Proc stats *100
    int16_t cpu_pct;

    // Cpu info
    uint8_t num_cores;
    int32_t clock_mhz;
  };

  class ServerStats : public ReferenceCount {
  public:

    ServerStats(const int64_t compute_period_millis) : m_compute_millis(compute_period_millis) {
      boost::xtime now;
      boost::xtime_get(&now, TIME_UTC);
      memcpy(&m_collection_time, &now, sizeof(boost::xtime));
      recompute();
    }

    void get(ServerStatsBundle &stats) {
      ScopedLock lock(m_mutex);
      boost::xtime now;
      boost::xtime_get(&now, TIME_UTC);
      int64_t period_millis = xtime_diff_millis(m_collection_time, now);

      if (period_millis > m_compute_millis) {
        memcpy(&m_collection_time, &now, sizeof(boost::xtime));
        recompute();
      }
      stats = m_stats;
    }


  protected :
    void recompute();

    Mutex m_mutex;
    int64_t m_compute_millis;
    boost::xtime m_collection_time;
    ServerStatsBundle m_stats;
  };

  typedef intrusive_ptr<ServerStats> ServerStatsPtr;

}

#endif // HYPERTABLE_SERVERSTATS_H


