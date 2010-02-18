/** -*- c++ -*-
 * Copyright (C) 2010 Doug Judd (Hypertable, Inc.)
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

#ifndef HYPERTABLE_RANGESERVERSTATS_H
#define HYPERTABLE_RANGESERVERSTATS_H

#include <boost/thread/xtime.hpp>

#include "Common/Logger.h"
#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"

namespace Hypertable {


  class RangeServerStats : public ReferenceCount {
  public:
    RangeServerStats(int64_t period_millis) : m_compute_period(period_millis) {
      boost::xtime_get(&m_start_time, TIME_UTC);
      m_running.clear();
      m_computed.clear();
    }

    void add_scan_data(uint32_t count, uint64_t total_bytes) {
      ScopedLock lock(m_mutex);
      m_running.scan_count += count;
      m_running.scan_bytes += total_bytes;
    }

    void add_update_data(uint32_t count, uint64_t total_bytes) {
      ScopedLock lock(m_mutex);
      m_running.update_count += count;
      m_running.update_bytes += total_bytes;
    }

    void recompute() {
      ScopedLock lock(m_mutex);
      int64_t period_millis;
      boost::xtime now;
      boost::xtime_get(&now, TIME_UTC);
      period_millis = xtime_diff_millis(m_start_time, now);
      if (period_millis < m_compute_period)
	return;
      memcpy(&m_computed, &m_running, sizeof(m_computed));
      m_computed.period_millis = period_millis;
      double time_diff = (double)m_computed.period_millis * 1000.0;
      if (time_diff) {
	m_computed.scan_mbps = (double)m_computed.scan_bytes / time_diff;
	m_computed.update_mbps = (double)m_computed.update_bytes / time_diff;
      }
      else {
	HT_ERROR("mbps calculation over zero time range");
	m_computed.scan_mbps = 0.0;
	m_computed.update_mbps = 0.0;
      }
      memcpy(&m_start_time, &now, sizeof(boost::xtime));
      m_running.clear();
      HT_INFOF("scans=(%u %llu %f) updates=(%u %llu %f)",
	       m_computed.scan_count, (Llu)m_computed.scan_bytes, m_computed.scan_mbps,
	       m_computed.update_count, (Llu)m_computed.update_bytes, m_computed.update_mbps);
    }

    uint32_t get_scan_count() { return m_computed.scan_count; }
    uint32_t get_scan_bytes() { return m_computed.scan_bytes; }

    uint32_t get_update_count() { return m_computed.update_count; }
    uint32_t get_update_bytes() { return m_computed.update_bytes; }

    double get_scan_mbps() { return m_computed.scan_mbps; }
    double get_update_mbps() { return m_computed.update_mbps; }

  private:

    struct StatsBundle {
      void clear() { 
	scan_count = update_count = 0;
	scan_bytes = update_bytes = 0;
	scan_mbps = 0.0;
	update_mbps = 0.0;
	period_millis = 0;
      }
      uint32_t scan_count;
      uint64_t scan_bytes;
      uint32_t update_count;
      uint64_t update_bytes;
      double scan_mbps;
      double update_mbps;
      int64_t period_millis;
    };

    Mutex m_mutex;
    boost::xtime m_start_time;
    int64_t m_compute_period;
    StatsBundle m_running;
    StatsBundle m_computed;
  };

  typedef intrusive_ptr<RangeServerStats> RangeServerStatsPtr;

}

#endif // HYPERTABLE_RANGESERVERSTATS_H


