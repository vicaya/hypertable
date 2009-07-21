/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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

#ifndef HYPERTABLE_MAINTENANCEPRIORITIZER_H
#define HYPERTABLE_MAINTENANCEPRIORITIZER_H

#include <boost/thread/xtime.hpp>

#include "Common/Logger.h"
#include "Common/ReferenceCount.h"

#include "Global.h"
#include "RangeStatsGatherer.h"

namespace Hypertable {

  class MaintenancePrioritizer : public ReferenceCount {
  public:

    class Stats {
    public:
      Stats() : m_scanner_generation(0) { start(); }
      void update_stats_bytes_loaded(uint32_t n) {
        ScopedLock lock(m_mutex);
        m_bytes_loaded += n;
      }
      void start() {
        m_bytes_loaded = 0;
        boost::xtime_get(&m_start_time, TIME_UTC);
        m_stop_time = m_start_time;
	m_scanner_generation = Global::scanner_generation;
      }
      void stop() {
        boost::xtime_get(&m_stop_time, TIME_UTC);
      }
      int64_t duration_millis() {
        return xtime_diff_millis(m_start_time, m_stop_time);
      }
      double mbps() {
        double mbps, time_diff = (double)xtime_diff_millis(m_start_time, m_stop_time) * 1000.0;
        if (time_diff)
          mbps = (double)m_bytes_loaded / time_diff;
        else {
          HT_ERROR("mbps calculation over zero time range");
          mbps = 0.0;
        }
        return mbps;
      }
      int64_t starting_scanner_generation() { return m_scanner_generation; }
    private:
      Mutex m_mutex;
      boost::xtime m_start_time;
      boost::xtime m_stop_time;
      uint64_t m_bytes_loaded;
      int64_t m_scanner_generation;
    };

    virtual void prioritize(RangeStatsVector &range_data, int64_t memory_needed,
			    String &trace_str) = 0;

  };
  typedef intrusive_ptr<MaintenancePrioritizer> MaintenancePrioritizerPtr;

}

#endif // HYPERTABLE_MAINTENANCEPRIORITIZER_H


