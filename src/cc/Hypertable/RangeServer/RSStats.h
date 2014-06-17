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

#include <vector>
#include <algorithm>

#include <boost/thread/xtime.hpp>

#include "Common/Logger.h"
#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"

#include "Range.h"

namespace Hypertable {

  using namespace std;
  class RSStats : public ReferenceCount {
  public:

    enum {
      STATS_COLLECTOR_MAINTENANCE = 0,
      STATS_COLLECTOR_MONITORING  = 1
    };

    RSStats(const vector<int64_t> &compute_period_millis) {
      foreach (int64_t compute_period, compute_period_millis) {
        m_stats_collectors.push_back(StatsCollector(compute_period));
      }
    }

    void lock() { m_mutex.lock(); }
    void unlock() { m_mutex.unlock(); }

    void add_scan_data(uint32_t count, uint32_t cells, uint64_t total_bytes) {
      for(vector<StatsCollector>::iterator it = m_stats_collectors.begin();
          it != m_stats_collectors.end(); ++it) {
        it->add_scan_data(count, cells, total_bytes);
      }
    }

    void add_update_data(uint32_t count, uint32_t cells, uint64_t total_bytes, uint32_t syncs) {
      for(vector<StatsCollector>::iterator it = m_stats_collectors.begin();
          it != m_stats_collectors.end(); ++it) {
        it->add_update_data(count, cells, total_bytes, syncs);
      }
    }

    void recompute(int collector_id) {
      ScopedLock lock(m_mutex);
      m_stats_collectors[collector_id].recompute();
      switch (collector_id) {
        case STATS_COLLECTOR_MAINTENANCE:
          HT_INFOF("Maintenance stats scans=(%u %u %llu %f) updates=(%u %u %llu %f %u)",
	          m_stats_collectors[collector_id].get_scan_count(),
           m_stats_collectors[collector_id].get_scan_cells(),
           (Llu)m_stats_collectors[collector_id].get_scan_bytes(),
           m_stats_collectors[collector_id].get_scan_mbps(),
           m_stats_collectors[collector_id].get_update_count(),
           m_stats_collectors[collector_id].get_update_cells(),
           (Llu)m_stats_collectors[collector_id].get_update_bytes(),
           m_stats_collectors[collector_id].get_update_mbps(),
           m_stats_collectors[collector_id].get_sync_count());
          break;
        case STATS_COLLECTOR_MONITORING:
          break;
        default:
          HT_ERROR_OUT << "Invalid stat bundle id " << collector_id << HT_END;
      }
    }

    int64_t get_measurement_period(int collector_id) {
      return m_stats_collectors[collector_id].get_measurement_period();
    }

    uint32_t get_scan_count(int collector_id) {
      return m_stats_collectors[collector_id].get_scan_count();
    }

    uint32_t get_scan_cells(int collector_id) {
      return m_stats_collectors[collector_id].get_scan_cells();
    }

    uint64_t get_scan_bytes(int collector_id) {
      return m_stats_collectors[collector_id].get_scan_bytes();
    }

    uint32_t get_update_count(int collector_id) {
      return m_stats_collectors[collector_id].get_update_count();
    }

    uint32_t get_update_cells(int collector_id) {
      return m_stats_collectors[collector_id].get_update_cells();
    }

    uint64_t get_update_bytes(int collector_id) {
      return m_stats_collectors[collector_id].get_update_bytes();
    }

    uint32_t get_sync_count(int collector_id) {
      return m_stats_collectors[collector_id].get_sync_count();
    }

    double get_scan_mbps(int collector_id) {
      return m_stats_collectors[collector_id].get_scan_mbps();
    }

    double get_update_mbps(int collector_id) {
      return m_stats_collectors[collector_id].get_update_mbps();
    }

  private:

    struct StatsBundle {
      void clear() {
        scan_count = update_count = sync_count = 0;
        scan_cells = update_cells = 0;
        scan_bytes = update_bytes = 0;
        scan_mbps = 0.0;
        update_mbps = 0.0;
        period_millis = 0;
      }
      uint32_t scan_count;
      uint32_t scan_cells;
      uint64_t scan_bytes;
      uint32_t update_count;
      uint32_t update_cells;
      uint64_t update_bytes;
      double scan_mbps;
      double update_mbps;
      uint32_t sync_count;
      int64_t period_millis;
    };

    class StatsCollector {
    public:
      StatsCollector(int64_t compute_period_millis): compute_period(compute_period_millis) {
        boost::xtime_get(&start_time, TIME_UTC);
        running.clear();
        computed.clear();
      }

      void add_scan_data(uint32_t count, uint32_t cells, uint64_t total_bytes) {
        running.scan_count += count;
        running.scan_cells += cells;
        running.scan_bytes += total_bytes;
      }

      void add_update_data(uint32_t count, uint32_t cells, uint64_t total_bytes,
                           uint32_t syncs) {
        running.update_count += count;
        running.update_cells += cells;
        running.update_bytes += total_bytes;
        running.sync_count += syncs;
      }

      uint32_t get_scan_count() { return computed.scan_count; }
      uint32_t get_scan_cells() { return computed.scan_cells; }
      uint64_t get_scan_bytes() { return computed.scan_bytes; }

      uint32_t get_update_count() { return computed.update_count; }
      uint32_t get_update_cells() { return computed.update_cells; }
      uint64_t get_update_bytes() { return computed.update_bytes; }
      uint32_t get_sync_count() { return computed.sync_count; }

      double get_scan_mbps() { return computed.scan_mbps; }
      double get_update_mbps() { return computed.update_mbps; }

      int64_t get_measurement_period() { return computed.period_millis; }

      void recompute() {
        int64_t period_millis;
        boost::xtime now;
        boost::xtime_get(&now, TIME_UTC);

        period_millis = xtime_diff_millis(start_time, now);
        if (period_millis < compute_period)
	         return;
        memcpy(&computed, &running, sizeof(computed));
        computed.period_millis = period_millis;
        double time_diff = (double)computed.period_millis * 1000.0;
        if (time_diff) {
          computed.scan_mbps =
              (double)computed.scan_bytes / time_diff;
          computed.update_mbps =
              (double)computed.update_bytes / time_diff;
        }
        else {
	         HT_ERROR("mbps calculation over zero time range");
	         computed.scan_mbps = 0.0;
	         computed.update_mbps = 0.0;
        }
        memcpy(&start_time, &now, sizeof(boost::xtime));
        running.clear();
      }

    private:
      StatsBundle running;
      StatsBundle computed;
      boost::xtime start_time;
      int64_t compute_period;
    };

    Mutex m_mutex;
    vector<StatsCollector> m_stats_collectors;
  };

  typedef intrusive_ptr<RSStats> RSStatsPtr;

}

#endif // HYPERTABLE_RANGESERVERSTATS_H


