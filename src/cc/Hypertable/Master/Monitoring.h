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

#ifndef HYPERTABLE_MONITORING_H
#define HYPERTABLE_MONITORING_H

#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"
#include "Common/StatsSystem.h"

#include "Hypertable/Lib/KeySpec.h"
#include "Hypertable/Lib/StatsRangeServer.h"

#include "RangeServerStatistics.h"


namespace Hypertable {

  /**
   */
  class Monitoring : public ReferenceCount {

  public:

    /**
     * Constructor.
     */
    Monitoring(PropertiesPtr &props);

    void add_server(const String &location, StatsSystem &system_info);

    void drop_server(const String &location);

    void add(std::vector<RangeServerStatistics> &stats);

  private:

    struct rangeserver_rrd_data {
      uint64_t timestamp;
      int32_t range_count;
      int64_t scans;
      int64_t updates;
      int64_t sync_count;
      double cell_read_rate;
      double cell_write_rate;
      double byte_read_rate;
      double byte_write_rate;
      double qcache_hit_pct;
      int64_t qcache_max_mem;
      int64_t qcache_fill;
      double bcache_hit_pct;
      int64_t bcache_max_mem;
      int64_t bcache_fill;
      double disk_used_pct;
      int64_t disk_read_KBps;
      int64_t disk_write_KBps;
      int64_t disk_read_iops;
      int64_t disk_write_iops;
      int64_t vm_size;
      int64_t vm_resident;
      double net_rx_rate;
      double net_tx_rate;
      double load_average;
    };

    void compute_clock_skew(int64_t server_timestamp, RangeServerStatistics *stats);
    void create_rangeserver_rrd(const String &filename);
    void update_rangeserver_rrd(const String &filename, struct rangeserver_rrd_data &rrd_data);
    void dump_rangeserver_summary_json(std::vector<RangeServerStatistics> &stats);

    typedef hash_map<String, RangeServerStatistics *> RangeServerMap;

    Mutex m_mutex;
    RangeServerMap m_server_map;
    String m_monitoring_dir;
    int32_t m_monitoring_interval;
    int32_t m_allowable_skew;
  };
  typedef intrusive_ptr<Monitoring> MonitoringPtr;
  
}


#endif // HYPERTABLE_MONITORING_H
