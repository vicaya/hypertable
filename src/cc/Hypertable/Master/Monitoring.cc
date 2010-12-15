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
#include "Common/Compat.h"
#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/Logger.h"
#include "Common/Path.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>

extern "C" {
#include <rrd.h>
#include <time.h>
}

#include <boost/algorithm/string.hpp>

#include "Monitoring.h"

using namespace Hypertable;
using namespace std;

/**
 *  TODO: 
 *  1. set step to monitoring interval
 *  2. Make rrd filename a constant
 */

Monitoring::Monitoring(PropertiesPtr &props) {

  /**
   * Create dir for storing monitoring stats
   */
  m_monitoring_interval = props->get_i32("Hypertable.Monitoring.Interval");
  Path data_dir = props->get_str("Hypertable.DataDirectory");
  m_monitoring_dir = (data_dir /= "/run/monitoring").string();
  if (!FileUtils::exists(m_monitoring_dir)) {
    if (!FileUtils::mkdirs(m_monitoring_dir)) {
      HT_THROW(Error::LOCAL_IO_ERROR, "Unable to create monitoring dir ");
    }
    HT_INFO("Created monitoring stats dir");
  }
  else
    HT_INFO("monitoring stats dir exists");
  m_allowable_skew = props->get_i32("Hypertable.RangeServer.ClockSkew.Max");
}


void Monitoring::add_server(const String &location, StatsSystem &system_info) {
  ScopedLock lock(m_mutex);
  uint32_t id;

  RangeServerMap::iterator iter = m_server_map.find(location);

  if (iter != m_server_map.end()) {
    (*iter).second->system_info = new StatsSystem(system_info);
    return;
  }

  const char *ptr = location.c_str();
  while (*ptr && !isdigit(*ptr))
    ptr++;
  id = atoi(ptr);
  HT_ASSERT(id > 0);

  m_server_map[location] = new RangeServerStatistics();
  m_server_map[location]->location = location;
  m_server_map[location]->id = id;
  m_server_map[location]->system_info = new StatsSystem(system_info);
}


void Monitoring::drop_server(const String &location) {
  ScopedLock lock(m_mutex);

  RangeServerMap::iterator iter = m_server_map.find(location);
  if (iter != m_server_map.end())
    (*iter).second->dropped = true;
}

namespace {
  /** STL Strict Weak Ordering for RangeServerStatistics  */
  struct LtRangeServerStatistics {
    bool operator()(const RangeServerStatistics &s1, const RangeServerStatistics &s2) const {
      return s1.id < s2.id;
    }
  };

}

void Monitoring::add(std::vector<RangeServerStatistics> &stats) {
  ScopedLock lock(m_mutex);
  struct rangeserver_rrd_data rrd_data;
  RangeServerMap::iterator iter;
  double numerator, denominator;

  for (size_t i=0; i<stats.size(); i++) {

    memset(&rrd_data, 0, sizeof(rrd_data));

    iter = m_server_map.find(stats[i].location);
    if (iter == m_server_map.end()) {
      HT_ERRORF("Statistics received for '%s' but not registered for Monitoring",
                stats[i].location.c_str());
      continue;
    }

    (*iter).second->fetch_error = stats[i].fetch_error;
    (*iter).second->fetch_error_msg = stats[i].fetch_error_msg;
    (*iter).second->fetch_timestamp = stats[i].fetch_timestamp;

    if (stats[i].fetch_error != 0)
      continue;

    if ((*iter).second->stats) {

      if (stats[i].stats->query_cache_accesses > (*iter).second->stats->query_cache_accesses) {
        numerator = (double)stats[i].stats->query_cache_hits - 
          (double)(*iter).second->stats->query_cache_hits;
        denominator = (double)stats[i].stats->query_cache_accesses - 
          (double)(*iter).second->stats->query_cache_accesses;
        rrd_data.qcache_hit_pct = (numerator/denominator)*100.0;
      }

      if (stats[i].stats->block_cache_accesses > (*iter).second->stats->block_cache_accesses) {
        numerator = (double)stats[i].stats->block_cache_hits - 
          (double)(*iter).second->stats->block_cache_hits;
        denominator = (double)stats[i].stats->block_cache_accesses - 
          (double)(*iter).second->stats->block_cache_accesses;
        rrd_data.bcache_hit_pct = (numerator/denominator)*100.0;
      }
    }


    rrd_data.timestamp = stats[i].stats_timestamp / 1000000000LL;
    rrd_data.range_count = stats[i].stats->range_count;
    rrd_data.scans = stats[i].stats->scan_count;
    rrd_data.updates = stats[i].stats->update_count;
    rrd_data.cells_read = stats[i].stats->scanned_cells;
    rrd_data.cells_written = stats[i].stats->updated_cells;
    rrd_data.bytes_read = stats[i].stats->scanned_bytes;
    rrd_data.bytes_written = stats[i].stats->updated_bytes;
    rrd_data.sync_count = stats[i].stats->sync_count;
    rrd_data.qcache_max_mem = stats[i].stats->query_cache_max_memory;
    rrd_data.qcache_avail_mem = stats[i].stats->query_cache_available_memory;
    rrd_data.bcache_max_mem = stats[i].stats->block_cache_max_memory;
    rrd_data.bcache_avail_mem = stats[i].stats->block_cache_available_memory;

    numerator = denominator = 0.0;
    for (size_t j=0; j<stats[i].stats->system.fs_stat.size(); j++) {
      numerator += stats[i].stats->system.fs_stat[j].used;
      denominator += stats[i].stats->system.fs_stat[j].total;
    }
    if (denominator != 0.0)
      rrd_data.disk_used_pct = (numerator/denominator)*100.0;

    for (size_t j=0; j<stats[i].stats->system.disk_stat.size(); j++) {
      rrd_data.disk_read_KBps += (int64_t)stats[i].stats->system.disk_stat[j].read_rate;
      rrd_data.disk_write_KBps += (int64_t)stats[i].stats->system.disk_stat[j].write_rate;
      rrd_data.disk_read_iops += (int64_t)stats[i].stats->system.disk_stat[j].reads_rate;
      rrd_data.disk_write_iops += (int64_t)stats[i].stats->system.disk_stat[j].writes_rate;
    }

    rrd_data.vm_size = (int64_t)stats[i].stats->system.proc_stat.vm_size * 1024*1024;
    rrd_data.vm_resident = (int64_t)stats[i].stats->system.proc_stat.vm_resident * 1024*1024;
    rrd_data.net_rx_rate = (int64_t)stats[i].stats->system.net_stat.rx_rate;
    rrd_data.net_tx_rate = (int64_t)stats[i].stats->system.net_stat.tx_rate;
    rrd_data.load_average = stats[i].stats->system.loadavg_stat.loadavg[0];

    compute_clock_skew(stats[i].stats->timestamp, (*iter).second);

    String rrd_file = m_monitoring_dir + "/" + stats[i].location + "_stats_v0.rrd";

    if (!FileUtils::exists(rrd_file))
      create_rangeserver_rrd(rrd_file);

    update_rangeserver_rrd(rrd_file, rrd_data);

    (*iter).second->stats = stats[i].stats;

  }

  // Dump RangeServer summary data
  std::vector<RangeServerStatistics> stats_vec;
  struct LtRangeServerStatistics comp;
  stats_vec.reserve(m_server_map.size());
  for (iter = m_server_map.begin(); iter != m_server_map.end(); ++iter)
    stats_vec.push_back(*(*iter).second);
  sort(stats_vec.begin(), stats_vec.end(), comp);
  dump_rangeserver_summary_json(stats_vec);

}

void Monitoring::compute_clock_skew(int64_t server_timestamp, RangeServerStatistics *stats) {
  int64_t skew;
  int32_t multiplier = 1;

  if (server_timestamp < stats->fetch_timestamp) {
    skew = stats->fetch_timestamp - server_timestamp;
    multiplier = -1;
  }
  else
    skew = server_timestamp - stats->fetch_timestamp;

  // if the reading difference is less than it took to make the request
  // then leave the old skew value in place
  if (skew < stats->fetch_duration)
    return;

  // discount fetch duration and convert to microseconds
  skew -= stats->fetch_duration;
  skew /= 1000;

  if (skew < m_allowable_skew)
    stats->clock_skew = 0;
  else
    stats->clock_skew =  (skew / 1000000L) * multiplier;
}

void Monitoring::create_rangeserver_rrd(const String &filename) {

  /**
   * Create rrd file read rrdcreate man page to understand what this does
   * http://oss.oetiker.ch/rrdtool/doc/rrdcreate.en.html
   */

  HT_INFOF("Creating rrd file %s", filename.c_str());

  std::vector<String> args;
  args.push_back((String)"create");
  args.push_back(filename);
  args.push_back((String)"-s 30"); // interpolate data to 30s intervals
  args.push_back((String)"DS:range_count:GAUGE:600:0:U"); // num_ranges is not a rate, 600s heartbeat
  args.push_back((String)"DS:scans:ABSOLUTE:600:0:U"); // scans is a rate, 600s heartbeat
  args.push_back((String)"DS:updates:ABSOLUTE:600:0:U");
  args.push_back((String)"DS:syncs:ABSOLUTE:600:0:U");
  args.push_back((String)"DS:cells_read:COUNTER:600:0:U");
  args.push_back((String)"DS:cells_written:COUNTER:600:0:U");
  args.push_back((String)"DS:bytes_read:COUNTER:600:0:U");
  args.push_back((String)"DS:bytes_written:COUNTER:600:0:U");
  args.push_back((String)"DS:qcache_hit_pct:GAUGE:600:0:100");
  args.push_back((String)"DS:qcache_max_mem:GAUGE:600:0:U");
  args.push_back((String)"DS:qcache_avail_mem:GAUGE:600:0:U");
  args.push_back((String)"DS:bcache_hit_pct:GAUGE:600:0:100");
  args.push_back((String)"DS:bcache_max_mem:GAUGE:600:0:U");
  args.push_back((String)"DS:bcache_avail_mem:GAUGE:600:0:U");
  args.push_back((String)"DS:disk_used_pct:GAUGE:600:0:100");
  args.push_back((String)"DS:disk_read_KBps:GAUGE:600:0:U");
  args.push_back((String)"DS:disk_write_KBps:GAUGE:600:0:U");
  args.push_back((String)"DS:disk_read_iops:GAUGE:600:0:U");
  args.push_back((String)"DS:disk_write_iops:GAUGE:600:0:U");
  args.push_back((String)"DS:vm_size:GAUGE:600:0:U");
  args.push_back((String)"DS:vm_resident:GAUGE:600:0:U");
  args.push_back((String)"DS:net_rx_rate:GAUGE:600:0:U");
  args.push_back((String)"DS:net_tx_rate:GAUGE:600:0:U");
  args.push_back((String)"DS:loadavg:GAUGE:600:0:U");

  args.push_back((String)"RRA:AVERAGE:.5:1:2880"); // higherst res (30s) has 2880 samples(1 day)
  args.push_back((String)"RRA:AVERAGE:.5:10:2880"); // 5min res for 10 days
  args.push_back((String)"RRA:AVERAGE:.5:60:1448"); // 30min res for 31 days
  args.push_back((String)"RRA:AVERAGE:.5:720:2190");// 6hr res for last 1.5 yrs
  args.push_back((String)"RRA:MAX:.5:10:2880"); // 5min res spikes for last 10 days
  args.push_back((String)"RRA:MAX:.5:720:2190");// 6hr res spikes for last 1.5 yrs
  
  int argc = args.size();
  const char **argv = new const char *[argc+1];
  for (int ii=0; ii< argc; ++ii)
    argv[ii] = args[ii].c_str();
  argv[argc] = NULL;

  rrd_create(argc, (char**)argv);

  if (rrd_test_error()!=0)
    HT_ERROR_OUT << "Error creating RRD " << filename << ": "<< rrd_get_error() << HT_END;

  rrd_clear_error();
  delete [] argv;
}


void Monitoring::update_rangeserver_rrd(const String &filename, struct rangeserver_rrd_data &rrd_data) {
  std::vector<String> args;
  int argc;
  const char **argv;
  String update;

  args.push_back((String)"update");
  args.push_back(filename);

  update = format("%llu:%d:%lld:%lld:%lld:%lld:%lld:%lld:%lld:%.2f:%lld:%lld:%.2f:%lld:%lld:%.2f:%lld:%lld:%lld:%lld:%lld:%lld:%.2f:%.2f:%.2f",
                  (Llu)rrd_data.timestamp,
                  rrd_data.range_count,
                  (Lld)rrd_data.scans,
                  (Lld)rrd_data.updates,
                  (Lld)rrd_data.sync_count,
                  (Lld)rrd_data.cells_read,
                  (Lld)rrd_data.cells_written,
                  (Lld)rrd_data.bytes_read,
                  (Lld)rrd_data.bytes_written,
                  rrd_data.qcache_hit_pct,
                  (Lld)rrd_data.qcache_max_mem,
                  (Lld)rrd_data.qcache_avail_mem,
                  rrd_data.bcache_hit_pct,
                  (Lld)rrd_data.bcache_max_mem,
                  (Lld)rrd_data.bcache_avail_mem,
                  rrd_data.disk_used_pct,
                  (Lld)rrd_data.disk_read_KBps,
                  (Lld)rrd_data.disk_write_KBps,
                  (Lld)rrd_data.disk_read_iops,
                  (Lld)rrd_data.disk_write_iops,
                  (Lld)rrd_data.vm_size,
                  (Lld)rrd_data.vm_resident,
                  rrd_data.net_rx_rate,
                  rrd_data.net_tx_rate,
                  rrd_data.load_average);

  //HT_INFOF("update=\"%s\"", update.c_str());

  args.push_back(update);

  argc = args.size();

  argv = new const char *[argc+1];
  for (int ii=0; ii< argc; ++ii)
    argv[ii] = args[ii].c_str();
  argv[argc] = NULL;

  rrd_update(argc, (char**)argv);
  if (rrd_test_error()!=0) {
    HT_ERROR_OUT << "Error updating RRD " << filename << ": " << rrd_get_error() << HT_END;
  }
  delete [] argv;
  rrd_clear_error();
}

namespace {
  const char *json_header = "{\"RangeServerSummary\": {\n  \"servers\": [\n";
  const char *json_footer= "\n  ]\n}}\n";
  const char *entry_format = 
    "{\"location\": \"%s\", \"hostname\": \"%s\", \"ip\": \"%s\", \"arch\": \"%s\","
    " \"cores\": \"%d\", \"skew\": \"%d\", \"os\": \"%s\", \"osVersion\": \"%s\","
    " \"vendor\": \"%s\", \"vendorVersion\": \"%s\", \"ram\": \"%.2f\","
    " \"disk\": \"%.2f\", \"diskUsePct\": \"%u\", \"lastContact\": \"%s\", \"lastError\": \"%s\"}";

}

void Monitoring::dump_rangeserver_summary_json(std::vector<RangeServerStatistics> &stats) {
  String str = String(json_header);
  String entry;
  double ram;
  double disk;
  unsigned disk_use_pct;
  String error_str;
  String contact_time;

  for (size_t i=0; i<stats.size(); i++) {
    if (stats[i].stats) {
      double numerator=0.0, denominator=0.0;
      ram = stats[i].stats->system.mem_stat.ram / 1000.0;
      disk = 0.0;
      disk_use_pct = 0;
      for (size_t j=0; j<stats[i].stats->system.fs_stat.size(); j++) {
        numerator += stats[i].stats->system.fs_stat[j].used;
        denominator += stats[i].stats->system.fs_stat[j].total;
        disk += stats[i].stats->system.fs_stat[j].total;
      }
      disk /= 1024*1024*1024;
      disk_use_pct = (unsigned)((numerator/denominator)*100.0);
      time_t contact = (time_t)(stats[i].fetch_timestamp / 1000000000LL);
      char buf[64];
      ctime_r(&contact, buf);
      contact_time = buf;
      boost::trim(contact_time);
    }
    else {
      ram = 0.0;
      disk = 0.0;
      disk_use_pct = 0;
      contact_time = String("N/A");
    }

    if (stats[i].fetch_error == 0)
      error_str = "ok";
    else
      error_str = Error::get_text(stats[i].fetch_error);
        
    entry = format(entry_format,
                   stats[i].location.c_str(),
                   stats[i].system_info->net_info.host_name.c_str(),
                   stats[i].system_info->net_info.primary_addr.c_str(),
                   stats[i].system_info->os_info.arch.c_str(),
                   stats[i].system_info->cpu_info.total_cores,
                   stats[i].clock_skew,
                   stats[i].system_info->os_info.name.c_str(),
                   stats[i].system_info->os_info.version.c_str(),
                   stats[i].system_info->os_info.vendor.c_str(),
                   stats[i].system_info->os_info.vendor_version.c_str(),
                   ram,
                   disk,
                   disk_use_pct,
                   contact_time.c_str(),
                   error_str.c_str());

    if (i != 0)
      str += String(",\n    ") + entry;
    else
      str += String("    ") + entry;

    str += json_footer;

    String tmp_filename = m_monitoring_dir + "/rangeserver_summary.tmp";
    String json_filename = m_monitoring_dir + "/rangeserver_summary.json";

    if (FileUtils::write(tmp_filename, str) == -1)
      return;

    FileUtils::rename(tmp_filename, json_filename);
    
  }
}
