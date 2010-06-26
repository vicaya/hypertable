/** -*- c++ -*-
 * Copyright (C) 2010 Sanjit Jhala (Hypertable, Inc.)
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


#include "Common/Compat.h"

extern "C" {
#include <rrd.h>
}

#include "Common/Serialization.h"
#include "Common/Error.h"
#include "Common/StringExt.h"
#include "Common/FileUtils.h"
#include "Common/ServerStats.h"

#include "StatsV0.h"

namespace Hypertable {

using namespace Hypertable::Serialization;

void RangeServerStatsV0::RangeStats::process_stats(const uint8_t **bufp, size_t *remainp,
    bool update_table, TableStats *table){

  uint8_t tag;
  uint8_t count = decode_i8(bufp, remainp);
  uint8_t access_groups = decode_i8(bufp, remainp);
  HT_ASSERT(0 == access_groups); // for V0 there shdnt be any access group data

  default_zero.reset();

  while (count > 0) {
    tag = decode_i8(bufp, remainp);
    // decode each stat and if its value is zero by default then update the table stats
    switch(tag) {
      case M0_SCAN_CREATES:
        default_zero.scans = decode_i32(bufp, remainp);
        if (update_table)
          table->scans += default_zero.scans;
        break;
      case M0_SCAN_CELLS_RETURNED:
        default_zero.cells_read = decode_i32(bufp, remainp);
        if (update_table)
          table->cells_read += default_zero.cells_read;
        break;
      case M0_UPDATE_CELLS:
        default_zero.cells_written = decode_i32(bufp, remainp);
        if (update_table)
          table->cells_written += default_zero.cells_written;
        break;
      case M0_BLOOM_FILTER_ACCESSES:
        default_zero.bloom_filter_accesses = decode_i32(bufp, remainp);
        if (update_table)
          table->bloom_filter_accesses += default_zero.bloom_filter_accesses;
        break;
      case M0_BLOOM_FILTER_MAYBES:
        default_zero.bloom_filter_maybes = decode_i32(bufp, remainp);
        if (update_table)
          table->bloom_filter_maybes += default_zero.bloom_filter_maybes;
        break;
      case M0_BLOOM_FILTER_FALSE_POSITIVES:
        default_zero.bloom_filter_false_positives = decode_i32(bufp, remainp);
        if (update_table)
          table->bloom_filter_false_positives += default_zero.bloom_filter_false_positives;
        break;
      case M0_SCAN_BYTES:
        default_zero.bytes_read = decode_i64(bufp, remainp);
        if (update_table)
          table->bytes_read += default_zero.bytes_read;
        break;
      case M0_UPDATE_BYTES:
        default_zero.bytes_written = decode_i64(bufp, remainp);
        if (update_table)
          table->bytes_written += default_zero.bytes_written;
        break;
      case ML_DISK_USED:
        default_last.disk_used = decode_i64(bufp, remainp);
        break;
      case ML_MEMORY_USED:
        default_last.memory_used = decode_i64(bufp, remainp);
        break;
      case ML_MEMORY_ALLOCATED:
        default_last.memory_allocated = decode_i64(bufp, remainp);
        break;
      case ML_SHADOW_CACHE_SIZE:
        default_last.shadow_cache_memory = decode_i64(bufp, remainp);
        break;
      case ML_BLOCK_INDEX_SIZE:
        default_last.block_index_memory = decode_i64(bufp, remainp);
        break;
      case ML_BLOOM_FILTER_SIZE:
        default_last.bloom_filter_memory = decode_i64(bufp, remainp);
        break;
      default:
        HT_ERROR_OUT << "Unknown Range stat: " << tag << HT_END;
        HT_THROWF(Error::PROTOCOL_ERROR, "Unknown Range stat: %d", tag);
        break;
    }
    --count;
  }
  if (update_table)
    update_table_last_stats(table);
}

void RangeServerStatsV0::RangeStats::set_id(const RangeIdentifier &range, uint32_t table,
                                            uint32_t generation)
{
  range_id = range;
  table_id = table;
  schema_generation = generation;
}

void RangeServerStatsV0::RangeStats::get_id(RangeIdentifier &range, uint32_t &table,
                                            uint32_t &generation)
{
  range = range_id;
  table = table_id;
  generation = schema_generation;
}

void RangeServerStatsV0::RangeStats::update_table_last_stats(TableStats *table)
{
  table->disk_used += default_last.disk_used;
  table->memory_used += default_last.memory_used;
  table->memory_allocated += default_last.memory_allocated;
  table->shadow_cache_memory += default_last.shadow_cache_memory;
  table->block_index_memory += default_last.block_index_memory;
  table->bloom_filter_memory += default_last.bloom_filter_memory;
}

void RangeServerStatsV0::RangeStats::dump_str(String &out)
{
  String str;
  range_id.to_str(str);

  out += (String)"RangeStats=";
  out += (String)" {RangeId=";
  out += str;
  out += (String)", TableId=" + table_id;
  out += (String)", SchemaGeneration=" + schema_generation;
  out += (String)", scans=" + default_zero.scans;
  out += (String)", cells_read=" + default_zero.cells_read;
  out += (String)", bytes_read=" + default_zero.bytes_read;
  out += (String)", cells_written=" + default_zero.cells_written;
  out += (String)", bytes_written=" + default_zero.bytes_written;
  out += (String)", bloom_filter_accesses=" + default_zero.bloom_filter_accesses;
  out += (String)", bloom_filter_maybes=" + default_zero.bloom_filter_maybes;
  out += (String)", bloom_filter_false_positives=" + default_zero.bloom_filter_false_positives;
  out += (String)", bloom_filter_memory" + default_last.bloom_filter_memory;
  out += (String)", shadow_cache_memory=" + default_last.shadow_cache_memory;
  out += (String)", block_index_memory=" + default_last.block_index_memory;
  out += (String)", memory_allocated=" + default_last.memory_allocated;
  out += (String)", memory_used=" + default_last.memory_used;
  out += (String)", disk_used=" + default_last.disk_used;
  out += (String)"}";
}



void RangeServerStatsV0::process_stats(const uint8_t **bufp, size_t *remainp,
                                       bool update_table, TableStatsMap  &table_stats_map)
{
  ScopedLock lock(mutex);

  timestamp = decode_i64(bufp, remainp);
  measurement_period = decode_i32(bufp, remainp);
  bool all = decode_bool(bufp, remainp);
  uint32_t count = decode_i8(bufp, remainp);
  num_ranges = decode_i32(bufp, remainp);
  uint8_t tag;
  RangeStatsMap::iterator range_it;
  TableStatsMap::iterator table_it;

  HT_DEBUG_OUT << "deserialize get_statistics() response buffer all=" << all
               << ", stat_count=" << count << ", num_ranges=" << num_ranges << HT_END;

  // reset existence map so we can purge deleted ranges
  RangeStatsExistenceMap::iterator range_exists_it = range_exists_map.begin();
  for (; range_exists_it != range_exists_map.end(); range_exists_it++) {
    range_exists_it->second = false;
  }

  // read RangeServer high level stats
  while (count > 0) {
    tag = decode_i8(bufp, remainp);
    switch(tag) {
      case ML_SYS_DISK_AVAILABLE:
        system_stats.disk_available = decode_i64(bufp, remainp);
        break;
      case ML_SYS_DISK_USED:
        system_stats.disk_used = decode_i64(bufp, remainp);
        break;
      case M0_SYS_DISK_READ_KBPS:
        system_stats.disk_read_KBps= decode_i32(bufp, remainp);
        break;
      case M0_SYS_DISK_WRITE_KBPS:
        system_stats.disk_write_KBps = decode_i32(bufp, remainp);
        break;
      case M0_SYS_DISK_READ_RATE:
        system_stats.disk_read_rate = decode_i32(bufp, remainp);
        break;
      case M0_SYS_DISK_WRITE_RATE:
        system_stats.disk_write_rate = decode_i32(bufp, remainp);
        break;
      case ML_SYS_MEMORY_TOTAL:
        system_stats.mem_total = decode_i32(bufp, remainp);
        break;
      case ML_SYS_MEMORY_USED:
        system_stats.mem_used = decode_i32(bufp, remainp);
        break;
      case ML_VM_SIZE:
        system_stats.vm_size = decode_i32(bufp, remainp);
        break;
      case ML_VM_RESIDENT:
        system_stats.vm_resident = decode_i32(bufp, remainp);
        break;
      case M0_SYS_NET_RECV_KBPS:
        system_stats.net_recv_KBps = decode_i32(bufp, remainp);
        break;
      case M0_SYS_NET_SEND_KBPS:
        system_stats.net_send_KBps = decode_i32(bufp, remainp);
        break;
      case M0_SYS_LOADAVG_0:
        system_stats.loadavg_0 = decode_i16(bufp, remainp);
        break;
      case M0_SYS_LOADAVG_1:
        system_stats.loadavg_1 = decode_i16(bufp, remainp);
        break;
      case M0_SYS_LOADAVG_2:
        system_stats.loadavg_2 = decode_i16(bufp, remainp);
        break;
      case ML_CPU_PCT:
        system_stats.cpu_pct = decode_i16(bufp, remainp);
        break;
      case ML_SYS_NUM_CORES:
        system_stats.num_cores = decode_i8(bufp, remainp);
        break;
      case ML_SYS_CLOCK_MHZ:
        system_stats.clock_mhz = decode_i32(bufp, remainp);
        break;
      case M0_SCAN_CREATES:
        scans = decode_i32(bufp, remainp);
        break;
      case M0_SCAN_CELLS_RETURNED:
        cells_read = decode_i32(bufp, remainp);
        break;
      case M0_UPDATES:
        updates = decode_i32(bufp, remainp);
        break;
      case M0_UPDATE_CELLS:
        cells_written = decode_i32(bufp, remainp);
        break;
      case M0_SCAN_BYTES:
        bytes_read = decode_i64(bufp, remainp);
        break;
      case M0_UPDATE_BYTES:
        bytes_written = decode_i64(bufp, remainp);
        break;
      case M0_SYNCS:
        syncs = decode_i32(bufp, remainp);
        break;
      case ML_QUERY_CACHE_MAX_MEMORY:
        query_cache_max_memory = decode_i64(bufp, remainp);
        break;
      case ML_QUERY_CACHE_AVAILABLE_MEMORY:
        query_cache_available_memory = decode_i64(bufp, remainp);
        break;
      case M0_QUERY_CACHE_ACCESSES:
        query_cache_accesses = decode_i32(bufp, remainp);
        break;
      case M0_QUERY_CACHE_HITS:
        query_cache_hits = decode_i32(bufp, remainp);
        break;
      case ML_BLOCK_CACHE_MAX_MEMORY:
        block_cache_max_memory = decode_i64(bufp, remainp);
        break;
      case ML_BLOCK_CACHE_AVAILABLE_MEMORY:
        block_cache_available_memory = decode_i64(bufp, remainp);
        break;
      case M0_BLOCK_CACHE_ACCESSES:
        block_cache_accesses = decode_i32(bufp, remainp);
        break;
      case M0_BLOCK_CACHE_HITS:
        block_cache_hits = decode_i32(bufp, remainp);
        break;
      default:
        HT_ERROR_OUT << "Unknown RangeServer stat: " << tag << HT_END;
        HT_THROWF(Error::PROTOCOL_ERROR, "Unknown RangeServer stat: %d" ,tag);
        break;
    }
    --count;
  }

  // read per range data
  count = num_ranges;
  uint32_t table_id, schema_generation;
  RangeIdentifier range_id;
  while (count > 0) {
    range_id.decode(bufp, remainp);
    table_id = decode_i32(bufp, remainp);
    schema_generation = decode_i32(bufp, remainp);


    // locate/setup range and table in respective maps
    if (update_table) {
      table_it = table_stats_map.find(table_id);
      if (table_it == table_stats_map.end()) {
        table_stats_map[table_id] = new TableStats;
        table_it = table_stats_map.find(table_id);
      }
    }
    range_it = range_stats_map.find(range_id);
    if (range_it == range_stats_map.end()) {
      range_stats_map[range_id] = new RangeServerStatsV0::RangeStats;
      range_it = range_stats_map.find(range_id);
      range_it->second->set_id(range_id, table_id, schema_generation);
    }
    range_exists_map[range_id] = true;

    // decode stats for this range
    if (update_table)
      range_it->second->process_stats(bufp, remainp, update_table, table_it->second);
    else
      range_it->second->process_stats(bufp, remainp, update_table, 0);

    count--;
  }

  // purge non-existent ranges
  range_exists_it = range_exists_map.begin();
  RangeStatsExistenceMap::iterator delete_it;
  while  (range_exists_it != range_exists_map.end()) {
    if (!range_exists_it->second) {
      delete_it = range_exists_it;
      range_it = range_stats_map.find(delete_it->first);
      HT_ASSERT(range_it != range_stats_map.end());
      ++range_exists_it;
      range_exists_map.erase(delete_it);
      delete range_it->second;
      range_stats_map.erase(range_it);
    }
    else
      ++range_exists_it;
  }

}

void RangeServerStatsV0::dump_rrd(const String &file_prefix)
{
  String filename = file_prefix + "_stats_v0.rrd";
  int64_t epoch_time = ((timestamp/1000000000LL));

  if (!FileUtils::exists(filename)){
    /**Create rrd file read rrdcreate man page to understand what this does
     * http://oss.oetiker.ch/rrdtool/doc/rrdcreate.en.html
     */
    vector<String> args;
    args.push_back((String)"create");
    args.push_back(filename);
    args.push_back((String)"-s 30"); // interpolate data to 30s intervals

    args.push_back((String)"DS:num_ranges:GAUGE:600:0:U"); // num_ranges is not a rate, 600s heartbeat
    args.push_back((String)"DS:scans:ABSOLUTE:600:0:U"); // scans is a rate, 600s heartbeat
    args.push_back((String)"DS:cells_read:ABSOLUTE:600:0:U");
    args.push_back((String)"DS:bytes_read:ABSOLUTE:600:0:U");
    args.push_back((String)"DS:cells_written:ABSOLUTE:600:0:U");
    args.push_back((String)"DS:bytes_written:ABSOLUTE:600:0:U");
    args.push_back((String)"DS:syncs:ABSOLUTE:600:0:U");
    args.push_back((String)"DS:q_c_accesses:ABSOLUTE:600:0:U");
    args.push_back((String)"DS:q_c_hits:ABSOLUTE:600:0:U");
    args.push_back((String)"DS:b_c_accesses:ABSOLUTE:600:0:U");
    args.push_back((String)"DS:b_c_hits:ABSOLUTE:600:0:U");
    args.push_back((String)"DS:q_c_max_mem:GAUGE:600:0:U");
    args.push_back((String)"DS:q_c_avail_mem:GAUGE:600:0:U");
    args.push_back((String)"DS:b_c_max_mem:GAUGE:600:0:U");
    args.push_back((String)"DS:b_c_avail_mem:GAUGE:600:0:U");
    //ServerStatsBundle
    args.push_back((String)"DS:disk_avail:GAUGE:600:0:U");
    args.push_back((String)"DS:disk_used:GAUGE:600:0:U");
    args.push_back((String)"DS:disk_read_KBps:GAUGE:600:0:U");
    args.push_back((String)"DS:disk_write_KBps:GAUGE:600:0:U");
    args.push_back((String)"DS:disk_read_rate:GAUGE:600:0:U");
    args.push_back((String)"DS:disk_write_rate:GAUGE:600:0:U");

    args.push_back((String)"DS:mem_total:GAUGE:600:0:U");
    args.push_back((String)"DS:mem_used:GAUGE:600:0:U");
    args.push_back((String)"DS:vm_size:GAUGE:600:0:U");
    args.push_back((String)"DS:vm_resident:GAUGE:600:0:U");
    args.push_back((String)"DS:net_recv_KBps:GAUGE:600:0:U");
    args.push_back((String)"DS:net_sent_KBps:GAUGE:600:0:U");
    args.push_back((String)"DS:loadavg:GAUGE:600:0:U");
    args.push_back((String)"DS:cpu_pct:GAUGE:600:0:U");
    args.push_back((String)"DS:num_cores:GAUGE:600:0:U");
    args.push_back((String)"DS:clock_mhz:GAUGE:600:0:U");

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

    if (rrd_test_error()!=0) {
      HT_ERROR_OUT << "Error creating RRD " << filename << ": "<< rrd_get_error() << HT_END;
    }

    rrd_clear_error();
    delete [] argv;
  }
  // Update rrd
  vector<String> args;
  int argc;
  const char **argv;
  String update;

  args.push_back((String)"update");
  args.push_back(filename);

  update = format("%llu:%d:%d:%d:%llu:%d:%llu:%d:%d:%d:%d:%d:%llu:%llu:%llu:%llu:%llu:%llu:%d:%d:%d:%d:%d:%d:%d:%d:%d:%d:%f:%f:%d:%d",
                  (Llu)epoch_time, num_ranges, scans,
                  cells_read, (Llu)bytes_read,
                  cells_written, (Llu)bytes_written, syncs,
                  query_cache_accesses, query_cache_hits,
                  block_cache_accesses, block_cache_hits,
                  (Llu)query_cache_max_memory,
                  (Llu)query_cache_available_memory,
                  (Llu)block_cache_max_memory,
                  (Llu)block_cache_available_memory,
                  (Llu)system_stats.disk_available,
                  (Llu)system_stats.disk_used,
                  system_stats.disk_read_KBps,
                  system_stats.disk_write_KBps,
                  system_stats.disk_read_rate,
                  system_stats.disk_write_rate,
                  system_stats.mem_total,
                  system_stats.mem_used,
                  system_stats.vm_size,
                  system_stats.vm_resident,
                  system_stats.net_recv_KBps,
                  system_stats.net_send_KBps,
                  ((float)system_stats.loadavg_0)/100,
                  ((float)system_stats.cpu_pct)/100,
                  system_stats.num_cores,
                  system_stats.clock_mhz);
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

void RangeServerStatsV0::get_hl_stats(RangeServerHLStats &hl_stats) {
  ScopedLock lock(mutex);
  hl_stats.timestamp                      = timestamp;
  hl_stats.measurement_period             = measurement_period;
  hl_stats.num_ranges                     = num_ranges;
  hl_stats.scans                          = scans;
  hl_stats.cells_read                     = cells_read;
  hl_stats.bytes_read                     = bytes_read;
  hl_stats.updates                        = updates;
  hl_stats.syncs                          = syncs;
  hl_stats.cells_written                  = cells_written;
  hl_stats.bytes_written                  = bytes_written;
  hl_stats.query_cache_max_memory         = query_cache_max_memory;
  hl_stats.query_cache_available_memory   = query_cache_available_memory;
  hl_stats.query_cache_accesses           = query_cache_accesses;
  hl_stats.query_cache_hits               = query_cache_hits;
  hl_stats.block_cache_max_memory         = block_cache_max_memory;
  hl_stats.block_cache_available_memory   = block_cache_available_memory;
  hl_stats.block_cache_accesses           = block_cache_accesses;
  hl_stats.block_cache_hits               = block_cache_hits;
  hl_stats.system_stats                   = system_stats;
}

void RangeServerStatsV0::dump_str(String &out)
{
  ScopedLock lock(mutex);
  out += (String)" RangeServerStats = {timestamp=" + timestamp;
  out += (String)", measurement_period=" + measurement_period;
  out += (String)", ";
  system_stats.dump_str(out);
  out += (String)", num_ranges=" + num_ranges;
  out += (String)", scans=" + scans;
  out += (String)", cells_read=" + cells_read;
  out += (String)", bytes_read=" + bytes_read;
  out += (String)", updates=" + updates;
  out += (String)", syncs=" + syncs;
  out += (String)", cells_written=" + cells_written;
  out += (String)", bytes_written=" + bytes_written;
  out += (String)", query_cache_max_memory=" + query_cache_max_memory;
  out += (String)", query_cache_available_memory=" + query_cache_available_memory;
  out += (String)", query_cache_accesses=" + query_cache_accesses;
  out += (String)", query_cache_hits=" + query_cache_hits;
  out += (String)", block_cache_max_memory=" + block_cache_max_memory;
  out += (String)", block_cache_available_memory=" + block_cache_available_memory;
  out += (String)", block_cache_accesses=" + block_cache_accesses;
  out += (String)", block_cache_hits=" + block_cache_hits;

  RangeStatsMap::iterator it = range_stats_map.begin();
  for(;it != range_stats_map.end(); ++it) {
    String range_str;
    it->second->dump_str(range_str);
    out += range_str + "\n";
  }
  out += "}";
}

std::ostream & operator<<(ostream &os, RangeServerStatsV0 &stats)
{
  String out;
  stats.dump_str(out);
  os << out;
  return os;
}

} // namespace Hypertable
