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
#include <algorithm>
#include <iostream>
#include <fstream>
#include "Common/StringExt.h"
#include "Stats.h"


namespace Hypertable {

void RangeServerHLStats::dump_str(String &out) {
  out += (String)"timestamp=" + timestamp;
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
}
void TableStats::dump_str(String &out)
{
  out += (String)"TableStats={";
  out += (String)"scans=" + scans;
  out += (String)", cells_read=" + cells_read;
  out += (String)", bytes_read=" + bytes_read;
  out += (String)", cells_written=" + cells_written;
  out += (String)", bytes_written=" +  bytes_written;
  out += (String)", bloom_filter_accesses=" + bloom_filter_accesses;
  out += (String)", bloom_filter_maybes=" +  bloom_filter_maybes;
  out += (String)", bloom_filter_false_positives=" +  bloom_filter_false_positives;
  out += (String)", bloom_filter_memory=" + bloom_filter_memory;
  out += (String)", block_index_memory=" + block_index_memory;
  out += (String)", shadow_cache_memory=" + shadow_cache_memory;
  out += (String)", memory_used=" +  memory_used;
  out += (String)", memory_allocated=" +  memory_allocated;
  out += (String)", disk_used=" +  disk_used;
  out += (String)"}";
}

ostream& operator<<(ostream &os, TableStats &stats)
{
  String out;
  stats.dump_str(out);
  os << out;
  return os;
}

namespace {
  void accumulate_table_stats(TableStatsSnapshotBuffer::iterator &begin,
                              TableStatsSnapshotBuffer::iterator &end, uint32_t table,
                              TableStats &result) {
    TableStatsSnapshotBuffer::iterator tssb_it = begin;
    TableStatsMap::iterator tsm_it;
    TableStats *current;
    int count=0;

    result.reset();
    while (tssb_it != end) {
      tsm_it = (*tssb_it)->map.find(table);
      // can't fine data for table in this snapshot
      if (tsm_it == (*tssb_it)->map.end()) {
        tssb_it++;
        continue;
      }
      current = tsm_it->second;
      result.scans                        += current->scans;
      result.cells_read                   += current->cells_read;
      result.bytes_read                   += current->bytes_read;
      result.cells_written                += current->cells_written;
      result.bytes_written                += current->bytes_written;
      result.bloom_filter_accesses        += current->bloom_filter_accesses;
      result.bloom_filter_maybes          += current->bloom_filter_maybes;
      result.bloom_filter_false_positives += current->bloom_filter_false_positives;
      result.bloom_filter_memory          += current->bloom_filter_memory;
      result.block_index_memory           += current->block_index_memory;
      result.shadow_cache_memory          += current->shadow_cache_memory;
      result.memory_used                  += current->memory_used;
      result.memory_allocated             += current->memory_allocated;
      result.disk_used                    += current->disk_used;

      ++tssb_it;
      ++count;
    }

    if (count>0) {
      result.bloom_filter_memory = (uint64_t)(result.bloom_filter_memory/count);
      result.block_index_memory = (uint64_t)(result.block_index_memory/count);
      result.shadow_cache_memory = (uint64_t)(result.shadow_cache_memory/count);
      result.memory_used = (uint64_t)(result.memory_used/count);
      result.memory_allocated = (uint64_t)(result.memory_allocated/count);
      result.disk_used = (uint64_t)(result.disk_used/count);
    }
  }

  void dump_table_stat_samples(uint32_t samples, vector<TableStats> &stats,
                               vector<int64_t> timestamps, ostream &os) {
    uint32_t ii;
    os << "\tTimestamps=" << timestamps[0];
    for(ii=1; ii<samples; ++ii) os << "," << timestamps[ii]; os << "\n";

    os << "\tscans=" << stats[0].scans;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].scans; os << "\n";
    os << "\tcells_read=" << stats[0].cells_read;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].cells_read; os << "\n";
    os << "\tbytes_read=" << stats[0].bytes_read;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].bytes_read; os << "\n";
    os << "\tcells_written=" << stats[0].cells_written;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].cells_written; os << "\n";
    os << "\tbytes_written=" << stats[0].bytes_written;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].bytes_written; os << "\n";
    os << "\tbloom_filter_accesses=" << stats[0].bloom_filter_accesses;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].bloom_filter_accesses; os << "\n";
    os << "\tbloom_filter_maybes=" << stats[0].bloom_filter_maybes;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].bloom_filter_maybes; os << "\n";
    os << "\tbloom_filter_false_positives=" << stats[0].bloom_filter_false_positives;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].bloom_filter_false_positives; os << "\n";
    os << "\tbloom_filter_memory=" << stats[0].bloom_filter_memory;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].bloom_filter_memory; os << "\n";
    os << "\tblock_index_memory=" << stats[0].block_index_memory;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].block_index_memory; os << "\n";
    os << "\tshadow_cache_memory=" << stats[0].shadow_cache_memory;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].shadow_cache_memory; os << "\n";
    os << "\tmemory_used=" << stats[0].memory_used;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].memory_used; os << "\n";
    os << "\tmemory_allocated=" << stats[0].memory_allocated;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].memory_allocated; os << "\n";
    os << "\tdisk_used=" << stats[0].disk_used;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].disk_used; os << "\n";
  }

  void accumulate_range_server_stats(RangeServerHLStatsSnapshotBuffer::iterator &begin,
                                     RangeServerHLStatsSnapshotBuffer::iterator &end,
                                     const String &server_id,
                                     RangeServerHLStats &result) {
    RangeServerHLStatsSnapshotBuffer::iterator rssb_it = begin;
    RangeServerHLStatsMap::iterator rsm_it;
    RangeServerHLStats *current;
    int count=0;


    while (rssb_it != end) {
      rsm_it = (*rssb_it)->map.find(server_id);
      // can't fine data for table in this snapshot
      if (rsm_it == (*rssb_it)->map.end()) {
        rssb_it++;
        continue;
      }
      current = rsm_it->second;
      result.system_stats                 += current->system_stats;
      result.scans                        += current->scans;
      result.num_ranges                   += current->num_ranges;
      result.scans                        += current->scans;
      result.cells_read                   += current->cells_read;
      result.bytes_read                   += current->bytes_read;
      result.updates                      += current->updates;
      result.cells_written                += current->cells_written;
      result.bytes_written                += current->bytes_written;
      result.syncs                        += current->syncs;
      result.query_cache_max_memory       += current->query_cache_max_memory;
      result.query_cache_available_memory += current->query_cache_available_memory;
      result.query_cache_accesses         += current->query_cache_accesses;
      result.query_cache_hits             += current->query_cache_hits;
      result.block_cache_max_memory       += current->block_cache_max_memory;
      result.block_cache_available_memory += current->block_cache_available_memory;
      result.block_cache_accesses         += current->block_cache_accesses;
      result.block_cache_hits             += current->block_cache_hits;
      ++rssb_it;
      ++count;
    }

    if (count>0) {
      result.system_stats /= count;
      result.num_ranges  = (uint32_t)(result.num_ranges/count);
      result.query_cache_max_memory = (uint64_t)(result.query_cache_max_memory/count);
      result.query_cache_available_memory =
          (uint64_t)(result.query_cache_available_memory/count);
      result.block_cache_max_memory = (uint64_t)(result.block_cache_max_memory/count);
      result.block_cache_available_memory =
          (uint64_t)(result.block_cache_available_memory/count);
    }
  }

  void dump_range_server_stat_samples(uint32_t samples, vector<RangeServerHLStats> &stats,
                                     vector<int64_t> timestamps, ostream &os) {
    uint32_t ii;
    os << "\tTimestamps=" << timestamps[0];
    for(ii=1; ii<samples; ++ii) os << "," << timestamps[ii]; os << "\n";
    // Generic server stats
    os <<"\tdisk_available=" << stats[0].system_stats.disk_available;
    for (ii=1; ii<samples; ++ii) os << "," << stats[ii].system_stats.disk_available; os << "\n";
    os <<"\tdisk_used=" << stats[0].system_stats.disk_used;
    for (ii=1; ii<samples; ++ii) os << "," << stats[ii].system_stats.disk_used; os << "\n";
    os <<"\tdisk_read_KBps=" << stats[0].system_stats.disk_read_KBps;
    for (ii=1; ii<samples; ++ii) os << "," << stats[ii].system_stats.disk_read_KBps; os << "\n";
    os <<"\tdisk_write_KBps=" << stats[0].system_stats.disk_write_KBps;
    for (ii=1; ii<samples; ++ii) os << "," << stats[ii].system_stats.disk_write_KBps; os << "\n";
    os <<"\tdisk_read_rate=" << stats[0].system_stats.disk_read_rate;
    for (ii=1; ii<samples; ++ii) os << "," << stats[ii].system_stats.disk_read_rate; os << "\n";
    os <<"\tdisk_write_rate=" << stats[0].system_stats.disk_write_rate;
    for (ii=1; ii<samples; ++ii) os << "," << stats[ii].system_stats.disk_write_rate; os << "\n";
    os <<"\tmem_total=" << stats[0].system_stats.mem_total;
    for (ii=1; ii<samples; ++ii) os << "," << stats[ii].system_stats.mem_total; os << "\n";
    os <<"\tmem_used=" << stats[0].system_stats.mem_used;
    for (ii=1; ii<samples; ++ii) os << "," << stats[ii].system_stats.mem_used; os << "\n";
    os <<"\tvm_size=" << stats[0].system_stats.vm_size;
    for (ii=1; ii<samples; ++ii) os << "," << stats[ii].system_stats.vm_size; os << "\n";
    os <<"\tvm_resident=" << stats[0].system_stats.vm_resident;
    for (ii=1; ii<samples; ++ii) os << "," << stats[ii].system_stats.vm_resident; os << "\n";
    os <<"\tnet_recv_KBps=" << stats[0].system_stats.net_recv_KBps;
    for (ii=1; ii<samples; ++ii) os << "," << stats[ii].system_stats.net_recv_KBps; os << "\n";
    os <<"\tnet_send_KBps=" << stats[0].system_stats.net_send_KBps;
    for (ii=1; ii<samples; ++ii) os << "," << stats[ii].system_stats.net_send_KBps; os << "\n";
    os <<"\tloadavg_0=" << format("%.2f", ((float)stats[0].system_stats.loadavg_0)/100);
    for (ii=1; ii<samples; ++ii) os << "," << format("%.2f", ((float)stats[ii].system_stats.loadavg_0)/100); os << "\n";
    os <<"\tloadavg_1=" << format("%.2f", ((float)stats[0].system_stats.loadavg_1)/100);
    for (ii=1; ii<samples; ++ii) os << "," << format("%.2f",((float)stats[ii].system_stats.loadavg_1)/100); os << "\n";
    os <<"\tloadavg_2=" << format("%.2f", ((float)stats[0].system_stats.loadavg_2)/100);
    for (ii=1; ii<samples; ++ii) os << "," << format("%.2f", ((float)stats[ii].system_stats.loadavg_2)/100); os << "\n";
    os <<"\tcpu_pct=" << format("%.2f",((float)stats[0].system_stats.cpu_pct)/100);
    for (ii=1; ii<samples; ++ii) os << "," << format("%.2f", ((float)stats[ii].system_stats.cpu_pct)/100); os << "\n";
    os <<"\tnum_cores=" << (int)stats[0].system_stats.num_cores;
    for (ii=1; ii<samples; ++ii) os << "," << (int)stats[ii].system_stats.num_cores; os << "\n";
    os <<"\tclock_mhz=" << stats[0].system_stats.clock_mhz;
    for (ii=1; ii<samples; ++ii) os << "," << stats[ii].system_stats.clock_mhz; os << "\n";
    // RangeServer info
    os << "\tnum_ranges=" << stats[0].num_ranges;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].num_ranges; os << "\n";
    os << "\tscans=" << stats[0].scans;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].scans; os << "\n";
    os << "\tcells_read=" << stats[0].cells_read;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].cells_read; os << "\n";
    os << "\tbytes_read=" << stats[0].bytes_read;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].bytes_read; os << "\n";
    os << "\tupdates=" << stats[0].updates;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].updates; os << "\n";
    os << "\tcells_written=" << stats[0].cells_written;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].cells_written; os << "\n";
    os << "\tbytes_written=" << stats[0].bytes_written;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].bytes_written; os << "\n";
    os << "\tsyncs=" << stats[0].syncs;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].syncs; os << "\n";
    os << "\tquery_cache_max_memory=" << stats[0].query_cache_max_memory;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].query_cache_max_memory; os << "\n";
    os << "\tquery_cache_available_memory=" << stats[0].query_cache_available_memory;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].query_cache_available_memory; os << "\n";
    os << "\tquery_cache_accesses=" << stats[0].query_cache_accesses;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].query_cache_accesses; os << "\n";
    os << "\tquery_cache_hits=" << stats[0].query_cache_hits;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].query_cache_hits; os << "\n";
    os << "\tblock_cache_max_memory=" << stats[0].block_cache_max_memory;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].block_cache_max_memory; os << "\n";
    os << "\tblock_cache_available_memory=" << stats[0].block_cache_available_memory;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].block_cache_available_memory; os << "\n";
    os << "\tblock_cache_accesses=" << stats[0].block_cache_accesses;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].block_cache_accesses; os << "\n";
    os << "\tblock_cache_hits=" << stats[0].block_cache_hits;
    for(ii=1; ii<samples; ++ii) os << "," << stats[ii].block_cache_hits; os << "\n";
  }

} // anonymous namespace

void dump_table_snapshot_buffer(TableStatsSnapshotBuffer &buffer,
    map<uint32_t, String> &table_ids_to_names, ostream &os)
{
  int capacity = buffer.capacity();
  int size = buffer.size();
  uint32_t samples;

  // iterate thru all tables in latest snapshot and dump out their stats
  TableStatsMap::iterator last_tsm_it;
  TableStatsMap::iterator tsm_it;
  TableStatsSnapshotBuffer::iterator last_tssb_it = buffer.begin();
  TableStatsSnapshotBuffer::iterator tssb_it;
  uint32_t table_id;
  String table_name;

  last_tsm_it = (*last_tssb_it)->map.begin();
  while (last_tsm_it != (*last_tssb_it)->map.end()) {
    vector<int64_t> timestamps(3);
    vector<TableStats> stats(3);
    table_id = last_tsm_it->first;
    if (table_ids_to_names.find(table_id) == table_ids_to_names.end())
      table_name.clear();
    else
      table_name = table_ids_to_names[table_id];

    // we have latest sample
    samples = 1;
    timestamps[0] = (*last_tssb_it)->timestamp;
    stats[0] = (*(last_tsm_it->second));

    // check if we have data for this table for half the history
    if (size >= capacity/2)  {
      tssb_it = last_tssb_it + (capacity-1)/2;
      tsm_it = (*tssb_it)->map.find(table_id);
      if (tsm_it != (*tssb_it)->map.end()) {
        // aggregate samples upto half the history
        samples++;
        timestamps[1] = (*tssb_it)->timestamp;
        ++tssb_it;
        accumulate_table_stats(last_tssb_it, tssb_it, table_id, stats[1]);
      }
      // check if we have data for this table all the way back
      if (size == capacity)  {
        tssb_it = last_tssb_it + capacity-1;
        tsm_it = (*tssb_it)->map.find(table_id);
        if (tsm_it != (*tssb_it)->map.end()) {
          // aggregate samples for the whole history
          samples++;
          timestamps[2] = (*tssb_it)->timestamp;
          ++tssb_it;
          accumulate_table_stats(last_tssb_it, tssb_it, table_id, stats[2]);
        }
      }
    }

    //os << "Table ID=" << table_id << "\n";
    os << "Table ID=" << table_name << "\n";
    dump_table_stat_samples(samples, stats, timestamps, os);

    ++last_tsm_it;
  }

}

void dump_range_server_snapshot_buffer(RangeServerHLStatsSnapshotBuffer &buffer, ostream &os)
{
  int capacity = buffer.capacity();
  int size = buffer.size();
  uint32_t samples;

  // iterate thru all rangeservers in latest snapshot and dump out their stats
  RangeServerHLStatsMap::iterator last_rsm_it;
  RangeServerHLStatsMap::iterator rsm_it;
  RangeServerHLStatsSnapshotBuffer::iterator last_rssb_it = buffer.begin();
  RangeServerHLStatsSnapshotBuffer::iterator rssb_it;
  String server_id;

  last_rsm_it = (*last_rssb_it)->map.begin();
  while (last_rsm_it != (*last_rssb_it)->map.end()) {
    vector<int64_t> timestamps(3);
    vector<RangeServerHLStats> stats(3);
    server_id = last_rsm_it->first;

    // we have latest sample
    samples = 1;
    timestamps[0] = (*last_rssb_it)->timestamp;
    stats[0] = *(last_rsm_it->second);

    // check if we have data for this table for half the history
    if (size >= capacity/2)  {
      rssb_it = last_rssb_it + (capacity-1)/2;
      rsm_it = (*rssb_it)->map.find(server_id);
      if (rsm_it != (*rssb_it)->map.end()) {
        // aggregate samples upto half the history
        samples++;
        timestamps[1] = (*rssb_it)->timestamp;
        ++rssb_it;
        accumulate_range_server_stats(last_rssb_it, rssb_it, server_id, stats[1]);
      }
      // check if we have data for this range server all the way back
      if (size == capacity)  {
        rssb_it = last_rssb_it + capacity - 1;
        rsm_it = (*rssb_it)->map.find(server_id);
        if (rsm_it != (*rssb_it)->map.end()) {
          // aggregate samples for the whole history
          samples++;
          timestamps[2] = (*rssb_it)->timestamp;
          ++rssb_it;
          accumulate_range_server_stats(last_rssb_it, rssb_it, server_id, stats[2]);
        }
      }
    }

    os << "RangeServer = " << server_id << "\n";
    dump_range_server_stat_samples(samples, stats, timestamps, os);

    ++last_rsm_it;
  }
}


} // namespace Hypertable
