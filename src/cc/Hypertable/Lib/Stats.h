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

#ifndef HYPERTABLE_STATS_H
#define HYPERTABLE_STATS_H

#include "Common/Compat.h"

#include <boost/circular_buffer.hpp>

#include "Common/HashMap.h"
#include "Common/ServerStats.h"
#include "Common/ReferenceCount.h"

#include "RangeServerProtocol.h"


namespace Hypertable {

  using namespace std;

  // abstract base class for table stats
  class TableStats {
  public:
    TableStats() : scans(0), cells_read(0), bytes_read(0), cells_written(0),
                   bytes_written(0), bloom_filter_accesses(0),
                   bloom_filter_maybes(0), bloom_filter_false_positives(0), disk_used(0),
                   memory_used(0), memory_allocated(0), shadow_cache_memory(0),
                   block_index_memory(0), bloom_filter_memory(0) {}

    void dump_str(String &out);
    void reset() {
      scans = 0;
      cells_read = 0;
      bytes_read = 0;
      cells_written = 0;
      bytes_written = 0;
      bloom_filter_accesses = 0;
      bloom_filter_maybes = 0;
      bloom_filter_false_positives = 0;
      disk_used = 0;
      memory_used = 0;
      memory_allocated = 0;
      shadow_cache_memory = 0;
      block_index_memory = 0;
      bloom_filter_memory = 0;
    }

    uint64_t scans;
    uint32_t cells_read;
    uint64_t bytes_read;
    uint32_t cells_written;
    uint64_t bytes_written;
    uint32_t bloom_filter_accesses;
    uint32_t bloom_filter_maybes;
    uint32_t bloom_filter_false_positives;
    uint64_t disk_used;
    uint64_t memory_used;
    uint64_t memory_allocated;
    uint64_t shadow_cache_memory;
    uint64_t block_index_memory;
    uint64_t bloom_filter_memory;
  };

  typedef hash_map<uint32_t, TableStats*> TableStatsMap;

  class TableStatsSnapshot : public ReferenceCount {
  public:
    ~TableStatsSnapshot() {
      TableStatsMap::iterator ts_it = map.begin();
       while (ts_it != map.end()) {
         delete ts_it->second;
         ++ts_it;
       }
    }

    int64_t timestamp;
    TableStatsMap map;
  };

  typedef boost::intrusive_ptr<TableStatsSnapshot> TableStatsSnapshotPtr;
  typedef boost::circular_buffer<TableStatsSnapshotPtr> TableStatsSnapshotBuffer;

  void dump_table_snapshot_buffer(TableStatsSnapshotBuffer &buffer,
                                  ostream &os);

  /** High level statistics of a RangeServer */
  class RangeServerHLStats {
  public:
    RangeServerHLStats() : timestamp(0), measurement_period(0), num_ranges(0),
        scans(0), cells_read(0), bytes_read(0), updates(0), cells_written(0), bytes_written(0),
        syncs(0), query_cache_max_memory(0), query_cache_available_memory(0),
        query_cache_accesses(0), query_cache_hits(0), block_cache_max_memory(0),
        block_cache_available_memory(0), block_cache_accesses(0), block_cache_hits(0) {}
    void dump_str(String &out);

    int64_t timestamp;
    uint32_t measurement_period;
    uint32_t num_ranges;
    uint32_t scans;
    uint32_t cells_read;
    uint64_t bytes_read;
    uint32_t updates;
    uint32_t cells_written;
    uint64_t bytes_written;
    uint32_t syncs;
    uint64_t query_cache_max_memory;
    uint64_t query_cache_available_memory;
    uint32_t query_cache_accesses;
    uint32_t query_cache_hits;
    uint64_t block_cache_max_memory;
    uint64_t block_cache_available_memory;
    uint32_t block_cache_accesses;
    uint32_t block_cache_hits;
    ServerStatsBundle system_stats;
  };

  /** Statistics of a RangeServer down to per range stats */
  class RangeServerStats {
  public:
    RangeServerStats() {}

    virtual ~RangeServerStats() {
    }
    virtual void process_stats(const uint8_t **bufp, size_t *remainp,
                               bool update_table_stats, TableStatsMap  &table_stats)=0;
    virtual void dump_str(String &out)=0;
    virtual void dump_rrd(const String &file_prefix)=0;
    virtual void get_hl_stats(RangeServerHLStats &hl_stats)=0;

  };

  typedef hash_map<String, RangeServerStats*> RangeServerStatsMap;
  typedef hash_map<String, RangeServerHLStats*> RangeServerHLStatsMap;

  /**
   * Store a snapshot of high level stats for all the RangeServers
   */
  class RangeServerHLStatsSnapshot : public ReferenceCount {
  public:
    ~RangeServerHLStatsSnapshot() {
      // All RangeServerHLStats objects are destroyed here
      RangeServerHLStatsMap::iterator rs_it = map.begin();
       while (rs_it != map.end()) {
         delete rs_it->second;
         ++rs_it;
       }
    }

    int64_t timestamp;
    RangeServerHLStatsMap map;
  };

  typedef boost::intrusive_ptr<RangeServerHLStatsSnapshot> RangeServerHLStatsSnapshotPtr;
  typedef boost::circular_buffer<RangeServerHLStatsSnapshotPtr> RangeServerHLStatsSnapshotBuffer;
  void dump_range_server_snapshot_buffer(RangeServerHLStatsSnapshotBuffer &buffer, ostream &os);


  ostream &operator<<(ostream &os, TableStats &stat);
} // namespace Hypertable

#endif // HYPERTABLE_STATS_H
