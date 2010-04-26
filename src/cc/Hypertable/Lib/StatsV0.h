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

#ifndef HYPERTABLE_STATSV0_H
#define HYPERTABLE_STATSV0_H

#include "Common/Compat.h"
#include <map>
#include <iostream>

#include "Stats.h"
#include "Types.h"

#include "Common/Mutex.h"

namespace Hypertable {

  using namespace std;
  using namespace MonitoringStatsV0;


  class RangeServerStatsV0 : public RangeServerStats {
  public:
    /** Statistics of a Range */
    class RangeStats {
      public:
      /**
       * Stats in this class need to be saved to fill in missing values
       * in the next future stats bundle. Use it only for stats who's missing value
       * does not default to 0.
       */
      class RangeStatsDefaultLast {
      public:
        RangeStatsDefaultLast(): disk_used(0), memory_used(0), memory_allocated(0),
            shadow_cache_memory(0), block_index_memory(0), bloom_filter_memory(0) {}
        uint64_t disk_used;
        uint64_t memory_used;
        uint64_t memory_allocated;
        uint64_t shadow_cache_memory;
        uint64_t block_index_memory;
        uint64_t bloom_filter_memory;
      };

      /**
       * Stats in this class default to zero in case of missing/unknown vals.
       */
      class RangeStatsDefaultZero {
      public:
        RangeStatsDefaultZero(): scans(0), cells_read(0), bytes_read(0), cells_written(0),
                           bytes_written(0), bloom_filter_accesses(0), bloom_filter_maybes(0),
                           bloom_filter_false_positives(0) {}

        void reset() {
          scans = 0;
          cells_read = 0;
          bytes_read = 0;
          cells_written = 0;
          bytes_written = 0;
          bloom_filter_accesses = 0;
          bloom_filter_maybes = 0;
          bloom_filter_false_positives = 0;
        }

        uint32_t scans;
        uint32_t cells_read;
        uint64_t bytes_read;
        uint32_t cells_written;
        uint64_t bytes_written;
        uint32_t bloom_filter_accesses;
        uint32_t bloom_filter_maybes;
        uint32_t bloom_filter_false_positives;
      };

      RangeStats() {}

      void set_id(const RangeIdentifier &range, uint32_t table, uint32_t generation);
      void update(const uint8_t **bufp, size_t *remainp, bool update_table_stats,
                  TableStats *table_stats);
      void get_id(RangeIdentifier &range, uint32_t &table, uint32_t &generation);
      uint32_t get_uint32_val(const uint8_t value_id);
      uint64_t get_uint64_val(const uint8_t value_id);
      void dump_str(String &out);

    protected:
      void update_table_last_stats(TableStats *table);

      RangeStatsDefaultLast    default_last;
      RangeStatsDefaultZero    default_zero;
      RangeIdentifier range_id;
      uint32_t table_id;
      uint32_t schema_generation;
    }; //RangeStats

  /** Statistics of a RangeServer */
  public:
    RangeServerStatsV0() : timestamp(0), measurement_period(0), num_ranges(0),
        scans(0), cells_read(0), bytes_read(0), updates(0), cells_written(0), bytes_written(0),
        syncs(0), query_cache_max_memory(0), query_cache_available_memory(0),
        query_cache_accesses(0), query_cache_hits(0), block_cache_max_memory(0),
        block_cache_available_memory(0), block_cache_accesses(0), block_cache_hits(0) {}

    void process_stats(const uint8_t **bufp, size_t *remainp, bool update_table_stats,
                       TableStatsMap  &table_stats_map);
    void dump_str(String &out);
    void dump_rrd(const String &file_prefix);
    void get_hl_stats(RangeServerHLStats &hl_stats);

  protected:
    Mutex mutex;

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

    typedef map<RangeIdentifier, RangeStats*> RangeStatsMap;
    typedef map<RangeIdentifier, bool> RangeStatsExistenceMap;

    RangeStatsExistenceMap range_exists_map;
    RangeStatsMap range_stats_map;
  };

  ostream &operator<<(ostream &os, RangeServerStatsV0 &stat);

} // namespace Hypertable

#endif // HYPERTABLE_STATSV0_H
