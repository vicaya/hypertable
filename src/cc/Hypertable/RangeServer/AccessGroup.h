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

#ifndef HYPERTABLE_ACCESSGROUP_H
#define HYPERTABLE_ACCESSGROUP_H

#include <queue>
#include <set>
#include <vector>
#include <cstdio>
#include <ctime>
#include <iostream>
#include <fstream>

#include <boost/thread/condition.hpp>

#include "Common/PageArena.h"
#include "Common/String.h"
#include "Common/StringExt.h"
#include "Common/HashMap.h"

#include "Hypertable/Lib/Schema.h"
#include "Hypertable/Lib/Types.h"

#include "AccessGroupGarbageTracker.h"
#include "CellCache.h"
#include "CellStore.h"
#include "CellStoreTrailerV4.h"
#include "LiveFileTracker.h"
#include "MaintenanceFlag.h"


namespace Hypertable {

  class AccessGroup : public CellList {

  public:

    class CellStoreMaintenanceData {
    public:
      void *user_data;
      CellStoreMaintenanceData *next;
      CellStore *cs;
      CellStore::IndexMemoryStats index_stats;
      uint64_t shadow_cache_size;
      int64_t  shadow_cache_ecr;
      uint32_t shadow_cache_hits;
      int16_t  maintenance_flags;
    };

    class MaintenanceData {
    public:
      void *user_data;
      MaintenanceData *next;
      AccessGroup *ag;
      CellStoreMaintenanceData *csdata;
      int64_t earliest_cached_revision;
      int64_t latest_stored_revision;
      int64_t mem_used;
      int64_t mem_allocated;
      int64_t cached_items;
      uint64_t cell_count;
      int64_t immutable_items;
      int64_t disk_used;
      int64_t disk_estimate;
      int64_t log_space_pinned;
      int32_t deletes;
      int32_t outstanding_scanners;
      float    compression_ratio;
      int16_t  maintenance_flags;
      uint64_t block_index_memory;
      uint64_t bloom_filter_memory;
      uint32_t bloom_filter_accesses;
      uint32_t bloom_filter_maybes;
      uint32_t bloom_filter_fps;
      uint64_t shadow_cache_memory;
      bool     in_memory;
      bool     gc_needed;
    };

    class CellStoreInfo {
    public:
      CellStoreInfo(CellStore *csp) :
	cs(csp), shadow_cache_ecr(TIMESTAMP_MAX), shadow_cache_hits(0), bloom_filter_accesses(0),
 bloom_filter_maybes(0), bloom_filter_fps(0) {
        init_from_trailer();
      }
      CellStoreInfo(CellStorePtr &csp) :
	cs(csp), shadow_cache_ecr(TIMESTAMP_MAX), shadow_cache_hits(0), bloom_filter_accesses(0),
 bloom_filter_maybes(0), bloom_filter_fps(0) {
        init_from_trailer();
      }
      CellStoreInfo(CellStorePtr &csp, CellCachePtr &scp, int64_t ecr) :
	cs(csp), shadow_cache(scp), shadow_cache_ecr(ecr), shadow_cache_hits(0),
 bloom_filter_accesses(0), bloom_filter_maybes(0), bloom_filter_fps(0)  {
        init_from_trailer();
      }
      CellStoreInfo() : cell_count(0), shadow_cache_ecr(TIMESTAMP_MAX), shadow_cache_hits(0),
      bloom_filter_accesses(0), bloom_filter_maybes(0), bloom_filter_fps(0) { }
      void init_from_trailer() {
        try {
           int divisor = (boost::any_cast<uint32_t>(cs->get_trailer()->get("flags")) & CellStoreTrailerV4::SPLIT) ? 2 : 1;
          cell_count = boost::any_cast<int64_t>(cs->get_trailer()->get("total_entries"))
                       / divisor;
          timestamp_min = boost::any_cast<int64_t>(cs->get_trailer()->get("timestamp_min"));
          timestamp_max = boost::any_cast<int64_t>(cs->get_trailer()->get("timestamp_max"));
          expirable_data = boost::any_cast<int64_t>(cs->get_trailer()->get("expirable_data"))
                           / divisor ;
        }
        catch (std::exception &e) {
          cell_count = 0;
          timestamp_min = TIMESTAMP_MAX;
          timestamp_max = TIMESTAMP_MIN;
          expirable_data = 0;
        }
      }
      CellStorePtr cs;
      CellCachePtr shadow_cache;
      uint64_t cell_count;
      int64_t shadow_cache_ecr;
      uint32_t shadow_cache_hits;
      uint32_t bloom_filter_accesses;
      uint32_t bloom_filter_maybes;
      uint32_t bloom_filter_fps;
      int64_t timestamp_min;
      int64_t timestamp_max;
      int64_t expirable_data;
      int64_t total_data;
    };

    AccessGroup(const TableIdentifier *identifier, SchemaPtr &schema,
                Schema::AccessGroup *ag, const RangeSpec *range);

    virtual void add(const Key &key, const ByteString value);

    virtual const char *get_split_row();
    virtual void get_split_rows(std::vector<String> &split_rows,
                                bool include_cache);
    virtual void get_cached_rows(std::vector<String> &rows);

    virtual int64_t get_total_entries() {
      boost::mutex::scoped_lock lock(m_mutex);
      int64_t total = m_cell_cache->get_total_entries();
      if (m_immutable_cache)
        total += m_immutable_cache->get_total_entries();
      if (!m_in_memory) {
        for (size_t i=0; i<m_stores.size(); i++)
          total += m_stores[i].cs->get_total_entries();
      }
      return total;
    }

    void update_schema(SchemaPtr &schema_ptr, Schema::AccessGroup *ag);

    void lock() { m_mutex.lock(); m_cell_cache->lock(); }
    void unlock() { m_cell_cache->unlock(); m_mutex.unlock(); }

    CellListScanner *create_scanner(ScanContextPtr &scan_ctx);

    bool include_in_scan(ScanContextPtr &scan_ctx);
    uint64_t disk_usage();
    uint64_t memory_usage();
    void space_usage(int64_t *memp, int64_t *diskp);
    void add_cell_store(CellStorePtr &cellstore);

    void compute_garbage_stats(uint64_t *input_bytesp, uint64_t *output_bytesp);

    void run_compaction(int maintenance_flags);

    uint64_t purge_memory(MaintenanceFlag::Map &subtask_map);

    MaintenanceData *get_maintenance_data(ByteArena &arena, time_t now);

    void stage_compaction();

    void unstage_compaction();

    const char *get_name() { return m_name.c_str(); }

    const char *get_full_name() { return m_full_name.c_str(); }

    void shrink(String &split_row, bool drop_high);

    uint64_t get_collision_count() {
      ScopedLock lock(m_mutex);
      return m_collisions + m_cell_cache->get_collision_count();
    }

    uint64_t get_cached_count() {
      ScopedLock lock(m_mutex);
      return m_cell_cache->size();
    }

    void get_file_list(String &file_list, bool include_blocked) {
      m_file_tracker.get_file_list(file_list, include_blocked);
    }

    void release_files(const std::vector<String> &files);

    void recovery_initialize() { m_recovering = true; }
    void recovery_finalize() { m_recovering = false; }

    void dump_keys(std::ofstream &out);

  private:

    void merge_caches();

    void range_dir_initialize();

    Mutex                m_mutex;
    Mutex                m_outstanding_scanner_mutex;
    int32_t              m_outstanding_scanner_count;
    TableIdentifierManaged m_identifier;
    SchemaPtr            m_schema;
    std::set<uint8_t>    m_column_families;
    String               m_name;
    String               m_full_name;
    String               m_table_name;
    String               m_range_dir;
    String               m_start_row;
    String               m_end_row;
    String               m_range_name;
    std::vector<CellStoreInfo> m_stores;
    PropertiesPtr        m_cellstore_props;
    CellCachePtr         m_cell_cache;
    CellCachePtr         m_immutable_cache;
    uint32_t             m_next_cs_id;
    uint64_t             m_disk_usage;
    float                m_compression_ratio;
    int64_t              m_compaction_revision;
    int64_t              m_earliest_cached_revision;
    int64_t              m_earliest_cached_revision_saved;
    int64_t              m_latest_stored_revision;
    uint64_t             m_collisions;
    LiveFileTracker      m_file_tracker;
    AccessGroupGarbageTracker m_garbage_tracker;
    bool                 m_is_root;
    bool                 m_in_memory;
    bool                 m_recovering;
    bool                 m_bloom_filter_disabled;

  };
  typedef boost::intrusive_ptr<AccessGroup> AccessGroupPtr;


} // namespace Hypertable

#endif // HYPERTABLE_ACCESSGROUP_H
