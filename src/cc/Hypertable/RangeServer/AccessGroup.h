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

#include <boost/thread/condition.hpp>

#include "Common/String.h"
#include "Common/StringExt.h"
#include "Common/HashMap.h"

#include "Hypertable/Lib/Schema.h"
#include "Hypertable/Lib/Types.h"

#include "CellCache.h"
#include "CellStore.h"
#include "LiveFileTracker.h"


namespace Hypertable {

  class AccessGroup : public CellList {

  public:

    struct CompactionPriorityData {
      AccessGroup *ag;
      int64_t earliest_cached_revision;
      uint64_t mem_used;
      uint64_t disk_used;
      uint64_t log_space_pinned;
      uint32_t deletes;
      void *user_data;
      bool in_memory;
    };

    AccessGroup(const TableIdentifier *identifier, SchemaPtr &schema,
                Schema::AccessGroup *ag, const RangeSpec *range);
    virtual ~AccessGroup();
    virtual void add(const Key &key, const ByteString value);

    virtual const char *get_split_row();
    virtual void get_split_rows(std::vector<String> &split_rows,
                                bool include_cache);
    virtual void get_cached_rows(std::vector<String> &rows);

    virtual uint32_t get_total_entries() {
      boost::mutex::scoped_lock lock(m_mutex);
      uint32_t total = m_cell_cache->get_total_entries();
      if (m_immutable_cache)
        total += m_immutable_cache->get_total_entries();
      if (!m_in_memory) {
        for (size_t i=0; i<m_stores.size(); i++)
          total += m_stores[i]->get_total_entries();
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
    void add_cell_store(CellStorePtr &cellstore, uint32_t id);
    void run_compaction(bool major);

    int64_t get_earliest_cached_revision() {
      ScopedLock lock(m_mutex);
      return m_earliest_cached_revision;
    }

    void get_compaction_priority_data(CompactionPriorityData &priority_data);

    void set_compaction_bit() { m_needs_compaction = true; }

    bool needs_compaction() { return m_needs_compaction; }

    void initiate_compaction();

    bool compaction_initiated() {
      ScopedLock lock(m_mutex);
      return (bool)m_immutable_cache;
    }

    const char *get_name() { return m_name.c_str(); }

    void shrink(String &split_row, bool drop_high);

    uint64_t get_collision_count() {
      ScopedLock lock(m_mutex);
      return m_collisions + m_cell_cache->get_collision_count();
    }

    uint64_t get_cached_count() {
      ScopedLock lock(m_mutex);
      return m_cell_cache->size();
    }

    void drop() { m_drop = true; }

    void get_file_list(String &file_list, bool include_blocked) {
      m_file_tracker.get_file_list(file_list, include_blocked);
    }

    void release_files(const std::vector<String> &files);

    void recovery_initialize() { m_recovering = true; }
    void recovery_finalize() { m_recovering = false; }

  private:
    void update_files_column(const String &end_row, const String &file_list);
    void merge_caches();

    Mutex                m_mutex;
    TableIdentifierManaged m_identifier;
    SchemaPtr            m_schema;
    std::set<uint8_t>    m_column_families;
    String               m_name;
    String               m_table_name;
    String               m_start_row;
    String               m_end_row;
    String               m_range_name;
    std::vector<CellStorePtr> m_stores;
    PropertiesPtr        m_cellstore_props;
    CellCachePtr         m_cell_cache;
    CellCachePtr         m_immutable_cache;
    uint32_t             m_next_cs_id;
    uint64_t             m_disk_usage;
    float                m_compression_ratio;
    bool                 m_is_root;
    int64_t              m_compaction_revision;
    int64_t              m_earliest_cached_revision;
    int64_t              m_earliest_cached_revision_saved;
    uint64_t             m_collisions;
    bool                 m_needs_compaction;
    bool                 m_in_memory;
    bool                 m_drop;
    LiveFileTracker      m_file_tracker;
    bool                 m_recovering;
    bool                 m_bloom_filter_disabled;
  };
  typedef boost::intrusive_ptr<AccessGroup> AccessGroupPtr;


} // namespace Hypertable

#endif // HYPERTABLE_ACCESSGROUP_H
