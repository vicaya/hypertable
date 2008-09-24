/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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


namespace Hypertable {

  class AccessGroup : CellList {

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

    AccessGroup(const TableIdentifier *identifier, SchemaPtr &schema_ptr, Schema::AccessGroup *ag, const RangeSpec *range);
    virtual ~AccessGroup();
    virtual int add(const Key &key, const ByteString value);

    virtual const char *get_split_row();
    virtual void get_split_rows(std::vector<String> &split_rows, bool include_cache);
    virtual void get_cached_rows(std::vector<String> &rows);

    void lock() { m_mutex.lock(); m_cell_cache_ptr->lock(); }
    void unlock() { m_cell_cache_ptr->unlock(); m_mutex.unlock(); }

    CellListScanner *create_scanner(ScanContextPtr &scan_ctx);

    bool include_in_scan(ScanContextPtr &scan_ctx);
    uint64_t disk_usage();
    uint64_t memory_usage();
    void add_cell_store(CellStorePtr &cellstore_ptr, uint32_t id);
    void run_compaction(bool major);

    int64_t get_earliest_cached_revision() {
      boost::mutex::scoped_lock lock(m_mutex);
      return m_earliest_cached_revision;
    }

    void get_compaction_priority_data(CompactionPriorityData &priority_data);

    void set_compaction_bit() { m_needs_compaction = true; }

    bool needs_compaction() { return m_needs_compaction; }

    void initiate_compaction();

    const char *get_name() { return m_name.c_str(); }

    int shrink(String &new_start_row);

    uint64_t get_collision_count() {
      boost::mutex::scoped_lock lock(m_mutex);
      return m_collisions + m_cell_cache_ptr->get_collision_count();
    }

    uint64_t get_cached_count() {
      boost::mutex::scoped_lock lock(m_mutex);
      return m_cell_cache_ptr->size();
    }

    void drop() { m_drop = true; }

    void get_files(String &text);

    void release_files(const std::vector<String> &files);

  private:

    typedef hash_map<String, uint32_t> FileRefCountMap;
    void increment_file_refcount(const String &filename);
    bool decrement_file_refcount(const String &filename);

    void update_files_column();
    void merge_caches();

    Mutex                m_mutex;
    boost::condition     m_scanner_blocked_cond;
    TableIdentifierManaged m_identifier;
    SchemaPtr            m_schema_ptr;
    std::set<uint8_t>    m_column_families;
    String               m_name;
    String               m_table_name;
    String               m_start_row;
    String               m_end_row;
    String               m_range_name;
    std::vector<CellStorePtr> m_stores;
    CellCachePtr         m_cell_cache_ptr;
    CellCachePtr         m_immutable_cache_ptr;
    uint32_t             m_next_table_id;
    uint64_t             m_disk_usage;
    uint32_t             m_blocksize;
    float                m_compression_ratio;
    String               m_compressor;
    bool                 m_is_root;
    int64_t              m_compaction_revision;
    int64_t              m_earliest_cached_revision;
    uint64_t             m_collisions;
    bool                 m_needs_compaction;
    bool                 m_in_memory;
    bool                 m_drop;
    std::set<String>     m_gc_locked_files;
    std::set<String>     m_live_files;
    FileRefCountMap      m_file_refcounts;
    bool                 m_scanners_blocked;
  };

}

#endif // HYPERTABLE_ACCESSGROUP_H
