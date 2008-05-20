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
#include <string>
#include <vector>

#include <ext/hash_map>

#include <boost/thread/condition.hpp>

#include "Common/String.h"
#include "Common/StringExt.h"

#include "Hypertable/Lib/Schema.h"
#include "Hypertable/Lib/Types.h"

#include "CellCache.h"
#include "CellStore.h"
#include "Timestamp.h"

using namespace Hypertable;

namespace Hypertable {

  class AccessGroup : CellList {

  public:

    typedef struct {
      AccessGroup *ag;
      uint64_t oldest_cached_timestamp;
      uint64_t mem_used;
      uint64_t disk_used;
      uint32_t deletes;
      bool in_memory;
      void *user_data;
    } CompactionPriorityDataT;

    AccessGroup(TableIdentifier *identifier, SchemaPtr &schemaPtr, Schema::AccessGroup *ag, RangeSpec *range);
    virtual ~AccessGroup();
    virtual int add(const ByteString key, const ByteString value, uint64_t real_timestamp);
    bool replay_add(const ByteString key, const ByteString value, uint64_t real_timestamp);

    virtual const char *get_split_row();
    virtual void get_split_rows(std::vector<String> &split_rows, bool include_cache);
    virtual void get_cached_rows(std::vector<String> &rows);

    void lock() { m_mutex.lock(); m_cell_cache_ptr->lock(); }
    void unlock() { m_cell_cache_ptr->unlock(); m_mutex.unlock(); }

    CellListScanner *create_scanner(ScanContextPtr &scanContextPtr);

    bool include_in_scan(ScanContextPtr &scanContextPtr);
    uint64_t disk_usage();
    void add_cell_store(CellStorePtr &cellstore_ptr, uint32_t id);
    void run_compaction(Timestamp timestamp, bool major);

    void get_compaction_timestamp(Timestamp &timestamp);
    uint64_t get_oldest_cached_timestamp() {
      boost::mutex::scoped_lock lock(m_mutex);
      return m_oldest_cached_timestamp;
    }

    void get_compaction_priority_data(CompactionPriorityDataT &priority_data);

    void set_compaction_bit() { m_needs_compaction = true; }

    bool needs_compaction() { return m_needs_compaction; }

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

    typedef __gnu_cxx::hash_map<String, uint32_t> FileRefCountMapT;
    void increment_file_refcount(const String &filename);
    bool decrement_file_refcount(const String &filename);

    void update_files_column();

    Mutex                m_mutex;
    boost::condition     m_scanner_blocked_cond;
    TableIdentifierCopy  m_identifier;
    SchemaPtr            m_schema_ptr;
    std::set<uint8_t>    m_column_families;
    String          m_name;
    String          m_table_name;
    String          m_start_row;
    String          m_end_row;
    std::vector<CellStorePtr> m_stores;
    CellCachePtr         m_cell_cache_ptr;
    uint32_t             m_next_table_id;
    uint64_t             m_disk_usage;
    uint32_t             m_blocksize;
    float                m_compression_ratio;
    String               m_compressor;
    bool                 m_is_root;
    Timestamp            m_compaction_timestamp;
    uint64_t             m_oldest_cached_timestamp;
    uint64_t             m_collisions;
    bool                 m_needs_compaction;
    bool                 m_in_memory;
    bool                 m_drop;
    std::set<String>     m_gc_locked_files;
    std::set<String>     m_live_files;
    FileRefCountMapT     m_file_refcounts;
    bool                 m_scanners_blocked;
  };

}

#endif // HYPERTABLE_ACCESSGROUP_H
