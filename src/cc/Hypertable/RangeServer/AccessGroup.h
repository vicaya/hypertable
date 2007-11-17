/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This file is part of Hypertable.
 * 
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
#ifndef HYPERTABLE_ACCESSGROUP_H
#define HYPERTABLE_ACCESSGROUP_H

#include <queue>
#include <string>
#include <vector>

#include "Hypertable/Lib/Schema.h"

#include "CellCache.h"
#include "CellStore.h"
#include "RangeInfo.h"

using namespace Hypertable;

namespace Hypertable {

  class AccessGroup : CellList {

  public:

    AccessGroup(SchemaPtr &schemaPtr, Schema::AccessGroup *lg, RangeInfoPtr &tabletInfoPtr);
    virtual ~AccessGroup();
    virtual int add(const ByteString32T *key, const ByteString32T *value);

    virtual const char *get_split_row();
    virtual void get_split_rows(std::vector<std::string> &split_rows);

    void lock() { m_lock.lock(); m_cell_cache_ptr->lock(); }
    void unlock() { m_cell_cache_ptr->unlock(); m_lock.unlock(); }

    CellListScanner *create_scanner(ScanContextPtr &scanContextPtr);

    bool include_in_scan(ScanContextPtr &scanContextPtr);
    uint64_t disk_usage();
    void add_cell_store(CellStorePtr &cellStorePtr, uint32_t id);
    bool needs_compaction();
    void run_compaction(uint64_t timestamp, bool major);
    uint64_t get_log_cutoff_time() { return m_log_cutoff_time; }

    int shrink(std::string &new_start_row);

  private:
    boost::mutex         m_mutex;
    boost::mutex::scoped_lock  m_lock;
    SchemaPtr            m_schema_ptr;
    std::set<uint8_t>    m_column_families;
    std::string          m_name;
    std::string          m_table_name;
    std::string          m_start_row;
    std::string          m_end_row;
    std::vector<CellStorePtr> m_stores;
    CellCachePtr         m_cell_cache_ptr;
    uint32_t             m_next_table_id;
    uint64_t             m_log_cutoff_time;
    uint64_t             m_disk_usage;
  };

}

#endif // HYPERTABLE_ACCESSGROUP_H
