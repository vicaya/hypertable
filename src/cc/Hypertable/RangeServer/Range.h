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

#ifndef HYPERTABLE_RANGE_H
#define HYPERTABLE_RANGE_H

#include <map>
#include <vector>

#include "Common/Barrier.h"
#include "Common/String.h"

#include "Hypertable/Lib/CommitLog.h"
#include "Hypertable/Lib/CommitLogReader.h"
#include "Hypertable/Lib/Key.h"
#include "Hypertable/Lib/MasterClient.h"
#include "Hypertable/Lib/RangeState.h"
#include "Hypertable/Lib/Schema.h"
#include "Hypertable/Lib/Timestamp.h"
#include "Hypertable/Lib/Types.h"

#include "AccessGroup.h"
#include "CellStore.h"
#include "MaintenanceFlag.h"
#include "Metadata.h"
#include "RangeMaintenanceGuard.h"
#include "RangeSet.h"
#include "SplitPredicate.h"

namespace Hypertable {

  /**
   * Represents a table row range.
   */
  class Range : public CellList {

  public:

    class MaintenanceData {
    public:
      int64_t compactable_memory() {
        int64_t total = 0;
        for (AccessGroup::MaintenanceData *ag = agdata; ag; ag = ag->next)
          total += ag->mem_used;
        return total;
      }
      Range *range;
      AccessGroup::MaintenanceData *agdata;
      const char *table_id;
      uint64_t bytes_read;
      uint64_t scans;
      uint64_t cells_read; // only includes cells returned by scans not skipped cells
      uint64_t bytes_written;
      uint64_t cells_written;
      int64_t  purgeable_index_memory;
      int64_t  compact_memory;
      uint32_t schema_generation;
      RangeIdentifier range_id;
      int32_t  priority;
      int16_t  state;
      int16_t  maintenance_flags;
      uint64_t memory_used;
      uint64_t memory_allocated;
      uint64_t disk_used;
      uint64_t shadow_cache_memory;
      uint64_t block_index_memory;
      uint64_t bloom_filter_memory;
      uint32_t bloom_filter_accesses;
      uint32_t bloom_filter_maybes;
      uint32_t bloom_filter_fps;
      bool     busy;
      bool     is_metadata;
    };

    typedef std::map<String, AccessGroup *> AccessGroupMap;
    typedef std::vector<AccessGroupPtr> AccessGroupVector;

    Range(MasterClientPtr &, const TableIdentifier *, SchemaPtr &,
          const RangeSpec *, RangeSet *, const RangeState *);
    virtual ~Range() {}
    virtual void add(const Key &key, const ByteString value);
    virtual const char *get_split_row() { return 0; }

    virtual int64_t get_total_entries() {
      boost::mutex::scoped_lock lock(m_mutex);
      int64_t total = 0;
      for (size_t i=0; i<m_access_group_vector.size(); i++)
        total += m_access_group_vector[i]->get_total_entries();
      return total;
    }

    void lock();
    void unlock();

    uint64_t disk_usage();

    CellListScanner *create_scanner(ScanContextPtr &scan_ctx);

    String start_row() {
      ScopedLock lock(m_mutex);
      return m_start_row;
    }

    /**
     * Returns the end row of the range.
     */
    String end_row() {
      ScopedLock lock(m_mutex);
      return m_end_row;
    }
    /**
     * Returns a Range id which is contains the first 8 bytes each of the md5 hashed
     * start row and end row
     */
    RangeIdentifier get_start_end_id() {
      ScopedLock lock(m_mutex);
      return m_start_end_id;
    }

    int64_t get_scan_revision();

    void replay_transfer_log(CommitLogReader *commit_log_reader);

    MaintenanceData *get_maintenance_data(ByteArena &arena, time_t now);

    void wait_for_maintenance_to_complete() {
      m_maintenance_guard.wait_for_complete();
    }

    void update_schema(SchemaPtr &schema);

    void split();

    void compact(MaintenanceFlag::Map &subtask_map);

    void purge_memory(MaintenanceFlag::Map &subtask_map);

    void recovery_initialize() {
      ScopedLock lock(m_mutex);
      for (size_t i=0; i<m_access_group_vector.size(); i++)
        m_access_group_vector[i]->recovery_initialize();
    }

    void recovery_finalize();

    void increment_update_counter() {
      m_update_barrier.enter();
    }
    void decrement_update_counter() {
      m_update_barrier.exit();
    }

    void increment_scan_counter() {
      m_scan_barrier.enter();
    }
    void decrement_scan_counter() {
      m_scan_barrier.exit();
    }

    /**
     * @param predicate
     * @param split_log
     * @param latest_revisionp
     * @param wait_for_maintenance true if this range has exceeded its capacity and
     *        future requests to this range need to be throttled till split/compaction reduces
     *        range size
     * @return
     */
    bool get_split_info(SplitPredicate &predicate, CommitLogPtr &split_log,
                        int64_t *latest_revisionp, bool &wait_for_maintenance) {
      bool retval;
      ScopedLock lock(m_mutex);

      wait_for_maintenance = false;
      *latest_revisionp = m_latest_revision;
      if (m_split_log) {
        predicate.load(m_split_row, m_split_off_high);
        split_log = m_split_log;
        retval = true;
      }
      else {
        predicate.clear();
        retval = false;
      }
      if (m_capacity_exceeded_throttle == true)
        wait_for_maintenance = true;

      return retval;
    }

    void add_read_data(uint64_t cells, uint64_t bytes) {
      m_bytes_read += bytes;
      m_cells_read += cells;
    }

    void add_bytes_written(uint64_t n) {
      m_bytes_written += n;
    }

    void add_cells_written(uint64_t n) {
      m_cells_written += n;
    }

    uint64_t get_size_limit() { return m_state.soft_limit; }

    bool need_maintenance();

    bool is_root() { return m_is_root; }

    void drop() {
      ScopedLock lock(m_mutex);
      for (size_t i=0; i<m_access_group_vector.size(); i++)
        m_access_group_vector[i]->drop();
      m_dropped = true;
    }

    String get_name() {
      ScopedLock lock(m_mutex);
      return (String)m_name;
    }

    int get_state() { return m_state.state; }

    int32_t get_error() { return m_error; }

  private:

    void load_cell_stores(Metadata *metadata);

    bool extract_csid_from_path(String &path, uint32_t *csidp);

    bool cancel_maintenance();

    void split_install_log();
    void split_compact_and_shrink();
    void split_notify_master();

    void split_install_log_rollback_metadata();

    // these need to be aligned
    uint64_t         m_bytes_read;
    uint64_t         m_cells_read;
    uint64_t         m_scans;
    uint64_t         m_bytes_written;
    uint64_t         m_cells_written;

    Mutex            m_mutex;
    Mutex            m_schema_mutex;
    MasterClientPtr  m_master_client;
    TableIdentifierManaged m_identifier;
    SchemaPtr        m_schema;
    String           m_start_row;
    String           m_end_row;
    String           m_name;
    RangeIdentifier  m_start_end_id;
    AccessGroupMap     m_access_group_map;
    AccessGroupVector  m_access_group_vector;
    std::vector<AccessGroup *>       m_column_family_vector;
    RangeMaintenanceGuard m_maintenance_guard;
    int64_t          m_revision;
    int64_t          m_latest_revision;
    String           m_split_row;
    CommitLogPtr     m_split_log;
    bool             m_split_off_high;
    Barrier          m_update_barrier;
    Barrier          m_scan_barrier;
    bool             m_is_root;
    uint64_t         m_added_deletes[3];
    uint64_t         m_added_inserts;
    RangeSet        *m_range_set;
    RangeStateManaged m_state;
    int32_t          m_error;
    bool             m_dropped;
    bool             m_capacity_exceeded_throttle;
    int64_t          m_maintenance_generation;
  };

  typedef intrusive_ptr<Range> RangePtr;

} // namespace Hypertable

#endif // HYPERTABLE_RANGE_H
