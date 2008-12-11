/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#include "Common/String.h"

#include "Hypertable/Lib/CommitLog.h"
#include "Hypertable/Lib/CommitLogReader.h"
#include "Hypertable/Lib/Key.h"
#include "Hypertable/Lib/MasterClient.h"
#include "Hypertable/Lib/RangeState.h"
#include "Hypertable/Lib/Schema.h"
#include "Hypertable/Lib/Stat.h"
#include "Hypertable/Lib/Timestamp.h"

#include "AccessGroup.h"
#include "CellStore.h"
#include "MaintenanceTask.h"
#include "Metadata.h"
#include "RangeSet.h"
#include "RangeUpdateBarrier.h"
#include "SplitPredicate.h"

namespace Hypertable {


  /**
   * Represents a table row range.
   */
  class Range : public CellList {

    typedef std::map<String, AccessGroup *> AccessGroupMap;
    typedef std::vector<AccessGroup *>  ColumnFamilyVector;

  public:
    Range(MasterClientPtr &, const TableIdentifier *, SchemaPtr &,
          const RangeSpec *range, RangeSet *range_set, const RangeState *);
    virtual ~Range();
    virtual int add(const Key &key, const ByteString value);
    virtual const char *get_split_row() { return 0; }

    virtual uint32_t get_total_entries() {
      boost::mutex::scoped_lock lock(m_mutex);
      uint32_t total = 0;
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
     *
     * @return the end row of the range
     *
     * This does not need to be protected by a lock because the end row of a
     * range never changes.
     */
    String end_row() {
      ScopedLock lock(m_mutex);
      return m_end_row;
    }

    /**
     * Checks if given row_key belongs to the range
     *
     * @param row_key row key to check
     * @return true if row_key belongs to range, false otherwise
     */
    bool belongs(const char *row_key) {
      ScopedLock lock(m_mutex);
      return (strcmp(row_key, m_start_row.c_str()) > 0) && (strcmp(row_key, m_end_row.c_str()) <= 0);
    }

    const char *table_name() const { return m_identifier.name; }

    uint32_t table_id() const { return m_identifier.id; }

    int64_t get_scan_revision();

    void replay_transfer_log(CommitLogReader *commit_log_reader);

    typedef std::vector<AccessGroup::CompactionPriorityData>
            CompactionPriorityData;
    void
    get_compaction_priority_data(CompactionPriorityData &priority_data_vector);

    bool test_and_set_maintenance() {
      ScopedLock lock(m_mutex);
      bool old_value = m_maintenance_in_progress;
      m_maintenance_in_progress = true;
      return old_value;
    }

    bool maintenance_in_progress() {
      return m_maintenance_in_progress;
    }

    void split();

    void compact(bool major=false);

    void post_replay();

    void increment_update_counter() {
      m_update_barrier.enter();
    }
    void decrement_update_counter() {
      m_update_barrier.exit();
    }

    bool get_split_info(SplitPredicate &predicate, CommitLogPtr &split_log_ptr) {
      ScopedLock lock(m_mutex);
      if (m_split_log_ptr) {
        predicate.load(m_split_row, m_split_off_high);
        split_log_ptr = m_split_log_ptr;
        return true;
      }
      predicate.clear();
      return false;
    }

    std::vector<AccessGroup *> &access_group_vector() {
      return m_access_group_vector;
    }

    AccessGroup *get_access_group(const String &agname) {
      return m_access_group_map[agname];
    }

    void dump_stats();
    void get_statistics(RangeStat *stat);

    uint64_t get_size_limit() { return m_state.soft_limit; }

    bool is_root() { return m_is_root; }

    void drop() {
      ScopedLock lock(m_mutex);
      for (size_t i=0; i<m_access_group_vector.size(); i++)
        m_access_group_vector[i]->drop();
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

    void run_compaction(bool major=false);

    void split_install_log();
    void split_compact_and_shrink();
    void split_notify_master();

    Mutex            m_mutex;
    MasterClientPtr  m_master_client_ptr;
    TableIdentifierManaged m_identifier;
    SchemaPtr        m_schema;
    String           m_start_row;
    String           m_end_row;
    String           m_name;
    AccessGroupMap   m_access_group_map;
    std::vector<AccessGroup *>  m_access_group_vector;
    ColumnFamilyVector  m_column_family_vector;
    bool             m_maintenance_in_progress;
    int64_t          m_revision;
    int64_t          m_latest_revision;

    String           m_split_row;
    CommitLogPtr     m_split_log_ptr;
    bool             m_split_off_high;

    RangeUpdateBarrier m_update_barrier;
    bool             m_is_root;
    uint64_t         m_added_deletes[3];
    uint64_t         m_added_inserts;
    RangeSetPtr      m_range_set_ptr;
    RangeStateManaged m_state;
    int32_t          m_error;
  };

  typedef boost::intrusive_ptr<Range> RangePtr;

}


#endif // HYPERTABLE_RANGE_H
