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

#include "AccessGroup.h"
#include "CellStore.h"
#include "MaintenanceTask.h"
#include "Metadata.h"
#include "RangeUpdateBarrier.h"
#include "ScannerTimestampController.h"
#include "Timestamp.h"

namespace Hypertable {

  /**
   * Represents a table row range.
   */
  class Range : public CellList {

    typedef std::map<String, AccessGroup *> AccessGroupMap;
    typedef std::vector<AccessGroup *>  ColumnFamilyVector;

  public:
    Range(MasterClientPtr &master_client_ptr, const TableIdentifier *identifier, SchemaPtr &schema_ptr, const RangeSpec *range, const RangeState *state);
    virtual ~Range();
    virtual int add(const ByteString key, const ByteString value, uint64_t real_timestamp);
    virtual const char *get_split_row() { return 0; }

    int replay_add(const ByteString key, const ByteString value, uint64_t real_timestamp, uint32_t *num_addedp);

    void lock();
    void unlock(uint64_t real_timestamp);

    uint64_t disk_usage();

    CellListScanner *create_scanner(ScanContextPtr &scan_ctx);

    String start_row() {
      boost::mutex::scoped_lock lock(m_mutex);
      return m_start_row;
    }

    /**
     * Returns the end row of the range.
     *
     * @return the end row of the range
     *
     * This does not need to be protected by a lock because the end row of a range never changes.
     */
    String end_row() {
      return m_end_row;
    }

    String table_name() {
      return (String)m_identifier.name;
    }

    uint64_t get_latest_timestamp();
    bool get_scan_timestamp(Timestamp &ts);

    void replay_transfer_log(CommitLogReader *commit_log_reader, uint64_t real_timestamp);

    void get_compaction_priority_data(std::vector<AccessGroup::CompactionPriorityData> &priority_data_vector);


    bool test_and_set_maintenance() {
      boost::mutex::scoped_lock lock(m_mutex);
      bool old_value = m_maintenance_in_progress;
      m_maintenance_in_progress = true;
      return old_value;
    }

    bool maintenance_in_progress() {
      return m_maintenance_in_progress;
    }

    void split();
    void compact(bool major=false);

    void increment_update_counter() {
      m_update_barrier.enter();
    }
    void decrement_update_counter() {
      m_update_barrier.exit();
    }

    void add_update_timestamp(Timestamp &ts) {
      m_scanner_timestamp_controller.add_update_timestamp(ts);
    }

    void remove_update_timestamp(Timestamp &ts) {
      m_scanner_timestamp_controller.remove_update_timestamp(ts);
    }

    bool get_split_info(String &split_row, CommitLogPtr &split_log_ptr) {
      boost::mutex::scoped_lock lock(m_mutex);
      split_row = m_split_row;
      split_log_ptr = m_split_log_ptr;
      return (bool)m_split_log_ptr;
    }

    std::vector<AccessGroup *> &access_group_vector() {
      return m_access_group_vector;
    }

    AccessGroup *get_access_group(const String &agname) {
      return m_access_group_map[agname];
    }

    void dump_stats();

    uint64_t get_size_limit() { return m_state.soft_limit; }

    bool is_root() { return m_is_root; }

    void drop() {
      boost::mutex::scoped_lock lock(m_mutex);
      for (size_t i=0; i<m_access_group_vector.size(); i++)
        m_access_group_vector[i]->drop();
    }

    String get_name() {
      boost::mutex::scoped_lock lock(m_mutex);
      return (String)m_name;
    }

  private:

    void load_cell_stores(Metadata *metadata);

    bool extract_csid_from_path(String &path, uint32_t *csidp);

    void run_compaction(bool major=false);

    void split_install_log(Timestamp *timestampp, String &old_start_row);
    void split_compact_and_shrink(Timestamp timestamp, String &old_start_row);
    void split_notify_master(String &old_start_row);

    boost::mutex        m_mutex;
    MasterClientPtr     m_master_client_ptr;
    TableIdentifierManaged m_identifier;
    SchemaPtr           m_schema;
    String  m_start_row;
    String  m_end_row;
    String  m_name;
    AccessGroupMap        m_access_group_map;
    std::vector<AccessGroup *>  m_access_group_vector;
    ColumnFamilyVector      m_column_family_vector;
    bool       m_maintenance_in_progress;

    Timestamp        m_timestamp;
    uint64_t         m_last_logical_timestamp;
    String      m_split_row;
    CommitLogPtr     m_split_log_ptr;

    RangeUpdateBarrier m_update_barrier;
    bool             m_is_root;
    ScannerTimestampController m_scanner_timestamp_controller;
    uint64_t         m_added_deletes[3];
    uint64_t         m_added_inserts;
    RangeStateManaged m_state;
    int32_t          m_error;
  };

  typedef boost::intrusive_ptr<Range> RangePtr;

}


#endif // HYPERTABLE_RANGE_H
