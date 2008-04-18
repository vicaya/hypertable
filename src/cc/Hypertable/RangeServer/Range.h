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
#include <string>
#include <vector>

#include <boost/thread/condition.hpp>

#include "Hypertable/Lib/CommitLog.h"
#include "Hypertable/Lib/Key.h"
#include "Hypertable/Lib/MasterClient.h"
#include "Hypertable/Lib/Schema.h"

#include "AccessGroup.h"
#include "CellStore.h"
#include "Metadata.h"
#include "ScannerTimestampController.h"
#include "MaintenanceTask.h"
#include "Timestamp.h"

namespace Hypertable {

  class Range : public CellList {

    typedef std::map<string, AccessGroup *> AccessGroupMapT;
    typedef std::vector<AccessGroup *>  ColumnFamilyVectorT;

  public:
    Range(MasterClientPtr &master_client_ptr, TableIdentifier &identifier, SchemaPtr &schemaPtr, RangeSpec *range, uint64_t soft_limit);
    virtual ~Range();
    virtual int add(const ByteString32T *key, const ByteString32T *value, uint64_t real_timestamp);
    virtual const char *get_split_row();
    void lock();
    void unlock(uint64_t real_timestamp);

    uint64_t disk_usage();

    CellListScanner *create_scanner(ScanContextPtr &scanContextPtr);

    string start_row() {
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
    string end_row() {
      return m_end_row;
    }

    string table_name() {
      return (std::string)m_identifier.name;
    }

    uint64_t get_latest_timestamp();
    bool get_scan_timestamp(Timestamp &ts);

    void replay_transfer_log(const string &logDir, uint64_t real_timestamp);

    void get_compaction_priority_data(std::vector<AccessGroup::CompactionPriorityDataT> &priority_data_vector);


    bool test_and_set_maintenance() {
      boost::mutex::scoped_lock lock(m_mutex);
      bool old_value = m_maintenance_in_progress;
      m_maintenance_in_progress = true;
      return old_value;
    }

    void do_split();
    void do_compaction(bool major=false);

    void increment_update_counter();
    void decrement_update_counter();

    void add_update_timestamp(Timestamp &ts) {
      m_scanner_timestamp_controller.add_update_timestamp(ts);
    }

    void remove_update_timestamp(Timestamp &ts) {
      m_scanner_timestamp_controller.remove_update_timestamp(ts);
    }

    bool get_split_info(std::string &split_row, CommitLogPtr &split_log_ptr) {
      boost::mutex::scoped_lock lock(m_mutex);
      split_row = m_split_row;
      split_log_ptr = m_split_log_ptr;
      return (bool)m_split_log_ptr;
    }

    std::vector<AccessGroup *> &access_group_vector() { return m_access_group_vector; }
    AccessGroup *get_access_group(string &lgName) { return m_access_group_map[lgName]; }

    void dump_stats();

    uint64_t get_size_limit() { return m_disk_limit; }

    bool is_root() { return m_is_root; }

    void drop() {
      boost::mutex::scoped_lock lock(m_mutex);
      for (size_t i=0; i<m_access_group_vector.size(); i++)
	m_access_group_vector[i]->drop();
    }

  private:

    void load_cell_stores(Metadata *metadata);

    bool extract_csid_from_path(std::string &path, uint32_t *storeIdp);

    void run_compaction(bool major=false);

    boost::mutex     m_mutex;
    MasterClientPtr  m_master_client_ptr;
    TableIdentifier m_identifier;
    SchemaPtr        m_schema;
    std::string      m_start_row;
    std::string      m_end_row;
    AccessGroupMapT        m_access_group_map;
    std::vector<AccessGroup *>  m_access_group_vector;
    ColumnFamilyVectorT      m_column_family_vector;
    bool       m_maintenance_in_progress;

    Timestamp        m_timestamp;
    uint64_t         m_last_logical_timestamp;
    std::string      m_split_row;
    CommitLogPtr     m_split_log_ptr;

    boost::mutex     m_maintenance_mutex;
    boost::condition m_maintenance_finished_cond;
    boost::condition m_update_quiesce_cond;
    bool             m_hold_updates;
    uint32_t         m_update_counter;
    bool             m_is_root;
    ScannerTimestampController m_scanner_timestamp_controller;
    uint64_t         m_added_deletes[3];
    uint64_t         m_added_inserts;
    uint64_t         m_disk_limit;
  };

  typedef boost::intrusive_ptr<Range> RangePtr;

}


#endif // HYPERTABLE_RANGE_H
