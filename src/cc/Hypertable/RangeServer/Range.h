/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
#include "RangeInfo.h"
#include "ScannerTimestampController.h"
#include "MaintenanceTask.h"
#include "Timestamp.h"

namespace Hypertable {

  class Range : public CellList {

    typedef std::map<string, AccessGroup *> AccessGroupMapT;
    typedef std::vector<AccessGroup *>  ColumnFamilyVectorT;

  public:
    Range(MasterClientPtr &master_client_ptr, TableIdentifierT &identifier, SchemaPtr &schemaPtr, RangeT *range, uint64_t soft_limit);
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

    string end_row() {
      boost::mutex::scoped_lock lock(m_mutex);
      return m_end_row;
    }

    uint64_t get_timestamp();

    int replay_split_log(string &logDir, uint64_t real_timestamp);

    MaintenanceTask *get_maintenance();
    void do_split();
    void do_compaction(bool major=false);

    void increment_update_counter();
    void decrement_update_counter();

    void add_update_timestamp(uint64_t timestamp) {
      m_scanner_timestamp_controller.add_update_timestamp(timestamp);
    }
    void remove_update_timestamp(uint64_t timestamp) {
      m_scanner_timestamp_controller.remove_update_timestamp(timestamp);      
    }

    bool get_split_info(std::string &split_row, CommitLogPtr &splitLogPtr, uint64_t *splitStartTime) {
      boost::mutex::scoped_lock lock(m_mutex);
      *splitStartTime = m_split_timestamp.logical;
      if (m_split_timestamp.logical == 0) {
	split_row = "";
	return false;
      }
      split_row = m_split_row;
      splitLogPtr = m_split_log_ptr;
      return true;
    }

    std::vector<AccessGroup *> &access_group_vector() { return m_access_group_vector; }
    AccessGroup *get_access_group(string &lgName) { return m_access_group_map[lgName]; }

    void dump_stats();

  private:

    void load_cell_stores(Metadata *metadata);

    bool extract_csid_from_path(std::string &path, uint32_t *storeIdp);

    void run_compaction(bool major=false);

    boost::mutex     m_mutex;
    MasterClientPtr  m_master_client_ptr;
    TableIdentifierT m_identifier;
    SchemaPtr        m_schema;
    std::string      m_start_row;
    std::string      m_end_row;
    ByteString32Ptr  m_start_key_ptr;
    ByteString32Ptr  m_end_key_ptr;
    AccessGroupMapT        m_access_group_map;
    std::vector<AccessGroup *>  m_access_group_vector;
    ColumnFamilyVectorT      m_column_family_vector;
    bool       m_maintenance_in_progress;

    Timestamp        m_timestamp;
    uint64_t         m_last_logical_timestamp;
    Timestamp        m_split_timestamp;
    std::string      m_split_row;
    CommitLogPtr     m_split_log_ptr;

    boost::mutex     m_maintenance_mutex;
    boost::condition m_maintenance_finished_cond;
    boost::condition m_update_quiesce_cond;
    bool             m_hold_updates;
    uint32_t         m_update_counter;
    bool             m_is_root;
    ScannerTimestampController m_scanner_timestamp_controller;
    uint64_t         m_added;
    uint64_t         m_disk_limit;

  };

  typedef boost::intrusive_ptr<Range> RangePtr;

}


#endif // HYPERTABLE_RANGE_H
