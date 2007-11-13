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
#ifndef HYPERTABLE_RANGE_H
#define HYPERTABLE_RANGE_H

#include <map>
#include <string>
#include <vector>

#include <boost/shared_ptr.hpp>
#include <boost/thread/condition.hpp>

#include "Hypertable/Lib/Key.h"
#include "Hypertable/Lib/Schema.h"

#include "AccessGroup.h"
#include "CellStore.h"
#include "CommitLog.h"
#include "RangeInfo.h"

namespace Hypertable {

  class Range : public CellList {

    typedef std::map<string, AccessGroup *> AccessGroupMapT;
    typedef std::vector<AccessGroup *>  ColumnFamilyVectorT;

  public:
    Range(SchemaPtr &schemaPtr, RangeInfoPtr &rangeInfoPtr);
    virtual ~Range() { return; }
    virtual int add(const ByteString32T *key, const ByteString32T *value);
    virtual void lock();
    virtual void unlock();
    virtual ByteString32T *get_split_key();

    uint64_t get_log_cutoff_time();
    uint64_t disk_usage();

    CellListScanner *create_scanner(ScanContextPtr &scanContextPtr);

    string &start_row() { return m_start_row; }
    string &end_row() { return m_end_row; }
    string &table_name() { return m_table_name; }

    uint64_t get_timestamp() {
      boost::mutex::scoped_lock lock(m_mutex);
      return m_latest_timestamp;
    }

    void schedule_maintenance();
    void do_maintenance();
    void do_compaction(bool major=false);

    void increment_update_counter();
    void decrement_update_counter();

    bool get_split_info(ByteString32Ptr &splitKeyPtr, CommitLogPtr &splitLogPtr, uint64_t *splitStartTime) {
      boost::mutex::scoped_lock lock(m_mutex);
      *splitStartTime = m_split_start_time;
      if (m_split_start_time == 0)
	return false;
      splitKeyPtr = m_split_key_ptr;
      splitLogPtr = m_split_log_ptr;
      return true;
    }

    std::vector<AccessGroup *> &access_group_vector() { return m_access_group_vector; }
    AccessGroup *get_access_group(string &lgName) { return m_access_group_map[lgName]; }

  private:
    void replay_commit_log(string &logDir, uint64_t minLogCutoff);
    bool extract_access_group_from_path(std::string &path, std::string &name, uint32_t *tableIdp);

    uint64_t run_compaction(bool major=false);

    boost::mutex     m_mutex;
    std::string      m_table_name;
    SchemaPtr        m_schema;
    std::string      m_start_row;
    std::string      m_end_row;
    ByteString32Ptr  m_start_key_ptr;
    ByteString32Ptr  m_end_key_ptr;
    AccessGroupMapT        m_access_group_map;
    std::vector<AccessGroup *>  m_access_group_vector;
    ColumnFamilyVectorT      m_column_family_vector;
    bool       m_maintenance_in_progress;

    uint64_t         m_latest_timestamp;
    uint64_t         m_split_start_time;
    ByteString32Ptr  m_split_key_ptr;
    CommitLogPtr     m_split_log_ptr;

    boost::mutex     m_maintenance_mutex;
    boost::condition m_maintenance_finished_cond;
    boost::condition m_update_quiesce_cond;
    bool             m_hold_updates;
    uint32_t         m_update_counter;
  };

  typedef boost::shared_ptr<Range> RangePtr;

}


#endif // HYPERTABLE_RANGE_H
