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

#include "Common/Compat.h"
#include <cassert>
#include <string>
#include <vector>

extern "C" {
#include <poll.h>
#include <string.h>
}

#include <boost/algorithm/string.hpp>

#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/md5.h"

#include "Hypertable/Lib/CommitLog.h"
#include "Hypertable/Lib/CommitLogReader.h"


#include "CellStoreV0.h"
#include "Global.h"
#include "MergeScanner.h"
#include "MetadataNormal.h"
#include "MetadataRoot.h"
#include "Range.h"


using namespace Hypertable;
using namespace std;


Range::Range(MasterClientPtr &master_client_ptr, TableIdentifier *identifier,
             SchemaPtr &schema_ptr, RangeSpec *range, RangeState *state)
    : m_master_client_ptr(master_client_ptr), m_identifier(*identifier),
      m_schema(schema_ptr), m_maintenance_in_progress(false),
      m_last_logical_timestamp(0), m_added_inserts(0), m_state(*state),
      m_error(Error::OK) {
  AccessGroup *ag;

  memset(m_added_deletes, 0, 3*sizeof(int64_t));

  if (m_state.soft_limit == 0 || m_state.soft_limit > Global::range_max_bytes)
    m_state.soft_limit = Global::range_max_bytes;

  m_start_row = range->start_row;
  m_end_row = range->end_row;

  m_name = std::string(identifier->name) + "[" + m_start_row + ".." + m_end_row + "]";

  m_is_root = (m_identifier.id == 0 && *range->start_row == 0 && !strcmp(range->end_row, Key::END_ROOT_ROW));

  m_column_family_vector.resize(m_schema->get_max_column_family_id() + 1);

  list<Schema::AccessGroup *> *aglist = m_schema->get_access_group_list();

  for (list<Schema::AccessGroup *>::iterator ag_it = aglist->begin(); ag_it != aglist->end(); ag_it++) {
    ag = new AccessGroup(identifier, m_schema, (*ag_it), range);
    m_access_group_map[(*ag_it)->name] = ag;
    m_access_group_vector.push_back(ag);
    for (list<Schema::ColumnFamily *>::iterator cf_it = (*ag_it)->columns.begin(); cf_it != (*ag_it)->columns.end(); cf_it++)
      m_column_family_vector[(*cf_it)->id] = ag;
  }

  /**
   * Read the cell store files from METADATA and
   */
  if (m_is_root) {
    MetadataRoot metadata(m_schema);
    load_cell_stores(&metadata);
  }
  else {
    MetadataNormal metadata(&m_identifier, m_end_row);
    load_cell_stores(&metadata);
  }

  return;
}


/**
 */
Range::~Range() {
  for (size_t i=0; i<m_access_group_vector.size(); i++)
    delete m_access_group_vector[i];
}



/**
 *
 */
void Range::load_cell_stores(Metadata *metadata) {
  int error;
  AccessGroup *ag;
  CellStorePtr cellstore;
  uint32_t csid;
  const char *base, *ptr, *end;
  std::vector<std::string> csvec;
  std::string ag_name;
  std::string files;
  std::string file_str;

  metadata->reset_files_scan();

  while (metadata->get_next_files(ag_name, files)) {
    csvec.clear();

    if ((ag = m_access_group_map[ag_name]) == 0) {
      HT_ERRORF("Unrecognized access group name '%s' found in METADATA for table '%s'", ag_name.c_str(), m_identifier.name);
      continue;
    }

    ptr = base = (const char *)files.c_str();
    end = base + strlen(base);
    while (ptr < end) {

      while (*ptr != ';' && ptr < end)
        ptr++;

      file_str = std::string(base, ptr-base);
      boost::trim(file_str);

      if (file_str[0] == '#')
        file_str = file_str.substr(1);

      if (file_str != "")
        csvec.push_back(file_str);

      ++ptr;
      base = ptr;
    }

    for (size_t i=0; i<csvec.size(); i++) {

      HT_INFOF("Loading CellStore %s", csvec[i].c_str());

      cellstore = new CellStoreV0(Global::dfs);

      if (!extract_csid_from_path(csvec[i], &csid)) {
        HT_ERRORF("Unable to extract cell store ID from path '%s'", csvec[i].c_str());
        continue;
      }
      if ((error = cellstore->open(csvec[i].c_str(), m_start_row.c_str(), m_end_row.c_str())) != Error::OK) {
        // this should throw an exception
        HT_ERRORF("Problem opening cell store '%s', skipping...", csvec[i].c_str());
        continue;
      }
      if ((error = cellstore->load_index()) != Error::OK) {
        // this should throw an exception
        HT_ERRORF("Problem loading index of cell store '%s', skipping...", csvec[i].c_str());
        continue;
      }

      ag->add_cell_store(cellstore, csid);
    }

  }

}



/**
 */
bool Range::extract_csid_from_path(std::string &path, uint32_t *csidp) {
  const char *base;

  if ((base = strrchr(path.c_str(), '/')) == 0 || strncmp(base, "/cs", 3))
    *csidp = 0;
  else
    *csidp = atoi(base+3);

  return true;
}


/**
 */
int Range::add(const ByteString key, const ByteString value, uint64_t real_timestamp) {
  Key key_comps;

  if (m_error)
    return m_error;

  HT_EXPECT(key_comps.load(key), Error::FAILED_EXPECTATION);

  if (key_comps.column_family_code >= m_column_family_vector.size()) {
    HT_ERRORF("Bad column family (%d)", key_comps.column_family_code);
    return Error::RANGESERVER_INVALID_COLUMNFAMILY;
  }

  /**
     keys in a batch may come in out-of-order if they're supplied because
     they get sorted by row key in the client.  This *shouldn't* be a problem.

  if (key_comps.timestamp <= m_last_logical_timestamp) {
    if (key_comps.flag == FLAG_INSERT) {
      HT_ERRORF("Problem adding key/value pair, key timestmap %llu <= %llu", key_comps.timestamp, m_last_logical_timestamp);
      return Error::RANGESERVER_TIMESTAMP_ORDER_ERROR;
    }
  }
  else
    m_last_logical_timestamp = key_comps.timestamp;
  */

  if (key_comps.timestamp > m_last_logical_timestamp)
    m_last_logical_timestamp = key_comps.timestamp;

  if (key_comps.flag == FLAG_DELETE_ROW) {
    for (AccessGroupMap::iterator iter = m_access_group_map.begin(); iter != m_access_group_map.end(); iter++) {
      (*iter).second->add(key, value, real_timestamp);
    }
  }
  else
    m_column_family_vector[key_comps.column_family_code]->add(key, value, real_timestamp);

  if (key_comps.flag == FLAG_INSERT)
    m_added_inserts++;
  else
    m_added_deletes[key_comps.flag]++;

  return Error::OK;
}


/**
 */
int Range::replay_add(const ByteString key, const ByteString value, uint64_t real_timestamp, uint32_t *num_addedp) {
  Key key_comps;

  HT_EXPECT(key_comps.load(key), Error::FAILED_EXPECTATION);

  *num_addedp = 0;

  if (key_comps.column_family_code >= m_column_family_vector.size()) {
    HT_ERRORF("Bad column family (%d)", key_comps.column_family_code);
    return Error::RANGESERVER_INVALID_COLUMNFAMILY;
  }

  if (key_comps.flag == FLAG_DELETE_ROW) {
    for (AccessGroupMap::iterator iter = m_access_group_map.begin(); iter != m_access_group_map.end(); iter++) {
      if ((*iter).second->replay_add(key, value, real_timestamp))
        (*num_addedp)++;
    }
  }
  else {
    if (m_column_family_vector[key_comps.column_family_code]->replay_add(key, value, real_timestamp))
      (*num_addedp)++;
  }

  if (key_comps.flag == FLAG_INSERT)
    m_added_inserts++;
  else
    m_added_deletes[key_comps.flag]++;

  return Error::OK;
}



CellListScanner *Range::create_scanner(ScanContextPtr &scan_ctx) {
  bool return_deletes = scan_ctx->spec ? scan_ctx->spec->return_deletes : false;
  MergeScanner *mscanner = new MergeScanner(scan_ctx, return_deletes);
  for (AccessGroupMap::iterator iter = m_access_group_map.begin(); iter != m_access_group_map.end(); iter++) {
    if ((*iter).second->include_in_scan(scan_ctx))
      mscanner->add_scanner((*iter).second->create_scanner(scan_ctx));
  }
  return mscanner;
}


uint64_t Range::disk_usage() {
  uint64_t usage = 0;
  for (size_t i=0; i<m_access_group_vector.size(); i++)
    usage += m_access_group_vector[i]->disk_usage();
  return usage;
}



/**
 *
 */
void Range::get_compaction_priority_data(std::vector<AccessGroup::CompactionPriorityData> &priority_data_vector) {
  size_t next_slot = priority_data_vector.size();

  priority_data_vector.resize(priority_data_vector.size() + m_access_group_vector.size());

  for (size_t i=0; i<m_access_group_vector.size(); i++) {
    m_access_group_vector[i]->get_compaction_priority_data(priority_data_vector[next_slot]);
    next_slot++;
  }
}


void Range::split() {
  Timestamp timestamp;
  String old_start_row;

  HT_EXPECT(m_maintenance_in_progress, Error::FAILED_EXPECTATION);
  HT_EXPECT(!m_is_root, Error::FAILED_EXPECTATION);

  try {

    switch (m_state.state) {

    case (RangeState::STEADY):
      split_install_log(&timestamp, old_start_row);
      m_state.state = RangeState::SPLIT_LOG_INSTALLED;

    case (RangeState::SPLIT_LOG_INSTALLED):
      split_compact_and_shrink(timestamp, old_start_row);
      m_state.state = RangeState::SPLIT_SHRUNK;

    case (RangeState::SPLIT_SHRUNK):
      split_notify_master(old_start_row);
      m_state.state = RangeState::STEADY;
      m_state.transfer_log = "";

    }

  }
  catch (Exception &e) {
    m_maintenance_in_progress = false;
    throw;
  }

  HT_INFOF("Split Complete.  New Range end_row=%s", m_start_row.c_str());

  m_maintenance_in_progress = false;
}




/**
 */
void Range::split_install_log(Timestamp *timestampp, String &old_start_row) {
  std::vector<String> split_rows;
  char md5DigestStr[33];

  for (size_t i=0; i<m_access_group_vector.size(); i++)
    m_access_group_vector[i]->get_split_rows(split_rows, false);

  /**
   * If we didn't get at least one row from each Access Group, then try again
   * the hard way (scans CellCache for middle row)
   */
  if (split_rows.size() < m_access_group_vector.size()) {
    for (size_t i=0; i<m_access_group_vector.size(); i++)
      m_access_group_vector[i]->get_split_rows(split_rows, true);
  }
  sort(split_rows.begin(), split_rows.end());

  /**
  cout << flush;
  cout << "thelma Dumping split rows for " << m_name << "\n";
  for (size_t i=0; i<split_rows.size(); i++)
    cout << "thelma Range::get_split_row [" << i << "] = " << split_rows[i] << "\n";
  cout << flush;
  */

  /**
   * If we still didn't get a good split row, try again the *really* hard way
   * by collecting all of the cached rows, sorting them and then taking the middle.
   */
  if (split_rows.size() > 0) {
    boost::mutex::scoped_lock lock(m_mutex);
    m_split_row = split_rows[split_rows.size()/2];
    if (m_split_row < m_start_row || m_split_row >= m_end_row) {
      split_rows.clear();
      for (size_t i=0; i<m_access_group_vector.size(); i++)
        m_access_group_vector[i]->get_cached_rows(split_rows);
      if (split_rows.size() > 0) {
        sort(split_rows.begin(), split_rows.end());
        m_split_row = split_rows[split_rows.size()/2];
        if (m_split_row < m_start_row || m_split_row >= m_end_row) {
          m_error = Error::RANGESERVER_ROW_OVERFLOW;
          HT_THROWF(Error::RANGESERVER_ROW_OVERFLOW,
                    "Unable to determine split row for range %s[%s..%s]",
                    m_identifier.name, m_start_row.c_str(), m_end_row.c_str());
        }
      }
      else {
        m_error = Error::RANGESERVER_ROW_OVERFLOW;
        HT_THROWF(Error::RANGESERVER_ROW_OVERFLOW,
                  "Unable to determine split row for range %s[%s..%s]",
                   m_identifier.name, m_start_row.c_str(), m_end_row.c_str());
      }
    }
  }
  else {
    m_error = Error::RANGESERVER_ROW_OVERFLOW;
    HT_THROWF(Error::RANGESERVER_ROW_OVERFLOW,
              "Unable to determine split row for range %s[%s..%s]",
              m_identifier.name, m_start_row.c_str(), m_end_row.c_str());
  }

  /**
   * Create split (transfer) log
   */
  md5_string(m_split_row.c_str(), md5DigestStr);
  md5DigestStr[24] = 0;
  m_state.set_transfer_log(Global::log_dir + "/" + md5DigestStr);

  // Create transfer log dir
  try { Global::log_dfs->mkdirs(m_state.transfer_log); }
  catch (Exception &e) {
    HT_ERRORF("Problem creating log directory '%s': %s",
              m_state.transfer_log, e.what());
    HT_ABORT;
  }

  /***************************************************/
  /** TBD: Write RS_SPLIT_START entry into meta-log **/
  /***************************************************/

  /**
   * Create and install the split log
   */
  {
    RangeUpdateBarrier::ScopedActivator block_updates(m_update_barrier);
    boost::mutex::scoped_lock lock(m_mutex);
    if (!m_scanner_timestamp_controller.get_oldest_update_timestamp(timestampp) ||
        timestampp->logical == 0)
      *timestampp = m_timestamp;
    old_start_row = m_start_row;
    m_split_log_ptr = new CommitLog(Global::dfs, m_state.transfer_log);
  }

}



/**
 *
 */
void Range::split_compact_and_shrink(Timestamp timestamp, String &old_start_row) {
  int error;

  /**
   * Perform major compactions
   */
  {
    for (size_t i=0; i<m_access_group_vector.size(); i++)
      m_access_group_vector[i]->run_compaction(timestamp, true);
  }

  /****************************************************/
  /** TBD: Write RS_SPLIT_SHRUNK entry into meta-log **/
  /****************************************************/

  try {
    String files;
    String metadata_key_str;
    KeySpec key;

    TableMutatorPtr mutator_ptr = Global::metadata_table_ptr->create_mutator();

    /**
     * Shrink old range in METADATA by updating the 'StartRow' column.
     */
    metadata_key_str = std::string("") + (uint32_t)m_identifier.id + ":" + m_end_row;
    key.row = metadata_key_str.c_str();
    key.row_len = metadata_key_str.length();
    key.column_qualifier = 0;
    key.column_qualifier_len = 0;
    key.column_family = "StartRow";
    mutator_ptr->set(key, (uint8_t *)m_split_row.c_str(), m_split_row.length());

    /**
     * Create an entry for the new range
     */
    metadata_key_str = std::string("") + (uint32_t)m_identifier.id + ":" + m_split_row;
    key.row = metadata_key_str.c_str();
    key.row_len = metadata_key_str.length();
    key.column_qualifier = 0;
    key.column_qualifier_len = 0;

    key.column_family = "StartRow";
    mutator_ptr->set(key, (uint8_t *)old_start_row.c_str(), old_start_row.length());

    key.column_family = "Files";
    for (size_t i=0; i<m_access_group_vector.size(); i++) {
      key.column_qualifier = m_access_group_vector[i]->get_name();
      key.column_qualifier_len = strlen(m_access_group_vector[i]->get_name());
      m_access_group_vector[i]->get_files(files);
      mutator_ptr->set(0, key, (uint8_t *)files.c_str(), files.length());
    }

    mutator_ptr->flush();

  }
  catch (Hypertable::Exception &e) {
    // TODO: propagate exception
    HT_ERRORF("Problem updating ROOT METADATA range (new=%s, existing=%s) - %s", m_split_row.c_str(), m_end_row.c_str(), Error::get_text(e.code()));
    // need to unblock updates and then return error
    HT_ABORT;
  }

  /**
   *  Shrink the range
   */

  {
    RangeUpdateBarrier::ScopedActivator block_updates(m_update_barrier);
    boost::mutex::scoped_lock lock(m_mutex);

    // Shrink access groups
    m_start_row = m_split_row;
    m_name = std::string(m_identifier.name) + "[" + m_start_row + ".." + m_end_row + "]";
    m_split_row = "";
    for (size_t i=0; i<m_access_group_vector.size(); i++)
      m_access_group_vector[i]->shrink(m_start_row);

    // Close and uninstall split log
    if ((error = m_split_log_ptr->close()) != Error::OK) {
      HT_ERRORF("Problem closing split log '%s' - %s", m_split_log_ptr->get_log_dir().c_str(), Error::get_text(error));
    }
    m_split_log_ptr = 0;
  }

}


/**
 *
 */
void Range::split_notify_master(String &old_start_row) {
  int error;
  RangeSpec range;

  range.start_row = old_start_row.c_str();
  range.end_row = m_start_row.c_str();

  // update the latest generation, this should probably be protected
  m_identifier.generation = m_schema->get_generation();

  HT_INFOF("Reporting newly split off range %s[%s..%s] to Master", m_identifier.name, range.start_row, range.end_row);

  if (m_state.soft_limit < Global::range_max_bytes) {
    m_state.soft_limit *= 2;
    if (m_state.soft_limit > Global::range_max_bytes)
      m_state.soft_limit = Global::range_max_bytes;
  }

  if ((error = m_master_client_ptr->report_split(&m_identifier, range, m_state.transfer_log, m_state.soft_limit)) != Error::OK) {
    HT_THROWF(error, "Problem reporting split (table=%s, start_row=%s, end_row=%s) to master.",
              m_identifier.name, range.start_row, range.end_row);
  }

}


void Range::compact(bool major) {
  run_compaction(major);
  m_maintenance_in_progress = false;
}


void Range::run_compaction(bool major) {
  Timestamp timestamp;

  {
    RangeUpdateBarrier::ScopedActivator block_updates(m_update_barrier);
    boost::mutex::scoped_lock lock(m_mutex);
    timestamp = m_timestamp;
  }

  /**
   * The following code ensures that pending updates that have not been
   * committed on this range do not get included in the compaction scan
   */
  Timestamp temp_timestamp;
  if (m_scanner_timestamp_controller.get_oldest_update_timestamp(&temp_timestamp) && temp_timestamp < timestamp)
    timestamp = temp_timestamp;

  for (size_t i=0; i<m_access_group_vector.size(); i++)
    m_access_group_vector[i]->run_compaction(timestamp, major);

}


/**
 *
 */
void Range::dump_stats() {
  std::string range_str = (std::string)m_identifier.name + "[" + m_start_row + ".." + m_end_row + "]";
  uint64_t collisions = 0;
  uint64_t cached = 0;
  for (size_t i=0; i<m_access_group_vector.size(); i++) {
    collisions += m_access_group_vector[i]->get_collision_count();
    cached += m_access_group_vector[i]->get_cached_count();
  }
  cout << "STAT\t" << range_str << "\tadded inserts\t" << m_added_inserts << endl;
  cout << "STAT\t" << range_str << "\tadded row deletes\t" << m_added_deletes[0] << endl;
  cout << "STAT\t" << range_str << "\tadded cf deletes\t" << m_added_deletes[1] << endl;
  cout << "STAT\t" << range_str << "\tadded cell deletes\t" << m_added_deletes[2] << endl;
  cout << "STAT\t" << range_str << "\tadded total\t" << (m_added_inserts + m_added_deletes[0] + m_added_deletes[1] + m_added_deletes[2]) << endl;
  cout << "STAT\t" << range_str << "\tcollisions\t" << collisions << endl;
  cout << "STAT\t" << range_str << "\tcached\t" << cached << endl;
  cout << flush;
}


void Range::lock() {
  for (AccessGroupMap::iterator iter = m_access_group_map.begin(); iter != m_access_group_map.end(); iter++)
    (*iter).second->lock();
}


void Range::unlock(uint64_t real_timestamp) {
  // This is a performance optimization to maintain the logical timestamp without excessive locking
  {
    boost::mutex::scoped_lock lock(m_mutex);
    m_timestamp.logical = m_last_logical_timestamp;
    m_timestamp.real = real_timestamp;
  }
  for (AccessGroupMap::iterator iter = m_access_group_map.begin(); iter != m_access_group_map.end(); iter++)
    (*iter).second->unlock();
}



/**
 */
void Range::replay_transfer_log(CommitLogReader *commit_log_reader, uint64_t real_timestamp) {
  BlockCompressionHeaderCommitLog header;
  const uint8_t *base, *ptr, *end;
  size_t len;
  ByteString key, value;
  size_t nblocks = 0;
  size_t count = 0;
  TableIdentifier table_id;
  uint64_t memory_added = 0;

  try {

    while (commit_log_reader->next(&base, &len, &header)) {

      ptr = base;
      end = base + len;

      table_id.decode(&ptr, &len);

      if (strcmp(m_identifier.name, table_id.name))
        HT_THROWF(Error::RANGESERVER_CORRUPT_COMMIT_LOG,
                  "Table name mis-match in split log replay \"%s\" != \"%s\"", m_identifier.name, table_id.name);
      memory_added += len;

      while (ptr < end) {
        key.ptr = ptr;
        ptr += key.length();
        value.ptr = ptr;
        ptr += value.length();
        add(key, value, real_timestamp);
        count++;
      }
      nblocks++;
    }

    {
      boost::mutex::scoped_lock lock(m_mutex);
      HT_INFOF("Replayed %d updates (%d blocks) from split log '%s' into %s[%s..%s]",
               count, nblocks, commit_log_reader->get_log_dir().c_str(),
               m_identifier.name, m_start_row.c_str(), m_end_row.c_str());
    }

    Global::memory_tracker.add_memory(memory_added);
    Global::memory_tracker.add_items(count);

    m_added_inserts = 0;
    memset(m_added_deletes, 0, 3*sizeof(int64_t));

  }
  catch (Hypertable::Exception &e) {
    HT_ERRORF("Problem replaying split log - %s '%s'", Error::get_text(e.code()), e.what());
    throw;
  }
}


/**
 */
uint64_t Range::get_latest_timestamp() {
  boost::mutex::scoped_lock lock(m_mutex);
  return m_timestamp.logical;
}


/**
 */
bool Range::get_scan_timestamp(Timestamp &ts) {
  boost::mutex::scoped_lock lock(m_mutex);
  return m_scanner_timestamp_controller.get_oldest_update_timestamp(&ts);
}


