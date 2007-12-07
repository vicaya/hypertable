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

#include "CellStoreV0.h"
#include "CommitLog.h"
#include "CommitLogReader.h"
#include "Global.h"
#include "MaintenanceThread.h"
#include "MergeScanner.h"
#include "Range.h"

using namespace Hypertable;
using namespace std;



Range::Range(MasterClientPtr &master_client_ptr, TableIdentifierT &identifier, SchemaPtr &schemaPtr, RangeT *range) : CellList(), m_mutex(), m_master_client_ptr(master_client_ptr), m_schema(schemaPtr), m_maintenance_in_progress(false), m_latest_timestamp(0), m_split_start_time(0), m_hold_updates(false), m_update_counter(0) {
  int error;
  AccessGroup *ag;

  Copy(identifier, m_identifier);

  m_start_row = range->startRow;
  m_end_row = range->endRow;

  m_is_root = (m_identifier.id == 0 && *range->startRow == 0 && !strcmp(range->endRow, Key::END_ROOT_ROW));

  m_column_family_vector.resize( m_schema->get_max_column_family_id() + 1 );

  list<Schema::AccessGroup *> *agList = m_schema->get_access_group_list();

  for (list<Schema::AccessGroup *>::iterator agIter = agList->begin(); agIter != agList->end(); agIter++) {
    ag = new AccessGroup(m_identifier, m_schema, (*agIter), range);
    m_access_group_map[(*agIter)->name] = ag;
    m_access_group_vector.push_back(ag);
    for (list<Schema::ColumnFamily *>::iterator cfIter = (*agIter)->columns.begin(); cfIter != (*agIter)->columns.end(); cfIter++)
      m_column_family_vector[(*cfIter)->id] = ag;
  }

  /**
   * Read the cell store files from METADATA and 
   */
  if (!m_is_root) {
    if ((error = load_cell_stores()) != Error::OK) {
      // TODO: throw exception
    }
  }
  else {
    // TODO: read from hyperspace
  }

  return;
}


/**
 */
Range::~Range() {
  Free(m_identifier);
}



/**
 *
 */
int Range::load_cell_stores() {
  AccessGroup *ag;
  CellStorePtr cellStorePtr;
  CellT cell;
  KeySpec key;
  ScanSpecificationT scan_spec;
  TableScannerPtr scanner_ptr;
  const char *base, *ptr, *end;
  int error;
  int32_t len;
  std::string ag_name;
  std::string file_str;
  uint32_t csid;
  uint64_t minLogCutoff = 0;
  std::vector<std::string> csvec;
  std::string metadata_key = std::string("") + (uint32_t)m_identifier.id + ":" + m_end_row;

  scan_spec.rowLimit = 1;
  scan_spec.max_versions = 1;
  scan_spec.startRow = metadata_key.c_str();
  scan_spec.startRowInclusive = true;
  scan_spec.endRow = metadata_key.c_str();
  scan_spec.endRowInclusive = true;
  scan_spec.interval.first = 0;
  scan_spec.interval.second = 0;
  scan_spec.columns.clear();
  scan_spec.columns.push_back("Files");

  if ((error = Global::metadata_table_ptr->create_scanner(scan_spec, scanner_ptr)) != Error::OK) {
    LOG_ERROR("Problem creating scanner on METADATA table");
    return error;
  }

  try {

    while (scanner_ptr->next(cell)) {

      if (strcmp(cell.column_family, "Files")) {
	// should never happen
	LOG_VA_ERROR("Scanner requested column 'Files' but got '%s'", cell.column_family);
	LOG_VA_ERROR("value = %s", std::string((const char *)cell.value, cell.value_len).c_str());
	return Error::INVALID_METADATA;
      }

      ag_name = std::string(cell.column_qualifier);

      ag = m_access_group_map[ag_name];
      if (ag == 0) {
	LOG_VA_ERROR("Unrecognized access group name '%s' found in METADATA for table '%s'", ag_name.c_str(), m_identifier.name);
	continue;
      }

      csvec.clear();
      ptr = base = (const char *)cell.value;
      end = (const char *)cell.value + cell.value_len;
      while (ptr < end) {

	while (*ptr != ';' && ptr < end)
	  ptr++;

	file_str = std::string(base, ptr-base);
	boost::trim(file_str);

	if (file_str != "")
	  csvec.push_back(file_str);

	++ptr;
	base = ptr;
      }

      for (size_t i=0; i<csvec.size(); i++) {

	cellStorePtr = new CellStoreV0(Global::dfs);

	if (!extract_csid_from_path(csvec[i], &csid)) {
	  LOG_VA_ERROR("Unable to extract cell store ID from path '%s'", csvec[i].c_str());
	  continue;
	}
	if ((error = cellStorePtr->open(csvec[i].c_str(), m_start_row.c_str(), m_end_row.c_str())) != Error::OK) {
	  // this should throw an exception
	  LOG_VA_ERROR("Problem opening cell store '%s', skipping...", csvec[i].c_str());
	  continue;
	}
	if ((error = cellStorePtr->load_index()) != Error::OK) {
	  // this should throw an exception
	  LOG_VA_ERROR("Problem loading index of cell store '%s', skipping...", csvec[i].c_str());
	  continue;
	}

	// TODO: maybe do something here?
	if (minLogCutoff == 0 || cellStorePtr->get_log_cutoff_time() < minLogCutoff)
	  minLogCutoff = cellStorePtr->get_log_cutoff_time();

	ag->add_cell_store(cellStorePtr, csid);
      }
    }

  }
  catch (Hypertable::Exception &e) {
    LOG_VA_ERROR("%s", e.what());
    return e.code();
  }

  return Error::OK;
}



/**
 */
bool Range::extract_csid_from_path(std::string &path, uint32_t *csidp) {
  const char *base;
  const char *endptr;

  if ((base = strrchr(path.c_str(), '/')) == 0 || strncmp(base, "/cs", 3))
    *csidp = 0;
  else
    *csidp = atoi(base+3);
  
  return true;
}


/**
 * TODO: Make this more robust
 */
int Range::add(const ByteString32T *key, const ByteString32T *value) {
  Key keyComps;

  if (!keyComps.load(key)) {
    LOG_ERROR("Problem parsing key!!");
    return 0;
  }

  if (keyComps.column_family_code >= m_column_family_vector.size()) {
    LOG_VA_ERROR("Bad column family (%d)", keyComps.column_family_code);
    return 0;
  }

  if (keyComps.timestamp > m_temp_timestamp)
    m_temp_timestamp = keyComps.timestamp;

  if (keyComps.flag == FLAG_DELETE_ROW) {
    for (AccessGroupMapT::iterator iter = m_access_group_map.begin(); iter != m_access_group_map.end(); iter++) {
      (*iter).second->add(key, value);
    }
  }
  else if (keyComps.flag == FLAG_DELETE_CELL || keyComps.flag == FLAG_INSERT) {
    m_column_family_vector[keyComps.column_family_code]->add(key, value);
  }
  else {
    LOG_VA_ERROR("Bad flag value (%d)", keyComps.flag);
  }
  return 0;
}


CellListScanner *Range::create_scanner(ScanContextPtr &scanContextPtr) {
  MergeScanner *mscanner = new MergeScanner(scanContextPtr, false);
  for (AccessGroupMapT::iterator iter = m_access_group_map.begin(); iter != m_access_group_map.end(); iter++) {
    if ((*iter).second->include_in_scan(scanContextPtr))
      mscanner->add_scanner((*iter).second->create_scanner(scanContextPtr));
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
const char *Range::get_split_row() {
  std::vector<std::string> split_rows;
  for (size_t i=0; i<m_access_group_vector.size(); i++)
    m_access_group_vector[i]->get_split_rows(split_rows, false);
  if (split_rows.size() == 0) {
    for (size_t i=0; i<m_access_group_vector.size(); i++)
      m_access_group_vector[i]->get_split_rows(split_rows, true);
  }
  sort(split_rows.begin(), split_rows.end());
  /**
  for (size_t i=0; i<split_rows.size(); i++)
    cout << "Range::get_split_row [" << i << "] = " << split_rows[i] << endl;
  */
  if (split_rows.size() > 0) {
    boost::mutex::scoped_lock lock(m_mutex);    
    m_split_row = split_rows[split_rows.size()/2];
  }
  return m_split_row.c_str();
}


uint64_t Range::get_log_cutoff_time() {
  uint64_t cutoffTime = m_access_group_vector[0]->get_log_cutoff_time();
  for (size_t i=1; i<m_access_group_vector.size(); i++) {
    if (m_access_group_vector[i]->get_log_cutoff_time() < cutoffTime)
      cutoffTime = m_access_group_vector[i]->get_log_cutoff_time();
  }
  return cutoffTime;
}


/**
 * 
 */
void Range::schedule_maintenance() {
  boost::mutex::scoped_lock lock(m_mutex);
  uint64_t disk_used = disk_usage();

  if (m_maintenance_in_progress)
    return;

  // Need split?
  if (disk_used > Global::rangeMaxBytes || 
      (Global::range_metadata_max_bytes && m_identifier.id == 0 && disk_used > Global::range_metadata_max_bytes)) {
    m_maintenance_in_progress = true;
    MaintenanceThread::schedule_maintenance(this);
  }
  else {
    // Need compaction?
    for (size_t i=0; i<m_access_group_vector.size(); i++) {
      if (m_access_group_vector[i]->needs_compaction()) {
	m_maintenance_in_progress = true;
	MaintenanceThread::schedule_maintenance(this);
	break;
      }
    }
  }
}


void Range::do_maintenance() {
  assert(m_maintenance_in_progress);
  uint64_t disk_used = disk_usage();

  if (disk_used > Global::rangeMaxBytes ||
      (Global::range_metadata_max_bytes && m_identifier.id == 0 && disk_used > Global::range_metadata_max_bytes)) {
    std::string splitLogDir;
    char md5DigestStr[33];
    int error;
    std::string old_start_row;
    MutatorPtr mutator_ptr;
    KeySpec key;
    std::string metadata_key_str;

    // this call sets m_split_row
    get_split_row();

    /**
     * Create Split LOG
     */
    md5_string(m_split_row.c_str(), md5DigestStr);
    md5DigestStr[24] = 0;
    std::string::size_type pos = Global::logDir.rfind("primary", Global::logDir.length());
    assert (pos != std::string::npos);
    splitLogDir = Global::logDir.substr(0, pos) + md5DigestStr;

    // Create split log dir
    if ((error = Global::logDfs->mkdirs(splitLogDir)) != Error::OK) {
      LOG_VA_ERROR("Problem creating DFS log directory '%s'", splitLogDir.c_str());
      exit(1);
    }

    /**
     *  Update METADATA with split information
     */
    if ((error = Global::metadata_table_ptr->create_mutator(mutator_ptr)) != Error::OK) {
      // TODO: throw exception
      LOG_ERROR("Problem creating mutator on METADATA table");
      return;
    }

    metadata_key_str = std::string("") + (uint32_t)m_identifier.id + ":" + m_end_row;
    key.row = metadata_key_str.c_str();
    key.row_len = metadata_key_str.length();
    key.column_qualifier = 0;
    key.column_qualifier_len = 0;

    try {
      key.column_family = "SplitPoint";
      mutator_ptr->set(0, key, (uint8_t *)m_split_row.c_str(), m_split_row.length());
      key.column_family = "SplitLogDir";
      mutator_ptr->set(0, key, (uint8_t *)splitLogDir.c_str(), splitLogDir.length());
      mutator_ptr->flush();
    }
    catch (Hypertable::Exception &e) {
      // TODO: propagate exception
      LOG_VA_ERROR("Problem updating METADATA with split info (row key = %s) - %s", metadata_key_str.c_str(), e.what());
      return;
    }

    /**
     * Atomically obtain timestamp and install split log
     */
    {
      boost::mutex::scoped_lock lock(m_maintenance_mutex);

      /** block updates **/
      m_hold_updates = true;
      while (m_update_counter > 0)
	m_update_quiesce_cond.wait(lock);

      {
	boost::mutex::scoped_lock lock(m_mutex);
	m_split_start_time = m_scanner_timestamp_controller.get_oldest_update_timestamp();
	if (m_split_start_time == 0 || m_latest_timestamp < m_split_start_time)
	  m_split_start_time = m_latest_timestamp;
	old_start_row = m_start_row;
      }

      m_split_log_ptr = new CommitLog(Global::dfs, splitLogDir, 0x100000000LL);

      /** unblock updates **/
      m_hold_updates = false;
      m_maintenance_finished_cond.notify_all();
    }


    /**
     * Perform major compactions
     */
    for (size_t i=0; i<m_access_group_vector.size(); i++)
      m_access_group_vector[i]->run_compaction(m_split_start_time, true);  // verify the timestamp


    /**
     * Create second-level METADATA entry for new range and update second-level
     * METADATA entry for existing range to reflect the shrink 
     */
    try {
      std::string files;
      metadata_key_str = std::string("") + (uint32_t)m_identifier.id + ":" + m_split_row;
      key.row = metadata_key_str.c_str();
      key.row_len = metadata_key_str.length();
      key.column_qualifier = 0;
      key.column_qualifier_len = 0;

      key.column_family = "StartRow";
      mutator_ptr->set(0, key, (uint8_t *)old_start_row.c_str(), old_start_row.length());
      key.column_family = "SplitLogDir";
      mutator_ptr->set(0, key, (uint8_t *)splitLogDir.c_str(), splitLogDir.length());

      key.column_family = "Files";
      for (size_t i=0; i<m_access_group_vector.size(); i++) {
	key.column_qualifier = m_access_group_vector[i]->get_name();
	key.column_qualifier_len = strlen(m_access_group_vector[i]->get_name());
	m_access_group_vector[i]->get_files(files);
	mutator_ptr->set(0, key, (uint8_t *)files.c_str(), files.length());
      }

      metadata_key_str = std::string("") + (uint32_t)m_identifier.id + ":" + m_end_row;
      key.row = metadata_key_str.c_str();
      key.row_len = metadata_key_str.length();
      key.column_qualifier = 0;
      key.column_qualifier_len = 0;

      key.column_family = "StartRow";
      mutator_ptr->set(0, key, (uint8_t *)m_split_row.c_str(), m_split_row.length());
      key.column_family = "SplitLogDir";
      mutator_ptr->set(0, key, 0, 0);

      mutator_ptr->flush();
    }
    catch (Hypertable::Exception &e) {
      // TODO: propagate exception
      LOG_VA_ERROR("Problem updating METADATA with new range information (row key = %s) - %s", metadata_key_str.c_str(), e.what());
      DUMP_CORE;
    }

    /**
     * If this is a METADATA range, then update the ROOT range
     */
    if (m_identifier.id == 0) {
      try {
	// new range
	metadata_key_str = std::string("0:") + m_split_row;
	key.row = metadata_key_str.c_str();
	key.row_len = metadata_key_str.length();
	key.column_qualifier = 0;
	key.column_qualifier_len = 0;
	key.column_family = "StartRow";
	mutator_ptr->set(0, key, (uint8_t *)old_start_row.c_str(), old_start_row.length());

	// existing range
	metadata_key_str = std::string("0:") + m_end_row;
	key.row = metadata_key_str.c_str();
	key.row_len = metadata_key_str.length();
	mutator_ptr->set(0, key, (uint8_t *)m_split_row.c_str(), m_split_row.length());
      }
      catch (Hypertable::Exception &e) {
	// TODO: propagate exception
	LOG_VA_ERROR("Problem updating ROOT METADATA range (new=%s, existing=%s) - %s", m_split_row.c_str(), m_end_row.c_str(), e.what());
	DUMP_CORE;
      }
    }

    /**
     *  Do the split
     */
    {
      boost::mutex::scoped_lock lock(m_maintenance_mutex);

      // block updates
      m_hold_updates = true;
      while (m_update_counter > 0)
	m_update_quiesce_cond.wait(lock);

      /*** At this point, there are no running updates ***/

      {
	boost::mutex::scoped_lock lock(m_mutex);
	m_start_row = m_split_row;
	m_split_start_time = 0;
	m_split_row = "";
	// Shrink this range's access groups
	for (size_t i=0; i<m_access_group_vector.size(); i++)
	  m_access_group_vector[i]->shrink(m_start_row);
      }

      // unblock updates
      m_hold_updates = false;
      m_maintenance_finished_cond.notify_all();
    }

    // close split log
    if ((error = m_split_log_ptr->close(Global::log->get_timestamp())) != Error::OK) {
      LOG_VA_ERROR("Problem closing split log '%s' - %s", m_split_log_ptr->get_log_dir().c_str(), Error::get_text(error));
    }
    m_split_log_ptr = 0;

    /**
     *  Notify Master of split
     */
    {
      RangeT range;

      range.startRow = old_start_row.c_str();
      range.endRow = m_start_row.c_str();

      // update the latest generation, this should probably be protected
      m_identifier.generation = m_schema->get_generation();

      LOG_VA_INFO("Reporting newly split off range %s[%s..%s] to Master", m_identifier.name, range.startRow, range.endRow);
      cout << flush;
      if ((error = m_master_client_ptr->report_split(m_identifier, range)) != Error::OK) {
	LOG_VA_ERROR("Problem reporting split (table=%s, start_row=%s, end_row=%s) to master.",
		     m_identifier.name, range.startRow, range.endRow);
      }
    }

    LOG_VA_INFO("Split Complete.  New Range startRow=%s", m_start_row.c_str());

  }
  else
    run_compaction(false);

  m_maintenance_in_progress = false;
}


void Range::do_compaction(bool major) {
  run_compaction(major);
  m_maintenance_in_progress = false;
}


uint64_t Range::run_compaction(bool major) {
  uint64_t timestamp;
  
  {
    boost::mutex::scoped_lock lock(m_maintenance_mutex);

    /** block updates **/
    m_hold_updates = true;
    while (m_update_counter > 0)
      m_update_quiesce_cond.wait(lock);

    {
      boost::mutex::scoped_lock lock(m_mutex);
      timestamp = m_latest_timestamp;
    }

    /** unblock updates **/
    m_hold_updates = false;
    m_maintenance_finished_cond.notify_all();
  }

  /**
   * The following code ensures that pending updates that have not been
   * committed on this range do not get included in the compaction
   * scan
   */
  int64_t temp_timestamp = m_scanner_timestamp_controller.get_oldest_update_timestamp();
  if (temp_timestamp != 0 && temp_timestamp < timestamp)
    timestamp = temp_timestamp;

  for (size_t i=0; i<m_access_group_vector.size(); i++)
    m_access_group_vector[i]->run_compaction(timestamp, major);

  return timestamp;
}



void Range::lock() {
  // this is a bit of a hack to collect the most recent timestamp seen
  {
    boost::mutex::scoped_lock lock(m_mutex);
    m_temp_timestamp = m_latest_timestamp;
  }
  for (AccessGroupMapT::iterator iter = m_access_group_map.begin(); iter != m_access_group_map.end(); iter++)
    (*iter).second->lock();
}


void Range::unlock() {
  for (AccessGroupMapT::iterator iter = m_access_group_map.begin(); iter != m_access_group_map.end(); iter++)
    (*iter).second->unlock();
  // this is a bit of a hack to collect the most recent timestamp seen
  {
    boost::mutex::scoped_lock lock(m_mutex);
    m_latest_timestamp = m_temp_timestamp;
  }
}



/**
 */
int Range::replay_split_log(string &log_dir) {
  int error;
  CommitLogReaderPtr commit_log_reader_ptr = new CommitLogReader(Global::dfs, log_dir);
  CommitLogHeaderT *header;  
  const uint8_t *base, *ptr, *end;
  ByteString32T *key, *value;
  size_t nblocks = 0;
  size_t count = 0;
  
  commit_log_reader_ptr->initialize_read(0);

  while (commit_log_reader_ptr->next_block(&header)) {

    assert(!strcmp(m_identifier.name, (const char *)&header[1]));
    assert(header->flags == 0);

    ptr = (uint8_t *)&header[1] + strlen((const char *)&header[1]) + 1;
    end = (uint8_t *)header + header->length;

    while (ptr < end) {
      key = (ByteString32T *)ptr;
      ptr += Length(key);
      value = (ByteString32T *)ptr;
      ptr += Length(value);
      add(key, value);
      count++;
    }
    nblocks++;
  }

  {
    boost::mutex::scoped_lock lock(m_mutex);
    LOG_VA_INFO("Replayed %d updates (%d blocks) from split log '%s' into %s[%s..%s]",
		count, nblocks, log_dir.c_str(), m_identifier.name, m_start_row.c_str(), m_end_row.c_str());
  }

  error = commit_log_reader_ptr->last_error();

  return Error::OK;
}


/**
 *
 */
void Range::increment_update_counter() {
  boost::mutex::scoped_lock lock(m_maintenance_mutex);
  while (m_hold_updates)
    m_maintenance_finished_cond.wait(lock);
  m_update_counter++;
}


/**
 *
 */
void Range::decrement_update_counter() {
  boost::mutex::scoped_lock lock(m_maintenance_mutex);
  m_update_counter--;
  if (m_hold_updates && m_update_counter == 0)
    m_update_quiesce_cond.notify_one();
}


/**
 * 
 */
uint64_t Range::get_timestamp() {
  boost::mutex::scoped_lock lock(m_mutex);
  uint64_t timestamp = m_scanner_timestamp_controller.get_oldest_update_timestamp();
  if (timestamp != 0 && timestamp <= m_latest_timestamp)
    return timestamp;
  
  if (m_latest_timestamp == 0) {
    boost::xtime now;
    boost::xtime_get(&now, boost::TIME_UTC);
    timestamp = ((uint64_t)now.sec * 1000000000LL) + (uint64_t)now.nsec;
    return timestamp;
  }
  return m_latest_timestamp + 1;
}
