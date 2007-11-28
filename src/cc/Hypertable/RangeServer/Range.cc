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



Range::Range(MasterClientPtr &master_client_ptr, TableIdentifierT &identifier, SchemaPtr &schemaPtr, RangeInfoPtr &rangeInfoPtr) : CellList(), m_mutex(), m_master_client_ptr(master_client_ptr), m_schema(schemaPtr), m_maintenance_in_progress(false), m_latest_timestamp(0), m_split_start_time(0), m_hold_updates(false), m_update_counter(0) {
  CellStorePtr cellStorePtr;
  std::string accessGroupName;
  AccessGroup *ag;
  uint64_t minLogCutoff = 0;
  uint32_t storeId;
  int32_t len;

  Copy(identifier, m_identifier);

  rangeInfoPtr->get_start_row(m_start_row);
  rangeInfoPtr->get_end_row(m_end_row);

  m_column_family_vector.resize( m_schema->get_max_column_family_id() + 1 );

  list<Schema::AccessGroup *> *agList = m_schema->get_access_group_list();

  for (list<Schema::AccessGroup *>::iterator agIter = agList->begin(); agIter != agList->end(); agIter++) {
    ag = new AccessGroup(m_identifier, m_schema, (*agIter), rangeInfoPtr);
    m_access_group_map[(*agIter)->name] = ag;
    m_access_group_vector.push_back(ag);
    for (list<Schema::ColumnFamily *>::iterator cfIter = (*agIter)->columns.begin(); cfIter != (*agIter)->columns.end(); cfIter++)
      m_column_family_vector[(*cfIter)->id] = ag;
  }

  /**
   * Load CellStores
   */
  vector<string> cellStoreVector;
  rangeInfoPtr->get_tables(cellStoreVector);
  for (size_t i=0; i<cellStoreVector.size(); i++) {
    cellStorePtr = new CellStoreV0(Global::dfs);
    if (!extract_access_group_from_path(cellStoreVector[i], accessGroupName, &storeId)) {
      LOG_VA_ERROR("Unable to extract locality group name from path '%s'", cellStoreVector[i].c_str());
      continue;
    }
    if (cellStorePtr->open(cellStoreVector[i].c_str(), m_start_row.c_str(), m_end_row.c_str()) != Error::OK)
      continue;
    if (cellStorePtr->load_index() != Error::OK)
      continue;
    ag = m_access_group_map[accessGroupName];
    if (ag == 0) {
      LOG_VA_ERROR("Unrecognized locality group name '%s' in path '%s'", accessGroupName.c_str(), cellStoreVector[i].c_str());
      continue;
    }
    if (minLogCutoff == 0 || cellStorePtr->get_log_cutoff_time() < minLogCutoff)
      minLogCutoff = cellStorePtr->get_log_cutoff_time();
    ag->add_cell_store(cellStorePtr, storeId);
    //cout << "Just added " << cellStorePtrVector[i].c_str() << endl;
  }

  return;
}


/**
 */
Range::~Range() {
  Free(m_identifier);
}


/**
 */
bool Range::extract_access_group_from_path(std::string &path, std::string &name, uint32_t *storeIdp) {
  const char *base = strstr(path.c_str(), m_identifier.name);
  const char *endptr;

  if (base == 0)
    return false;

  base += strlen(m_identifier.name);
  if (*base++ != '/')
    return false;
  endptr = strchr(base, '/');
  if (endptr == 0)
    return false;
  name = string(base, endptr-base);

  if ((base = strrchr(path.c_str(), '/')) == 0 || strncmp(base, "/cs", 3))
    *storeIdp = 0;
  else
    *storeIdp = atoi(base+3);
  
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

  if (keyComps.timestamp > m_latest_timestamp)
    m_latest_timestamp = keyComps.timestamp;

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
    m_access_group_vector[i]->get_split_rows(split_rows);
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

  if (m_maintenance_in_progress)
    return;

  // Need split?
  if (disk_usage() > Global::rangeMaxBytes) {
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
  if (disk_usage() > Global::rangeMaxBytes) {
    std::string splitLogDir;
    char md5DigestStr[33];
    RangeInfoPtr rangeInfoPtr;
    int error;
    std::string old_start_row;

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
    if ((error = Global::metadata->get_range_info(m_identifier.name, m_end_row.c_str(), rangeInfoPtr)) != Error::OK) {
      LOG_VA_ERROR("Unable to find range (table='%s' endRow='%s') in metadata - %s",
		   m_identifier.name, m_end_row.c_str(), Error::get_text(error));
      exit(1);
    }
    rangeInfoPtr->set_split_point(m_split_row);
    rangeInfoPtr->set_split_log_dir(splitLogDir);
    Global::metadata->sync();

#ifdef METADATA_TEST
    if (Global::metadata_range_server) {
      DynamicBuffer send_buf(0);
      std::string row_key = std::string("") + (uint32_t)m_identifier.id + ":" + m_end_row;
      CreateKeyAndAppend(send_buf, FLAG_INSERT, row_key.c_str(), Global::metadata_column_family_SplitPoint, 0, Global::log->get_timestamp());
      CreateAndAppend(send_buf, m_split_row.c_str());
      CreateKeyAndAppend(send_buf, FLAG_INSERT, row_key.c_str(), Global::metadata_column_family_SplitLogDir, 0, Global::log->get_timestamp());
      CreateAndAppend(send_buf, splitLogDir.c_str());
      if ((error = Global::metadata_range_server->update(Global::local_addr, Global::metadata_identifier, send_buf.buf, send_buf.fill())) != Error::OK) {
	LOG_VA_ERROR("Problem updating METADATA LogDir: column - %s", Error::get_text(error));
	DUMP_CORE;
      }
      send_buf.release();
    }
#endif

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
	m_split_start_time = m_latest_timestamp;
      }

      m_split_log_ptr = new CommitLog(Global::dfs, splitLogDir, 0x100000000LL);

      /** unblock updates **/
      m_hold_updates = false;
      m_maintenance_finished_cond.notify_all();
    }

    /**
     * Perform major compaction
     */
    for (size_t i=0; i<m_access_group_vector.size(); i++) {
      m_access_group_vector[i]->run_compaction(m_split_start_time, true);  // verify the timestamp
    }

    /**
     * Create METADATA entry for new range
     */
    {
      std::vector<std::string> stores;
      std::string table_name = m_identifier.name;
      RangeInfoPtr newRangePtr( new RangeInfo() );
      newRangePtr->set_table_name(table_name);
      newRangePtr->set_start_row(m_start_row);
      newRangePtr->set_end_row(m_split_row);
      newRangePtr->set_split_log_dir(splitLogDir);
      rangeInfoPtr->get_tables(stores);
      for (std::vector<std::string>::iterator iter = stores.begin(); iter != stores.end(); iter++)
	newRangePtr->add_cell_store(*iter);
      Global::metadata->add_range_info(newRangePtr);
      Global::metadata->sync();

#ifdef METADATA_TEST
      if (Global::metadata_range_server) {
	DynamicBuffer send_buf(0);
	std::string row_key = std::string("") + (uint32_t)m_identifier.id + ":" + m_split_row;
	std::string files = "";
	CreateKeyAndAppend(send_buf, FLAG_INSERT, row_key.c_str(), Global::metadata_column_family_StartRow, 0, Global::log->get_timestamp());
	CreateAndAppend(send_buf, m_start_row.c_str());
	CreateKeyAndAppend(send_buf, FLAG_INSERT, row_key.c_str(), Global::metadata_column_family_SplitLogDir, 0, Global::log->get_timestamp());
	CreateAndAppend(send_buf, splitLogDir.c_str());
	for (std::vector<std::string>::iterator iter = stores.begin(); iter != stores.end(); iter++)
	  files += *iter + "\n";
	CreateKeyAndAppend(send_buf, FLAG_INSERT, row_key.c_str(), Global::metadata_column_family_Files, 0, Global::log->get_timestamp());
	CreateAndAppend(send_buf, files.c_str());
	if ((error = Global::metadata_range_server->update(Global::local_addr, Global::metadata_identifier, send_buf.buf, send_buf.fill())) != Error::OK) {
	  LOG_VA_ERROR("Problem updating METADATA LogDir: column - %s", Error::get_text(error));
	  DUMP_CORE;
	}
	send_buf.release();
      }
#endif

    }

    /**
     *  Shrink range and remove pending split info from METADATA for existing range
     */
    {
      boost::mutex::scoped_lock lock(m_mutex);
      old_start_row = m_start_row;
      m_start_row = m_split_row;
      splitLogDir = "";
      rangeInfoPtr->set_start_row(m_start_row);
      rangeInfoPtr->set_split_log_dir(splitLogDir);
      Global::metadata->sync();

#ifdef METADATA_TEST
      if (Global::metadata_range_server) {
	DynamicBuffer send_buf(0);
	std::string row_key = std::string("") + (uint32_t)m_identifier.id + ":" + m_end_row;
	CreateKeyAndAppend(send_buf, FLAG_INSERT, row_key.c_str(), Global::metadata_column_family_StartRow, 0, Global::log->get_timestamp());
	CreateAndAppend(send_buf, m_start_row.c_str());
	CreateKeyAndAppend(send_buf, FLAG_INSERT, row_key.c_str(), Global::metadata_column_family_SplitLogDir, 0, Global::log->get_timestamp());
	CreateAndAppend(send_buf, "");
	if ((error = Global::metadata_range_server->update(Global::local_addr, Global::metadata_identifier, send_buf.buf, send_buf.fill())) != Error::OK) {
	  LOG_VA_ERROR("Problem updating METADATA LogDir: column - %s", Error::get_text(error));
	  DUMP_CORE;
	}
	send_buf.release();
      }
#endif

    }

    /**
     *  Do the split
     */
    {
      boost::mutex::scoped_lock lock(m_maintenance_mutex);

      {
	boost::mutex::scoped_lock lock(m_mutex);
	m_split_start_time = 0;
	m_split_row = "";
      }

      // block updates
      m_hold_updates = true;
      while (m_update_counter > 0)
	m_update_quiesce_cond.wait(lock);

      /*** At this point, there are no running updates ***/

      // Shrink this range's access groups
      for (size_t i=0; i<m_access_group_vector.size(); i++)
	m_access_group_vector[i]->shrink(m_start_row);

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

      LOG_INFO("About to report split");
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

  for (size_t i=0; i<m_access_group_vector.size(); i++)
    m_access_group_vector[i]->run_compaction(timestamp, major);

  return timestamp;
}



void Range::lock() {
  for (AccessGroupMapT::iterator iter = m_access_group_map.begin(); iter != m_access_group_map.end(); iter++)
    (*iter).second->lock();
}


void Range::unlock() {
  for (AccessGroupMapT::iterator iter = m_access_group_map.begin(); iter != m_access_group_map.end(); iter++)
    (*iter).second->unlock();
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

  LOG_VA_INFO("Replayed %d updates (%d blocks) from split log '%s'", count, nblocks, log_dir.c_str());

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

