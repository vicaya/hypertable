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



Range::Range(SchemaPtr &schemaPtr, RangeInfoPtr &rangeInfoPtr) : CellList(), m_mutex(), m_schema(schemaPtr), m_access_group_map(), m_access_group_vector(), m_column_family_vector(), m_maintenance_in_progress(false), m_latest_timestamp(0), m_split_start_time(0), m_split_log_ptr(), m_maintenance_mutex(), m_maintenance_finished_cond(), m_update_quiesce_cond(), m_hold_updates(false), m_update_counter(0) {
  CellStorePtr cellStorePtr;
  std::string accessGroupName;
  AccessGroup *ag;
  uint64_t minLogCutoff = 0;
  uint32_t storeId;
  int32_t len;

  rangeInfoPtr->get_table_name(m_table_name);
  rangeInfoPtr->get_start_row(m_start_row);
  rangeInfoPtr->get_end_row(m_end_row);

  m_column_family_vector.resize( m_schema->get_max_column_family_id() + 1 );

  list<Schema::AccessGroup *> *agList = m_schema->get_access_group_list();

  for (list<Schema::AccessGroup *>::iterator agIter = agList->begin(); agIter != agList->end(); agIter++) {
    ag = new AccessGroup(m_schema, (*agIter), rangeInfoPtr);
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

  /**
   * Replay commit log
   */
  string logDir;
  rangeInfoPtr->get_log_dir(logDir);
  if (logDir != "")
    replay_commit_log(logDir, minLogCutoff);

  return;
}

bool Range::extract_access_group_from_path(std::string &path, std::string &name, uint32_t *storeIdp) {
  const char *base = strstr(path.c_str(), m_table_name.c_str());
  const char *endptr;

  if (base == 0)
    return false;

  base += strlen(m_table_name.c_str());
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

  if (keyComps.columnFamily >= m_column_family_vector.size()) {
    LOG_VA_ERROR("Bad column family (%d)", keyComps.columnFamily);
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
    m_column_family_vector[keyComps.columnFamily]->add(key, value);
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
  if (split_rows.size() > 0)
    return (split_rows[split_rows.size()/2]).c_str();
  return "";
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
    std::string splitPoint;
    std::string splitLogDir;
    char md5DigestStr[33];
    RangeInfoPtr rangeInfoPtr;
    int error;

    splitPoint = get_split_row();

    /**
     * Create Split LOG
     */
    md5_string(splitPoint.c_str(), md5DigestStr);
    md5DigestStr[24] = 0;
    std::string::size_type pos = Global::logDir.rfind("primary", Global::logDir.length());
    assert (pos != std::string::npos);
    splitLogDir = Global::logDir.substr(0, pos) + md5DigestStr;

    // Create split log dir
    string logDir = Global::logDirRoot + splitLogDir;
    if (!FileUtils::mkdirs(logDir.c_str())) {
      LOG_VA_ERROR("Problem creating local log directory '%s'", logDir.c_str());
      exit(1);
    }

    /**
     *  Update METADATA with split information
     */
    if ((error = Global::metadata->get_range_info(m_table_name, m_end_row, rangeInfoPtr)) != Error::OK) {
      LOG_VA_ERROR("Unable to find range (table='%s' endRow='%s') in metadata - %s",
		   m_table_name.c_str(), m_end_row.c_str(), Error::get_text(error));
      exit(1);
    }
    rangeInfoPtr->set_split_point(splitPoint);
    rangeInfoPtr->set_split_log_dir(splitLogDir);
    Global::metadata->sync();


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

      m_split_row = splitPoint;
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
      RangeInfoPtr newRangePtr( new RangeInfo() );
      newRangePtr->set_table_name(m_table_name);
      newRangePtr->set_start_row(m_start_row);
      newRangePtr->set_end_row(splitPoint);
      newRangePtr->set_split_log_dir(splitLogDir);
      rangeInfoPtr->get_tables(stores);
      for (std::vector<std::string>::iterator iter = stores.begin(); iter != stores.end(); iter++)
	newRangePtr->add_cell_store(*iter);
      Global::metadata->add_range_info(newRangePtr);
      Global::metadata->sync();
    }

    /**
     *  Shrink range and remove pending split info from METADATA for existing range
     */
    {
      boost::mutex::scoped_lock lock(m_mutex);      
      m_start_row = splitPoint;
      splitLogDir = "";
      rangeInfoPtr->set_start_row(m_start_row);
      rangeInfoPtr->set_split_log_dir(splitLogDir);
      Global::metadata->sync();
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

      /** block updates **/
      m_hold_updates = true;
      while (m_update_counter > 0)
	m_update_quiesce_cond.wait(lock);

      // at this point, there are no running updates

      // TBD: Do the actual split

      /** unblock updates **/
      m_hold_updates = false;
      m_maintenance_finished_cond.notify_all();
    }

    if ((error = m_split_log_ptr->close(Global::log->get_timestamp())) != Error::OK) {
      LOG_VA_ERROR("Problem closing split log '%s' - %s", m_split_log_ptr->get_log_dir().c_str(), Error::get_text(error));
    }
    m_split_log_ptr = 0;

    /**
     *  TBD:  Notify Master of split
     */
    LOG_VA_INFO("Split Complete.  New Range endRow=%s", splitPoint.c_str());

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
 * This whole thing needs to be optimized.  Instead of having each range load
 * read the entire log file, the following should happen:
 * 1. When a range server dies, the master should do the following:
 *   - Determine the new range assignment
 *   - Sort the commit log of the dead range server
 *   - Ask the new range servers to laod the ranges and provide the
 *     the new range servers with the portions of the logs that are
 *     relevant to the range being loaded.
 *
 * TODO: This method should make sure replayed log updates are successfully commited before inserting
 *       them into the memtables
 *
 * FIX ME!!!!
 */
void Range::replay_commit_log(string &logDir, uint64_t minLogCutoff) {
  CommitLogReader *clogReader = new CommitLogReader(Global::dfs, logDir);
  CommitLogHeaderT *header;  
  string tableName;
  const uint8_t *modPtr, *modEnd, *modBase;
  int32_t keyLen, valueLen;
  string rowkey;
  DynamicBuffer goBuffer(65536);
  uint8_t *goNext = goBuffer.buf;
  uint8_t *goEnd = goBuffer.buf + 65536;
  size_t len;
  size_t count = 0;
  size_t nblocks = 0;
  pair<ByteString32T *, ByteString32T *>  kvPair;
  queue< pair<ByteString32T *, ByteString32T *> >  insertQueue;
  Key keyComps;
  uint64_t cutoffTime;
  
  clogReader->initialize_read(0);

  while (clogReader->next_block(&header)) {
    tableName = (const char *)&header[1];
    nblocks++;

    if (m_table_name == tableName) {
      modPtr = (uint8_t *)&header[1] + strlen((const char *)&header[1]) + 1;
      modEnd = (uint8_t *)header + header->length;
    
      while (modPtr < modEnd) {
	modBase = modPtr;
	if (!keyComps.load((ByteString32T *)modBase)) {
	  LOG_ERROR("Problem deserializing key/value pair from commit log, skipping block...");
	  break;
	}
	memcpy(&keyLen, modPtr, sizeof(int32_t));
	rowkey = keyComps.rowKey;
	modPtr += sizeof(int32_t) + keyLen;
	memcpy(&valueLen, modPtr, sizeof(int32_t));
	modPtr += sizeof(int32_t) + valueLen;

	// TODO: Check for valid column family!!!
	if (keyComps.columnFamily == 0 || keyComps.columnFamily >= m_column_family_vector.size())
	  cutoffTime = minLogCutoff;
	else
	  cutoffTime = m_column_family_vector[keyComps.columnFamily]->get_log_cutoff_time();

	if (rowkey < m_end_row && m_start_row <= rowkey && keyComps.timestamp > cutoffTime) {
	  len = modPtr - modBase;
	  if (len > (size_t)(goEnd-goNext)) {
	    // Commit them to current log
	    if (Global::log->write(m_table_name.c_str(), goBuffer.buf, goNext-goBuffer.buf, header->timestamp) != Error::OK)
	      return;
	    // Replay them into memory
	    while (!insertQueue.empty()) {
	      add(insertQueue.front().first, insertQueue.front().second);
	      insertQueue.pop();
	    }
	    goNext = goBuffer.buf;
	  }
	  assert(len <= (size_t)(goEnd-goNext));
	  memcpy(goNext, modBase, len);
	  kvPair.first = (ByteString32T *)goNext;
	  kvPair.second = (ByteString32T *)(goNext + sizeof(int32_t) + keyLen);
	  goNext += len;
	  insertQueue.push(kvPair);
	  count++;
	}
      }
    }
  }

  if (goNext > goBuffer.buf) {
    if (Global::log->write(m_table_name.c_str(), goBuffer.buf, goNext-goBuffer.buf, header->timestamp) != Error::OK)
      return;
    // Replay them into memory
    while (!insertQueue.empty()) {
      add(insertQueue.front().first, insertQueue.front().second);
      insertQueue.pop();
    }
  }

  LOG_VA_INFO("LOAD RANGE replayed %d updates (%d blocks) from commit log '%s'", count, nblocks, logDir.c_str());

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

