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

#include <algorithm>
#include <iterator>
#include <vector>

#include "Common/Error.h"
#include "Common/md5.h"

#include "AccessGroup.h"
#include "CellCache.h"
#include "CellCacheScanner.h"
#include "CellStoreV0.h"
#include "Global.h"
#include "MergeScanner.h"


AccessGroup::AccessGroup(TableIdentifierT &table_identifier, SchemaPtr &schemaPtr, Schema::AccessGroup *ag, RangeT *range) : CellList(), m_mutex(), m_lock(m_mutex,false), m_schema_ptr(schemaPtr), m_name(ag->name), m_stores(), m_cell_cache_ptr(), m_next_table_id(0), m_log_cutoff_time(0), m_disk_usage(0) {
  m_table_name = table_identifier.name;
  m_start_row = range->startRow;
  m_end_row = range->endRow;
  m_cell_cache_ptr = new CellCache();
  for (list<Schema::ColumnFamily *>::iterator iter = ag->columns.begin(); iter != ag->columns.end(); iter++) {
    m_column_families.insert((uint8_t)(*iter)->id);
  }
  Copy(table_identifier, m_table_identifier);
}


AccessGroup::~AccessGroup() {
  Free(m_table_identifier);
  return;
}

/**
 * This should be called with the CellCache locked
 * Also, at the end of compaction processing, when m_cell_cache_ptr gets reset to a new value,
 * the CellCache should be locked as well.
 */
int AccessGroup::add(const ByteString32T *key, const ByteString32T *value) {
  return m_cell_cache_ptr->add(key, value);
}


CellListScanner *AccessGroup::create_scanner(ScanContextPtr &scanContextPtr) {
  boost::mutex::scoped_lock lock(m_mutex);
  MergeScanner *scanner = new MergeScanner(scanContextPtr);
  scanner->add_scanner( m_cell_cache_ptr->create_scanner(scanContextPtr) );
  for (size_t i=0; i<m_stores.size(); i++)
    scanner->add_scanner( m_stores[i]->create_scanner(scanContextPtr) );
  return scanner;
}

bool AccessGroup::include_in_scan(ScanContextPtr &scanContextPtr) {
  boost::mutex::scoped_lock lock(m_mutex);
  for (std::set<uint8_t>::iterator iter = m_column_families.begin(); iter != m_column_families.end(); iter++) {
    if (scanContextPtr->familyMask[*iter])
      return true;
  }
  return false;
}

const char *AccessGroup::get_split_row() {
  std::vector<std::string> split_rows;
  get_split_rows(split_rows);
  if (split_rows.size() > 0) {
    sort(split_rows.begin(), split_rows.end());
    return (split_rows[split_rows.size()/2]).c_str();
  }
  return "";
}

void AccessGroup::get_split_rows(std::vector<std::string> &split_rows) {
  boost::mutex::scoped_lock lock(m_mutex);
  for (size_t i=0; i<m_stores.size(); i++)
    split_rows.push_back(m_stores[i]->get_split_row());
}

uint64_t AccessGroup::disk_usage() {
  boost::mutex::scoped_lock lock(m_mutex);
  return m_disk_usage + m_cell_cache_ptr->memory_used();
}

bool AccessGroup::needs_compaction() {
  boost::mutex::scoped_lock lock(m_mutex);
  if (m_cell_cache_ptr->memory_used() >= (uint32_t)Global::localityGroupMaxMemory)
    return true;
  return false;
}


void AccessGroup::add_cell_store(CellStorePtr &cellStorePtr, uint32_t id) {
  boost::mutex::scoped_lock lock(m_mutex);
  if (id >= m_next_table_id) 
    m_next_table_id = id+1;
  if (cellStorePtr->get_log_cutoff_time() > m_log_cutoff_time)
    m_log_cutoff_time = cellStorePtr->get_log_cutoff_time();
  m_stores.push_back(cellStorePtr);
  m_disk_usage += cellStorePtr->disk_usage();
}


namespace {
  struct ltCellStore {
    bool operator()(const CellStorePtr &csPtr1, const CellStorePtr &csPtr2) const {
      return !(csPtr1->disk_usage() < csPtr2->disk_usage());
    }
  };
}


void AccessGroup::run_compaction(uint64_t timestamp, bool major) {
  std::string cellStoreFile;
  char md5DigestStr[33];
  char filename[16];
  ByteString32T *key = 0;
  ByteString32T *value = 0;
  Key keyComps;
  CellListScannerPtr scannerPtr;
  size_t tableIndex = 1;
  int error;
  CellStorePtr cellStorePtr;
  vector<string> replacedFiles;
  std::string files;
  std::string metadata_key_str;

  timestamp++;

  {
    boost::mutex::scoped_lock lock(m_mutex);
    if (major) {
      // TODO: if the oldest CellCache entry is newer than timestamp, then return
      if (m_cell_cache_ptr->memory_used() == 0 && m_stores.size() <= (size_t)1)
	return;
      tableIndex = 0;
      LOG_VA_INFO("Starting Major Compaction (%s.%s)", m_table_name.c_str(), m_name.c_str());
    }
    else {
      if (m_cell_cache_ptr->memory_used() < (uint32_t)Global::localityGroupMaxMemory)
	return;

      if (m_stores.size() > (size_t)Global::localityGroupMaxFiles) {
	ltCellStore sortObj;
	sort(m_stores.begin(), m_stores.end(), sortObj);
	tableIndex = m_stores.size() - Global::localityGroupMergeFiles;
	LOG_VA_INFO("Starting Merging Compaction (%s.%s)", m_table_name.c_str(), m_name.c_str());
      }
      else {
	tableIndex = m_stores.size();
	LOG_VA_INFO("Starting Minor Compaction (%s.%s)", m_table_name.c_str(), m_name.c_str());
      }
    }
  }

  if (m_end_row == "")
    memset(md5DigestStr, '0', 24);
  else
    md5_string(m_end_row.c_str(), md5DigestStr);
  md5DigestStr[24] = 0;
  sprintf(filename, "cs%d", m_next_table_id++);
  cellStoreFile = (string)"/hypertable/tables/" + m_table_name + "/" + m_name + "/" + md5DigestStr + "/" + filename;

  cellStorePtr = new CellStoreV0(Global::dfs);

  if (cellStorePtr->create(cellStoreFile.c_str()) != 0) {
    LOG_VA_ERROR("Problem compacting locality group to file '%s'", cellStoreFile.c_str());
    return;
  }

  {
    ScanContextPtr scanContextPtr = new ScanContext(timestamp, 0, m_schema_ptr);

    if (major || tableIndex < m_stores.size()) {
      MergeScanner *mscanner = new MergeScanner(scanContextPtr, !major);
      mscanner->add_scanner( m_cell_cache_ptr->create_scanner(scanContextPtr) );
      for (size_t i=tableIndex; i<m_stores.size(); i++)
	mscanner->add_scanner( m_stores[i]->create_scanner(scanContextPtr) );
      scannerPtr = mscanner;
    }
    else
      scannerPtr = m_cell_cache_ptr->create_scanner(scanContextPtr);
  }

  while (scannerPtr->get(&key, &value)) {
    if (!keyComps.load(key)) {
      LOG_ERROR("Problem deserializing key/value pair");
      return;
    }
    // this assumes that the timestamp of the oldest entry in all CellStores is less than timestamp
    // this should be asserted somewhere.
    if (keyComps.timestamp <= timestamp)
      cellStorePtr->add(key, value);
    scannerPtr->forward();
  }

  {
    boost::mutex::scoped_lock lock(m_mutex);
    string fname;
    for (size_t i=tableIndex; i<m_stores.size(); i++) {
      fname = m_stores[i]->get_filename();
      replacedFiles.push_back(fname);  // hack: fix me!
    }
  }
    
  if (cellStorePtr->finalize(timestamp) != 0) {
    LOG_VA_ERROR("Problem finalizing CellStore '%s'", cellStoreFile.c_str());
    return;
  }

  /**
   * HACK: Delete underlying files -- fix me!!!
  for (size_t i=tableIndex; i<m_stores.size(); i++) {
    if ((m_stores[i]->get_flags() & CellStore::FLAG_SHARED) == 0) {
      std::string &fname = m_stores[i]->get_filename();
      Global::hdfsClient->remove(fname.c_str());
    }
  }
  */

  /**
   * Install new CellCache and CellStore
   */
  {
    boost::mutex::scoped_lock lock(m_mutex);

    /** Slice and install new CellCache **/
    m_cell_cache_ptr = m_cell_cache_ptr->slice_copy(timestamp);

    /** Drop the compacted tables from the table vector **/
    if (tableIndex < m_stores.size())
      m_stores.resize(tableIndex);

    /** Add the new table to the table vector **/
    m_stores.push_back(cellStorePtr);

    get_files(files);

    /** Re-compute disk usage **/
    m_disk_usage = 0;
    for (size_t i=0; i<m_stores.size(); i++)
      m_disk_usage += m_stores[i]->disk_usage();
  }

  try {
    MutatorPtr mutator_ptr;
    KeySpec key;

    if ((error = Global::metadata_table_ptr->create_mutator(mutator_ptr)) != Error::OK) {
      // TODO: throw exception
      LOG_VA_ERROR("Problem creating mutator on METADATA table - %s", Error::get_text(error));
    }
    else {
      metadata_key_str = std::string("") + (uint32_t)m_table_identifier.id + ":" + m_end_row;
      key.row = metadata_key_str.c_str();
      key.row_len = metadata_key_str.length();
      key.column_family = "Files";
      key.column_qualifier = m_name.c_str();
      key.column_qualifier_len = m_name.length();
      mutator_ptr->set(0, key, (uint8_t *)files.c_str(), files.length());
      mutator_ptr->flush();
    }
  }
  catch (Hypertable::Exception &e) {
    // TODO: propagate exception
    LOG_VA_ERROR("Problem updating METADATA with new file information (row key = %s) - %s", metadata_key_str.c_str(), e.what());
  }

  // TODO: Compaction thread function should re-shuffle the heap of locality groups and purge the commit log

  LOG_VA_INFO("Finished Compaction (%s.%s)", m_table_name.c_str(), m_name.c_str());
}


/**
 * 
 */
int AccessGroup::shrink(std::string &new_start_row) {
  boost::mutex::scoped_lock lock(m_mutex);
  int error;
  CellCachePtr old_cell_cache_ptr = m_cell_cache_ptr;
  ScanContextPtr scanContextPtr = new ScanContext(ScanContext::END_OF_TIME, 0, m_schema_ptr);
  CellListScannerPtr cell_cache_scanner_ptr;
  ByteString32T *key;
  ByteString32T *value;
  std::vector<CellStorePtr> new_stores;
  CellStore *new_cell_store;

  m_start_row = new_start_row;

  m_cell_cache_ptr = new CellCache();

  cell_cache_scanner_ptr = old_cell_cache_ptr->create_scanner(scanContextPtr);

  /**
   * Shrink the CellCache
   */
  while (cell_cache_scanner_ptr->get(&key, &value)) {
    if (strcmp((const char *)key->data, m_start_row.c_str()) > 0)
      add(key, value);
    cell_cache_scanner_ptr->forward();
  }

  /**
   * Shrink the CellStores
   */
  for (size_t i=0; i<m_stores.size(); i++) {
    std::string filename = m_stores[i]->get_filename();
    new_cell_store = new CellStoreV0(Global::dfs);
    if ((error = new_cell_store->open(filename.c_str(), m_start_row.c_str(), m_end_row.c_str())) != Error::OK) {
      LOG_VA_ERROR("Problem opening cell store '%s' [%s:%s] - %s",
		   filename.c_str(), m_start_row.c_str(), m_end_row.c_str(), Error::get_text(error));
      return error;
    }
    if ((error = new_cell_store->load_index()) != Error::OK) {
      LOG_VA_ERROR("Problem loading index of cell store '%s' [%s:%s] - %s",
		   filename.c_str(), m_start_row.c_str(), m_end_row.c_str(), Error::get_text(error));
      return error;
    }
    new_stores.push_back(new_cell_store);
  }

  m_stores = new_stores;

  return Error::OK;
}



/**
 *
 */
void AccessGroup::get_files(std::string &text) {
  text = "";
  for (size_t i=0; i<m_stores.size(); i++)
    text += m_stores[i]->get_filename() + ";\n";
}
