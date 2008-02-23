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
#include "MetadataNormal.h"
#include "MetadataRoot.h"


namespace {
  const uint32_t DEFAULT_BLOCKSIZE = 65536;
}


AccessGroup::AccessGroup(TableIdentifierT &table_identifier, SchemaPtr &schemaPtr, Schema::AccessGroup *ag, RangeT *range) : CellList(), m_mutex(), m_schema_ptr(schemaPtr), m_name(ag->name), m_stores(), m_cell_cache_ptr(), m_next_table_id(0), m_disk_usage(0), m_blocksize(DEFAULT_BLOCKSIZE), m_compression_ratio(1.0), m_is_root(false), m_oldest_cached_timestamp(0), m_collisions(0), m_needs_compaction(false), m_drop(false) {
  m_table_name = table_identifier.name;
  m_start_row = range->startRow;
  m_end_row = range->endRow;
  m_cell_cache_ptr = new CellCache();

  for (list<Schema::ColumnFamily *>::iterator iter = ag->columns.begin(); iter != ag->columns.end(); iter++) {
    m_column_families.insert((uint8_t)(*iter)->id);
  }
  Copy(table_identifier, m_table_identifier);
  if (ag->blocksize != 0)
    m_blocksize = ag->blocksize;

  m_compressor = (ag->compressor != "") ? ag->compressor : schemaPtr->get_compressor();

  m_is_root = (table_identifier.id == 0 && *range->startRow == 0 && !strcmp(range->endRow, Key::END_ROOT_ROW));

  m_in_memory = ag->in_memory;
}


AccessGroup::~AccessGroup() {
  if (m_drop) {
    if (m_table_identifier.id == 0) {
      HT_ERROR("~AccessGroup has drop bit set, but table is METADATA");
      Free(m_table_identifier);
      return;
    }
    std::string metadata_key = std::string("") + (uint32_t)m_table_identifier.id + ":" + m_end_row;
    int error;
    TableMutatorPtr mutator_ptr;
    KeySpec key;

    if ((error = Global::metadata_table_ptr->create_mutator(mutator_ptr)) != Error::OK) {
      HT_ERRORF("Problem creating mutator on METADATA table - %s", Error::get_text(error));
      Free(m_table_identifier);
      return;
    }

    try {
      key.row = metadata_key.c_str();
      key.row_len = metadata_key.length();
      key.column_family = "Files";
      key.column_qualifier = m_name.c_str();
      key.column_qualifier_len = m_name.length();
      mutator_ptr->set(key, (uint8_t *)"!", 1);
      mutator_ptr->flush();
    }
    catch (Hypertable::Exception &e) {
      // TODO: propagate exception
      HT_ERRORF("Problem updating 'File' column of METADATA (%s) - %s",
		metadata_key.c_str(), Error::get_text(e.code()));
    }
  }
  Free(m_table_identifier);
  return;
}

/**
 * This should be called with the CellCache locked
 * Also, at the end of compaction processing, when m_cell_cache_ptr gets reset to a new value,
 * the CellCache should be locked as well.
 */
int AccessGroup::add(const ByteString32T *key, const ByteString32T *value, uint64_t real_timestamp) {
  // assumes timestamps are coming in order
  if (m_oldest_cached_timestamp == 0)
    m_oldest_cached_timestamp = real_timestamp;
  return m_cell_cache_ptr->add(key, value, real_timestamp);
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
  get_split_rows(split_rows, true);
  if (split_rows.size() > 0) {
    sort(split_rows.begin(), split_rows.end());
    return (split_rows[split_rows.size()/2]).c_str();
  }
  return "";
}

void AccessGroup::get_split_rows(std::vector<std::string> &split_rows, bool include_cache) {
  boost::mutex::scoped_lock lock(m_mutex);
  const char *row;
  for (size_t i=0; i<m_stores.size(); i++) {
    row = m_stores[i]->get_split_row();
    if (row)
      split_rows.push_back(row);
  }
  if (include_cache)
    m_cell_cache_ptr->get_split_rows(split_rows);
}

void AccessGroup::get_cached_rows(std::vector<std::string> &rows) {
  m_cell_cache_ptr->get_rows(rows);
}

uint64_t AccessGroup::disk_usage() {
  boost::mutex::scoped_lock lock(m_mutex);
  return m_disk_usage + (uint64_t)(m_compression_ratio * (float)m_cell_cache_ptr->memory_used());
}

void AccessGroup::get_compaction_priority_data(CompactionPriorityDataT &priority_data) {
  boost::mutex::scoped_lock lock(m_mutex);
  priority_data.ag = this;
  priority_data.oldest_cached_timestamp = m_oldest_cached_timestamp;
  priority_data.mem_used = m_cell_cache_ptr->memory_used();
  priority_data.disk_used = m_disk_usage + (uint64_t)(m_compression_ratio * (float)m_cell_cache_ptr->memory_used());
  priority_data.in_memory = m_in_memory;
  priority_data.deletes = m_cell_cache_ptr->get_delete_count();
}


void AccessGroup::add_cell_store(CellStorePtr &cellstore_ptr, uint32_t id) {
  boost::mutex::scoped_lock lock(m_mutex);
  if (id >= m_next_table_id) 
    m_next_table_id = id+1;
  m_stores.push_back(cellstore_ptr);
  m_disk_usage += cellstore_ptr->disk_usage();
  if (m_compaction_timestamp.logical == 0)
    cellstore_ptr->get_timestamp(m_compaction_timestamp);
  else {
    Timestamp timestamp;
    cellstore_ptr->get_timestamp(timestamp);
    if (timestamp.real > m_compaction_timestamp.real)
      m_compaction_timestamp = timestamp;
  }
}


namespace {
  struct ltCellStore {
    bool operator()(const CellStorePtr &csPtr1, const CellStorePtr &csPtr2) const {
      return !(csPtr1->disk_usage() < csPtr2->disk_usage());
    }
  };
}


void AccessGroup::run_compaction(Timestamp timestamp, bool major) {
  std::string cellStoreFile;
  char md5DigestStr[33];
  char filename[16];
  ByteString32T *key = 0;
  ByteString32T *value = 0;
  Key keyComps;
  CellListScannerPtr scannerPtr;
  size_t tableIndex = 1;
  CellStorePtr cellStorePtr;
  std::string files;
  std::string metadata_key_str;

  if (!major && !m_needs_compaction)
    return;

  m_needs_compaction = false;

  {
    boost::mutex::scoped_lock lock(m_mutex);
    if (m_in_memory) {
      tableIndex = 0;
      HT_INFOF("Starting InMemory Compaction (%s.%s end_row='%s')", m_table_name.c_str(), m_name.c_str(), m_end_row.c_str());
    }
    else if (major) {
      // TODO: if the oldest CellCache entry is newer than timestamp, then return
      if (m_cell_cache_ptr->memory_used() == 0 && m_stores.size() <= (size_t)1)
	return;
      tableIndex = 0;
      HT_INFOF("Starting Major Compaction (%s.%s)", m_table_name.c_str(), m_name.c_str());
    }
    else {
      if (m_stores.size() > (size_t)Global::localityGroupMaxFiles) {
	ltCellStore sortObj;
	sort(m_stores.begin(), m_stores.end(), sortObj);
	tableIndex = m_stores.size() - Global::localityGroupMergeFiles;
	HT_INFOF("Starting Merging Compaction (%s.%s)", m_table_name.c_str(), m_name.c_str());
      }
      else {
	tableIndex = m_stores.size();
	HT_INFOF("Starting Minor Compaction (%s.%s)", m_table_name.c_str(), m_name.c_str());
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

  if (cellStorePtr->create(cellStoreFile.c_str(), m_blocksize, m_compressor) != 0) {
    HT_ERRORF("Problem compacting locality group to file '%s'", cellStoreFile.c_str());
    return;
  }

  {
    ScanContextPtr scanContextPtr = new ScanContext(timestamp.logical+1, m_schema_ptr);

    if (m_in_memory) {
      MergeScanner *mscanner = new MergeScanner(scanContextPtr, false);
      mscanner->add_scanner( m_cell_cache_ptr->create_scanner(scanContextPtr) );
      scannerPtr = mscanner;
    }
    else if (major || tableIndex < m_stores.size()) {
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
      HT_ERROR("Problem deserializing key/value pair");
      return;
    }

    if (keyComps.timestamp <= timestamp.logical)
      cellStorePtr->add(key, value, timestamp.real);

    scannerPtr->forward();
  }

  if (cellStorePtr->finalize(timestamp) != 0) {
    HT_ERRORF("Problem finalizing CellStore '%s'", cellStoreFile.c_str());
    return;
  }

  /**
   * Install new CellCache and CellStore
   */
  {
    boost::mutex::scoped_lock lock(m_mutex);
    CellCachePtr tmp_cell_cache_ptr;

    /** Slice and install new CellCache **/
    tmp_cell_cache_ptr = m_cell_cache_ptr;
    tmp_cell_cache_ptr->lock();
    m_collisions += m_cell_cache_ptr->get_collision_count();
    if (m_in_memory)
      m_cell_cache_ptr->purge_deletes();
    else
      m_cell_cache_ptr = m_cell_cache_ptr->slice_copy(timestamp.logical);
    // If inserts have arrived since we started splitting, then set the oldest cached timestamp value, otherwise clear it
    m_oldest_cached_timestamp = (m_cell_cache_ptr->size() > 0) ? timestamp.real + 1 : 0;
    tmp_cell_cache_ptr->unlock();    

    /** Drop the compacted tables from the table vector **/
    if (tableIndex < m_stores.size())
      m_stores.resize(tableIndex);

    /** Add the new table to the table vector **/
    m_stores.push_back(cellStorePtr);

    get_files(files);

    /** Re-compute disk usage and compresion ratio**/
    m_disk_usage = 0;
    m_compression_ratio = 0.0;
    for (size_t i=0; i<m_stores.size(); i++) {
      m_disk_usage += m_stores[i]->disk_usage();
      m_compression_ratio += m_stores[i]->compression_ratio();
    }
    m_compression_ratio /= m_stores.size();

    m_compaction_timestamp = timestamp;

  }

  try {
    if (m_is_root) {
      MetadataRoot metadata(m_schema_ptr);
      metadata.write_files(m_name, files);
    }
    else {
      MetadataNormal metadata(m_table_identifier, m_end_row);
      metadata.write_files(m_name, files);
    }
  }
  catch (Hypertable::Exception &e) {
    // TODO: propagate exception
    HT_ERRORF("Problem updating 'File' column of METADATA (%d:%s) - %s",
		 m_table_identifier.id, metadata_key_str.c_str(), Error::get_text(e.code()));
  }

  HT_INFOF("Finished Compaction (%s.%s)", m_table_name.c_str(), m_name.c_str());
}


/**
 * 
 */
int AccessGroup::shrink(std::string &new_start_row) {
  boost::mutex::scoped_lock lock(m_mutex);
  int error;
  CellCachePtr old_cell_cache_ptr = m_cell_cache_ptr;
  CellCachePtr new_cell_cache_ptr;
  ScanContextPtr scanContextPtr = new ScanContext(ScanContext::END_OF_TIME, m_schema_ptr);
  CellListScannerPtr cell_cache_scanner_ptr;
  ByteString32T *key;
  ByteString32T *value;
  std::vector<CellStorePtr> new_stores;
  CellStore *new_cell_store;

  m_start_row = new_start_row;

  new_cell_cache_ptr = new CellCache();
  new_cell_cache_ptr->lock();

  m_cell_cache_ptr = new_cell_cache_ptr;

  cell_cache_scanner_ptr = old_cell_cache_ptr->create_scanner(scanContextPtr);

  /**
   * Shrink the CellCache
   */
  while (cell_cache_scanner_ptr->get(&key, &value)) {
    if (strcmp((const char *)key->data, m_start_row.c_str()) > 0)
      add(key, value, 0);
    cell_cache_scanner_ptr->forward();
  }

  new_cell_cache_ptr->unlock();

  /**
   * Shrink the CellStores
   */
  for (size_t i=0; i<m_stores.size(); i++) {
    std::string filename = m_stores[i]->get_filename();
    new_cell_store = new CellStoreV0(Global::dfs);
    if ((error = new_cell_store->open(filename.c_str(), m_start_row.c_str(), m_end_row.c_str())) != Error::OK) {
      HT_ERRORF("Problem opening cell store '%s' [%s:%s] - %s",
		   filename.c_str(), m_start_row.c_str(), m_end_row.c_str(), Error::get_text(error));
      return error;
    }
    if ((error = new_cell_store->load_index()) != Error::OK) {
      HT_ERRORF("Problem loading index of cell store '%s' [%s:%s] - %s",
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


/**
 *
 */
void AccessGroup::get_compaction_timestamp(Timestamp &timestamp) {
  boost::mutex::scoped_lock lock(m_mutex);
  timestamp = m_compaction_timestamp;
}
