/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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
#include <algorithm>
#include <iterator>
#include <vector>

#include "Common/Error.h"
#include "Common/md5.h"

#include "AccessGroup.h"
#include "CellCache.h"
#include "CellCacheScanner.h"
#include "CellStoreReleaseCallback.h"
#include "CellStoreV0.h"
#include "Global.h"
#include "MergeScanner.h"
#include "MetadataNormal.h"
#include "MetadataRoot.h"

using namespace Hypertable;

namespace {
  const uint32_t DEFAULT_BLOCKSIZE = 65536;
}


AccessGroup::AccessGroup(const TableIdentifier *identifier,
    SchemaPtr &schema_ptr, Schema::AccessGroup *ag, const RangeSpec *range)
  : m_identifier(*identifier), m_schema_ptr(schema_ptr), m_name(ag->name),
    m_next_table_id(0), m_disk_usage(0), m_blocksize(DEFAULT_BLOCKSIZE),
    m_compression_ratio(1.0), m_is_root(false), m_compaction_revision(0),
    m_earliest_cached_revision(TIMESTAMP_NULL),
    m_earliest_cached_revision_saved(TIMESTAMP_NULL),
    m_collisions(0), m_needs_compaction(false), m_drop(false),
    m_file_tracker(identifier, schema_ptr, range, ag->name),
    m_scanners_blocked(false) {
  m_table_name = m_identifier.name;
  m_start_row = range->start_row;
  m_end_row = range->end_row;
  m_range_name = m_table_name + "[" + m_start_row + ".." + m_end_row + "]";
  m_cell_cache_ptr = new CellCache();

  foreach(Schema::ColumnFamily *cf, ag->columns)
    m_column_families.insert(cf->id);

  if (ag->blocksize != 0)
    m_blocksize = ag->blocksize;

  m_compressor = (ag->compressor != "") ? ag->compressor
                                        : schema_ptr->get_compressor();

  m_is_root = (m_identifier.id == 0 && *range->start_row == 0
               && !strcmp(range->end_row, Key::END_ROOT_ROW));

  m_in_memory = ag->in_memory;
}


AccessGroup::~AccessGroup() {
  if (m_drop) {
    if (m_identifier.id == 0) {
      HT_ERROR("~AccessGroup has drop bit set, but table is METADATA");
      return;
    }
    String metadata_key = format("%u:%s", m_identifier.id, m_end_row.c_str());
    TableMutatorPtr mutator_ptr;
    KeySpec key;

    try {

      mutator_ptr = Global::metadata_table_ptr->create_mutator();

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
  return;
}

/**
 * This should be called with the CellCache locked Also, at the end of
 * compaction processing, when m_cell_cache_ptr gets reset to a new value, the
 * CellCache should be locked as well.
 */
int AccessGroup::add(const Key &key, const ByteString value) {
  if (!m_recovering || key.revision > m_compaction_revision || m_in_memory) {
    if (m_earliest_cached_revision == TIMESTAMP_NULL)
      m_earliest_cached_revision = key.revision;
    return m_cell_cache_ptr->add(key, value);
  }
  return Error::OK;
}


CellListScanner *AccessGroup::create_scanner(ScanContextPtr &scan_context_ptr) {
  MergeScanner *scanner = new MergeScanner(scan_context_ptr);
  std::vector<String> filenames;

  {
    ScopedLock lock(m_mutex);

    while (m_scanners_blocked)
      m_scanner_blocked_cond.wait(lock);

    scanner->add_scanner(m_cell_cache_ptr->create_scanner(scan_context_ptr));
    if (m_immutable_cache_ptr)
      scanner->add_scanner(m_immutable_cache_ptr->create_scanner(
                           scan_context_ptr));
    if (!m_in_memory) {
      CellStoreReleaseCallback callback(this);
      for (size_t i=0; i<m_stores.size(); i++) {
        scanner->add_scanner(m_stores[i]->create_scanner(scan_context_ptr));
        callback.add_file( m_stores[i]->get_filename() );
      }
      filenames = callback.get_file_vector();
      scanner->install_release_callback(callback);
    }
  }

  // This can't be done while holding m_mutex (causes deadlock)
  m_file_tracker.add_references(filenames);

  return scanner;
}

bool AccessGroup::include_in_scan(ScanContextPtr &scan_context_ptr) {
  ScopedLock lock(m_mutex);
  for (std::set<uint8_t>::iterator iter = m_column_families.begin();
       iter != m_column_families.end(); ++iter) {
    if (scan_context_ptr->family_mask[*iter])
      return true;
  }
  return false;
}

const char *AccessGroup::get_split_row() {
  std::vector<String> split_rows;
  get_split_rows(split_rows, true);
  if (split_rows.size() > 0) {
    sort(split_rows.begin(), split_rows.end());
    return (split_rows[split_rows.size()/2]).c_str();
  }
  return "";
}

void
AccessGroup::get_split_rows(std::vector<String> &split_rows,
                            bool include_cache) {
  ScopedLock lock(m_mutex);
  const char *row;
  for (size_t i=0; i<m_stores.size(); i++) {
    row = m_stores[i]->get_split_row();
    if (row)
      split_rows.push_back(row);
  }
  if (include_cache) {
    if (m_immutable_cache_ptr &&
        m_immutable_cache_ptr->size() > m_cell_cache_ptr->size())
      m_immutable_cache_ptr->get_split_rows(split_rows);
    else
      m_cell_cache_ptr->get_split_rows(split_rows);
  }
}

void AccessGroup::get_cached_rows(std::vector<String> &rows) {
  ScopedLock lock(m_mutex);
  if (m_immutable_cache_ptr &&
      m_immutable_cache_ptr->size() > m_cell_cache_ptr->size())
    m_immutable_cache_ptr->get_rows(rows);
  else
    m_cell_cache_ptr->get_rows(rows);
}

uint64_t AccessGroup::disk_usage() {
  ScopedLock lock(m_mutex);
  uint64_t du = (m_in_memory) ? 0 : m_disk_usage;
  uint64_t mu = m_cell_cache_ptr->memory_used();
  if (m_immutable_cache_ptr)
    mu += m_immutable_cache_ptr->memory_used();
  return du + (uint64_t)(m_compression_ratio * (float)mu);
}

uint64_t AccessGroup::memory_usage() {
  ScopedLock lock(m_mutex);
  uint64_t mu = m_cell_cache_ptr->memory_used();
  if (m_immutable_cache_ptr)
    mu += m_immutable_cache_ptr->memory_used();
  return mu;
}

void
AccessGroup::get_compaction_priority_data(
    CompactionPriorityData &priority_data) {
  ScopedLock lock(m_mutex);
  priority_data.ag = this;

  if (m_earliest_cached_revision_saved != TIMESTAMP_NULL)
    priority_data.earliest_cached_revision = m_earliest_cached_revision_saved;
  else
    priority_data.earliest_cached_revision = m_earliest_cached_revision;

  uint64_t mu = m_cell_cache_ptr->memory_used();
  if (m_immutable_cache_ptr)
    mu += m_immutable_cache_ptr->memory_used();
  priority_data.mem_used = mu;
  priority_data.disk_used = m_disk_usage + (uint64_t)(m_compression_ratio
                                                      * (float)mu);
  priority_data.in_memory = m_in_memory;
  priority_data.deletes = m_cell_cache_ptr->get_delete_count();
  if (m_immutable_cache_ptr)
    priority_data.deletes += m_immutable_cache_ptr->get_delete_count();
}


void AccessGroup::add_cell_store(CellStorePtr &cellstore_ptr, uint32_t id) {
  ScopedLock lock(m_mutex);

  // Figure out the "next" CellStore number
  if (id >= m_next_table_id)
    m_next_table_id = id+1;
  else if (m_in_memory)
    return;

  m_disk_usage += cellstore_ptr->disk_usage();
  m_compression_ratio = cellstore_ptr->compression_ratio();

  // Record the most recent compaction revision
  if (m_compaction_revision == 0)
    m_compaction_revision = cellstore_ptr->get_revision();
  else {
    int64_t revision = cellstore_ptr->get_revision();
    if (revision > m_compaction_revision)
      m_compaction_revision = revision;
  }

  if (m_in_memory) {
    ScanContextPtr scan_context_ptr = new ScanContext(m_schema_ptr);
    CellListScannerPtr scanner_ptr =
        cellstore_ptr->create_scanner(scan_context_ptr);
    ByteString key, value;
    Key key_comps;
    m_cell_cache_ptr = new CellCache();
    while (scanner_ptr->get(key_comps, value)) {
      m_cell_cache_ptr->add(key_comps, value);
      scanner_ptr->forward();
    }
  }

  m_stores.push_back(cellstore_ptr);

}

namespace {
  struct LtCellStore {
    bool operator()(const CellStorePtr &x, const CellStorePtr &y) const {
      return !(x->disk_usage() < y->disk_usage());
    }
  };
}


void AccessGroup::run_compaction(bool major) {
  ByteString bskey;
  ByteString value;
  Key key;
  CellListScannerPtr scanner_ptr;
  size_t tableidx = 1;
  CellStorePtr cellstore;
  String metadata_key_str;

  try {

    if (!major && !m_needs_compaction)
      HT_THROW(Error::OK, "");

    HT_ASSERT(m_immutable_cache_ptr);

    {
      ScopedLock lock(m_mutex);
      if (m_in_memory) {
        if (m_immutable_cache_ptr->memory_used() == 0) {
          m_immutable_cache_ptr = 0;
          HT_THROW(Error::OK, "");
        }
        tableidx = m_stores.size();
        HT_INFOF("Starting InMemory Compaction of %s(%s)",
                 m_range_name.c_str(), m_name.c_str());
      }
      else if (major) {
        if (m_immutable_cache_ptr->memory_used() == 0
            && m_stores.size() <= (size_t)1) {
          m_immutable_cache_ptr = 0;
          HT_THROW(Error::OK, "");
        }
        tableidx = 0;
        HT_INFOF("Starting Major Compaction of %s(%s)",
                 m_range_name.c_str(), m_name.c_str());
      }
      else {
        if (m_stores.size() > (size_t)Global::access_group_max_files) {
          LtCellStore ascending;
          sort(m_stores.begin(), m_stores.end(), ascending);
          tableidx = m_stores.size() - Global::access_group_merge_files;
          HT_INFOF("Starting Merging Compaction of %s(%s)",
                   m_range_name.c_str(), m_name.c_str());
        }
        else {
          if (m_immutable_cache_ptr->memory_used() == 0) {
            m_immutable_cache_ptr = 0;
            HT_THROW(Error::OK, "");
          }
          tableidx = m_stores.size();
          HT_INFOF("Starting Minor Compaction of %s(%s)",
                   m_range_name.c_str(), m_name.c_str());
        }
      }
    }

  }
  catch (Exception &e) {
    if (m_earliest_cached_revision_saved != TIMESTAMP_NULL)
      m_earliest_cached_revision = m_earliest_cached_revision_saved;
    return;
  }

  try {

    // TODO: Issue 11
    char hash_str[33];

    if (m_end_row == "")
      memset(hash_str, '0', 24);
    else
      md5_string(m_end_row.c_str(), hash_str);

    hash_str[24] = 0;
    String cs_file = format("/hypertable/tables/%s/%s/%s/cs%d",
                            m_table_name.c_str(), m_name.c_str(), hash_str,
                            m_next_table_id++);

    cellstore = new CellStoreV0(Global::dfs);

    cellstore->create(cs_file.c_str(), m_blocksize, m_compressor);

    {
      ScopedLock lock(m_mutex);
      ScanContextPtr scan_context_ptr = new ScanContext(m_schema_ptr);

      if (m_in_memory) {
        MergeScanner *mscanner = new MergeScanner(scan_context_ptr, false);
        mscanner->add_scanner(m_immutable_cache_ptr->create_scanner(
                              scan_context_ptr));
        scanner_ptr = mscanner;
      }
      else if (major || tableidx < m_stores.size()) {
        MergeScanner *mscanner = new MergeScanner(scan_context_ptr, !major);
        mscanner->add_scanner(m_immutable_cache_ptr->create_scanner(
                              scan_context_ptr));
        for (size_t i=tableidx; i<m_stores.size(); i++)
          mscanner->add_scanner(m_stores[i]->create_scanner(scan_context_ptr));
        scanner_ptr = mscanner;
      }
      else
        scanner_ptr = m_immutable_cache_ptr->create_scanner(scan_context_ptr);
    }

    while (scanner_ptr->get(key, value)) {
      cellstore->add(key, value);
      scanner_ptr->forward();
    }

    cellstore->finalize(&m_identifier);

    /**
     * Update 'Files' column
     */
    {
      ScopedLock lock(m_mutex);

      m_file_tracker.clear_live();

      if (!m_in_memory) {
        for (size_t i=0; i<tableidx; i++)
          m_file_tracker.add_live(m_stores[i]->get_filename());
      }
      if (cellstore->get_total_entries() > 0)
        m_file_tracker.add_live(cellstore->get_filename());
    }

    m_file_tracker.update_files_column();

    /**
     * Install new CellCache and CellStore
     */
    {
      ScopedLock lock(m_mutex);

      if (m_in_memory) {
        merge_caches();
        m_stores.clear();
      }
      else {

        m_immutable_cache_ptr = 0;

        /** Drop the compacted tables from the table vector **/
        if (tableidx < m_stores.size())
          m_stores.resize(tableidx);
      }

      /** Add the new cell store to the table vector, or delete it if
       * it contains no entries
       */
      if (cellstore->get_total_entries() > 0) {
        m_stores.push_back(cellstore);
      }
      else {
        String fname = cellstore->get_filename();
        cellstore = 0;
        try {
          Global::dfs->remove(fname);
        }
        catch (Hypertable::Exception &e) {
          HT_ERROR_OUT << "Problem removing '" << fname.c_str() << "' " \
                       << e << HT_END;
        }
      }

      /** Re-compute disk usage and compresion ratio**/
      m_disk_usage = 0;
      m_compression_ratio = 0.0;
      for (size_t i=0; i<m_stores.size(); i++) {
        double disk_usage = m_stores[i]->disk_usage();
        m_disk_usage += (uint64_t)disk_usage;
        m_compression_ratio += m_stores[i]->compression_ratio() * disk_usage;
      }
      m_compression_ratio /= m_disk_usage;

      if (cellstore)
        m_compaction_revision = cellstore->get_revision();

      m_scanners_blocked = true;
    }

    /**
     * un-block scanners
     */
    m_scanners_blocked = false;
    m_scanner_blocked_cond.notify_all();

    m_earliest_cached_revision_saved = TIMESTAMP_NULL;

    HT_INFOF("Finished Compaction of %s(%s)", m_range_name.c_str(),
             m_name.c_str());

  }
  catch (Exception &e) {
    ScopedLock lock(m_mutex);
    HT_ERROR_OUT << m_range_name << "(" << m_name << ") " << e << HT_END;
    merge_caches();
    if (m_earliest_cached_revision_saved != TIMESTAMP_NULL)
      m_earliest_cached_revision = m_earliest_cached_revision_saved;
    if (m_scanners_blocked) {
      m_scanners_blocked = false;
      m_scanner_blocked_cond.notify_all();
    }
    throw;
  }

  m_needs_compaction = false;
}



/**
 *
 */
void AccessGroup::shrink(String &split_row, bool drop_high) {
  ScopedLock lock(m_mutex);
  CellCachePtr old_cell_cache_ptr = m_cell_cache_ptr;
  CellCachePtr new_cell_cache_ptr;
  ScanContextPtr scan_context_ptr = new ScanContext(m_schema_ptr);
  CellListScannerPtr cell_cache_scanner_ptr;
  ByteString key;
  ByteString value;
  Key key_comps;
  std::vector<CellStorePtr> new_stores;
  CellStore *new_cell_store;
  uint64_t memory_added = 0;
  uint64_t items_added = 0;
  int cmp;

  if (drop_high) {
    m_end_row = split_row;
    m_next_table_id = 0;
  }
  else
    m_start_row = split_row;

  m_range_name = m_table_name + "[" + m_start_row + ".." + m_end_row + "]";

  m_file_tracker.change_range(m_start_row, m_end_row);

  new_cell_cache_ptr = new CellCache();
  new_cell_cache_ptr->lock();

  m_cell_cache_ptr = new_cell_cache_ptr;

  cell_cache_scanner_ptr = old_cell_cache_ptr->create_scanner(scan_context_ptr);

  /**
   * Shrink the CellCache
   */
  while (cell_cache_scanner_ptr->get(key_comps, value)) {

    cmp = strcmp(key_comps.row, split_row.c_str());

    if ((cmp > 0 && !drop_high) || (cmp <= 0 && drop_high)) {
      add(key_comps, value);
      memory_added += key.length() + value.length();
      items_added++;
    }
    cell_cache_scanner_ptr->forward();
  }

  new_cell_cache_ptr->unlock();

  /**
   * Shrink the CellStores
   */
  for (size_t i=0; i<m_stores.size(); i++) {
    String filename = m_stores[i]->get_filename();
    new_cell_store = new CellStoreV0(Global::dfs);
    new_cell_store->open(filename.c_str(), m_start_row.c_str(),
                         m_end_row.c_str());
    new_cell_store->load_index();
    new_stores.push_back(new_cell_store);
  }

  m_stores = new_stores;
}



/**
 */
void AccessGroup::get_files(String &text) {
  ScopedLock lock(m_mutex);
  text = "";
  for (size_t i=0; i<m_stores.size(); i++)
    text += m_stores[i]->get_filename() + ";\n";
}


/**
 */
void AccessGroup::release_files(const std::vector<String> &files) {
  m_file_tracker.remove_references(files);
  m_file_tracker.update_files_column();
}


void AccessGroup::initiate_compaction() {
  ScopedLock lock(m_mutex);
  HT_ASSERT(!m_immutable_cache_ptr);
  m_immutable_cache_ptr = m_cell_cache_ptr;
  m_immutable_cache_ptr->freeze();
  m_cell_cache_ptr = new CellCache();
  m_earliest_cached_revision_saved = m_earliest_cached_revision;
  m_earliest_cached_revision = TIMESTAMP_NULL;
}



/**
 * Assumes mutex is locked
 */
void AccessGroup::merge_caches() {
  CellListScannerPtr scanner_ptr;
  Key key;
  ByteString value;
  CellCachePtr merged_cache_ptr = new CellCache();
  ScanContextPtr scan_context_ptr = new ScanContext(m_schema_ptr);

  if (!m_immutable_cache_ptr)
    return;

  // Add immutable cache
  if (m_immutable_cache_ptr->size() > 0) {
    scanner_ptr = m_immutable_cache_ptr->create_scanner(scan_context_ptr);
    while (scanner_ptr->get(key, value)) {
      merged_cache_ptr->add(key, value);
      scanner_ptr->forward();
    }
  }

  // Add cell cache
  if (m_cell_cache_ptr->size() > 0) {
    scanner_ptr = m_cell_cache_ptr->create_scanner(scan_context_ptr);
    while (scanner_ptr->get(key, value)) {
      merged_cache_ptr->add(key, value);
      scanner_ptr->forward();
    }
  }
  m_immutable_cache_ptr = 0;
  m_cell_cache_ptr = merged_cache_ptr;
}

