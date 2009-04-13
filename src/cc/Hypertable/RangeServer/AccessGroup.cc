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
#include "Config.h"

using namespace Hypertable;


AccessGroup::AccessGroup(const TableIdentifier *identifier,
    SchemaPtr &schema, Schema::AccessGroup *ag, const RangeSpec *range)
  : m_identifier(*identifier), m_schema(schema), m_name(ag->name),
    m_next_cs_id(0), m_disk_usage(0), m_compression_ratio(1.0),
    m_is_root(false), m_compaction_revision(0),
    m_earliest_cached_revision(TIMESTAMP_NULL),
    m_earliest_cached_revision_saved(TIMESTAMP_NULL),
    m_collisions(0), m_needs_compaction(false), m_drop(false),
    m_file_tracker(identifier, schema, range, ag->name),
    m_recovering(false) {

  m_table_name = m_identifier.name;
  m_start_row = range->start_row;
  m_end_row = range->end_row;
  m_range_name = m_table_name + "[" + m_start_row + ".." + m_end_row + "]";
  m_full_name = m_range_name + "(" + m_name + ")";
  m_cell_cache = new CellCache();

  foreach(Schema::ColumnFamily *cf, ag->columns)
    m_column_families.insert(cf->id);

  m_is_root = (m_identifier.id == 0 && *range->start_row == 0
               && !strcmp(range->end_row, Key::END_ROOT_ROW));
  m_in_memory = ag->in_memory;

  m_cellstore_props = new Properties();
  m_cellstore_props->set("compressor", ag->compressor.size() ?
      ag->compressor : schema->get_compressor());
  m_cellstore_props->set("blocksize", ag->blocksize);

  if (ag->bloom_filter.size())
    Schema::parse_bloom_filter(ag->bloom_filter, m_cellstore_props);
  else {
    assert(Config::properties); // requires Config::init* first
    Schema::parse_bloom_filter(Config::get_str("Hypertable.RangeServer"
        ".CellStore.DefaultBloomFilter"), m_cellstore_props);
  }
  m_bloom_filter_disabled = BLOOM_FILTER_DISABLED ==
      m_cellstore_props->get<BloomFilterMode>("bloom-filter-mode");
}


AccessGroup::~AccessGroup() {
  if (m_drop) {
    if (m_identifier.id == 0) {
      HT_ERROR("~AccessGroup has drop bit set, but table is METADATA");
      return;
    }
    String metadata_key = format("%u:%s", m_identifier.id, m_end_row.c_str());
    TableMutatorPtr mutator;
    KeySpec key;

    try {
      mutator = Global::metadata_table->create_mutator();

      key.row = metadata_key.c_str();
      key.row_len = metadata_key.length();
      key.column_family = "Files";
      key.column_qualifier = m_name.c_str();
      key.column_qualifier_len = m_name.length();
      mutator->set(key, (uint8_t *)"!", 1);
      mutator->flush();
    }
    catch (Hypertable::Exception &e) {
      HT_ERRORF("Problem updating 'File' column of METADATA (%s) - %s",
                metadata_key.c_str(), Error::get_text(e.code()));
    }
  }
}

/**
 * Currently supports only adding and deleting column families
 * from AccessGroup. Changing other attributes of existing
 * AccessGroup is not supported.
 * Schema is only updated if the new schema has a more recent generation
 * number than the existing schema.
 */
void AccessGroup::update_schema(SchemaPtr &schema_ptr,
                                Schema::AccessGroup *ag) {
  ScopedLock lock(m_mutex);
  std::set<uint8_t>::iterator iter;

  if (schema_ptr->get_generation() > m_schema->get_generation()) {
    foreach(Schema::ColumnFamily *cf, ag->columns) {
      if((iter = m_column_families.find(cf->id)) == m_column_families.end()) {
        // Add new column families
        if(cf->deleted == false ) {
          m_column_families.insert(cf->id);
        }
      }
      else {
        // Delete existing cfs
        if (cf->deleted == true) {
          m_column_families.erase(iter);
        }
        // TODO: In future other types of updates
        // such as alter table modify etc will go in here
      }
    }
    // Update schema ptr
    m_schema = schema_ptr;
  }
}

/**
 * This should be called with the CellCache locked Also, at the end of
 * compaction processing, when m_cell_cache gets reset to a new value, the
 * CellCache should be locked as well.
 */
void AccessGroup::add(const Key &key, const ByteString value) {
  if (key.revision > m_compaction_revision || !m_recovering) {
    if (m_earliest_cached_revision == TIMESTAMP_NULL)
      m_earliest_cached_revision = key.revision;
    return m_cell_cache->add(key, value);
  }
}


CellListScanner *AccessGroup::create_scanner(ScanContextPtr &scan_context) {
  MergeScanner *scanner = new MergeScanner(scan_context);
  CellStoreReleaseCallback callback(this);

  {
    ScopedLock lock(m_mutex);

    scanner->add_scanner(m_cell_cache->create_scanner(scan_context));

    if (m_immutable_cache)
      scanner->add_scanner(m_immutable_cache->create_scanner(scan_context));

    if (!m_in_memory) {
      bool no_filter = m_bloom_filter_disabled || !scan_context->single_row;

      foreach(CellStorePtr &cellstore, m_stores) {
        if (no_filter || cellstore->may_contain(scan_context)) {
          scanner->add_scanner(cellstore->create_scanner(scan_context));
          callback.add_file(cellstore->get_filename());
        }
      }
    }
  }

  m_file_tracker.add_references(callback.get_file_vector());
  scanner->install_release_callback(callback);

  return scanner;
}

bool AccessGroup::include_in_scan(ScanContextPtr &scan_context) {
  ScopedLock lock(m_mutex);
  for (std::set<uint8_t>::iterator iter = m_column_families.begin();
       iter != m_column_families.end(); ++iter) {
    if (scan_context->family_mask[*iter])
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
    if (m_immutable_cache &&
        m_immutable_cache->size() > m_cell_cache->size())
      m_immutable_cache->get_split_rows(split_rows);
    else
      m_cell_cache->get_split_rows(split_rows);
  }
}

void AccessGroup::get_cached_rows(std::vector<String> &rows) {
  ScopedLock lock(m_mutex);
  if (m_immutable_cache &&
      m_immutable_cache->size() > m_cell_cache->size())
    m_immutable_cache->get_rows(rows);
  else
    m_cell_cache->get_rows(rows);
}

uint64_t AccessGroup::disk_usage() {
  ScopedLock lock(m_mutex);
  uint64_t du = (m_in_memory) ? 0 : m_disk_usage;
  uint64_t mu = m_cell_cache->memory_used();
  if (m_immutable_cache)
    mu += m_immutable_cache->memory_used();
  return du + (uint64_t)(m_compression_ratio * (float)mu);
}

uint64_t AccessGroup::memory_usage() {
  ScopedLock lock(m_mutex);
  uint64_t mu = m_cell_cache->memory_used();
  if (m_immutable_cache)
    mu += m_immutable_cache->memory_used();
  return mu;
}

void AccessGroup::space_usage(int64_t *memp, int64_t *diskp) {
  ScopedLock lock(m_mutex);
  *memp = m_cell_cache->memory_used();
  if (m_immutable_cache)
    *memp += m_immutable_cache->memory_used();
  *diskp = (m_in_memory) ? 0 : m_disk_usage;
  *diskp += (int64_t)(m_compression_ratio * (float)*memp);
}



AccessGroup::MaintenanceData *AccessGroup::get_maintenance_data(ByteArena &arena) {
  ScopedLock lock(m_mutex);
  MaintenanceData *mdata = (MaintenanceData *)arena.alloc(sizeof(MaintenanceData));

  mdata->ag = this;

  if (m_earliest_cached_revision_saved != TIMESTAMP_NULL)
    mdata->earliest_cached_revision = m_earliest_cached_revision_saved;
  else
    mdata->earliest_cached_revision = m_earliest_cached_revision;

  int64_t mu = m_cell_cache->memory_used();
  if (m_immutable_cache)
    mu += m_immutable_cache->memory_used();
  mdata->mem_used = mu;

  int64_t du = m_in_memory ? 0 : m_disk_usage;
  mdata->disk_used = du + (int64_t)(m_compression_ratio * (float)mu);

  mdata->in_memory = m_in_memory;
  mdata->deletes = m_cell_cache->get_delete_count();

  // add TTL stuff

  return mdata;
}


void AccessGroup::add_cell_store(CellStorePtr &cellstore, uint32_t id) {
  ScopedLock lock(m_mutex);

  // Figure out the "next" CellStore number
  if (id >= m_next_cs_id)
    m_next_cs_id = id+1;
  else if (m_in_memory)
    return;

  m_disk_usage += cellstore->disk_usage();
  m_compression_ratio = cellstore->compression_ratio();

  // Record the most recent compaction revision
  if (m_compaction_revision == 0)
    m_compaction_revision = cellstore->get_revision();
  else {
    int64_t revision = cellstore->get_revision();
    if (revision > m_compaction_revision)
      m_compaction_revision = revision;
  }

  if (m_in_memory) {
    HT_ASSERT(m_stores.empty());
    ScanContextPtr scan_context = new ScanContext(m_schema);
    CellListScannerPtr scanner = cellstore->create_scanner(scan_context);
    ByteString key, value;
    Key key_comps;
    m_cell_cache = new CellCache();
    while (scanner->get(key_comps, value)) {
      m_cell_cache->add(key_comps, value);
      scanner->forward();
    }
  }

  m_stores.push_back(cellstore);

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
  CellListScannerPtr scanner;
  size_t tableidx = 1;
  CellStorePtr cellstore;
  String metadata_key_str;

  try {

    if (!major && !m_needs_compaction)
      HT_THROW(Error::OK, "");

    HT_ASSERT(m_immutable_cache);

    {
      ScopedLock lock(m_mutex);
      if (m_in_memory) {
        if (m_immutable_cache->memory_used() == 0)
          HT_THROW(Error::OK, "");
        tableidx = m_stores.size();
        HT_INFOF("Starting InMemory Compaction of %s(%s)",
                 m_range_name.c_str(), m_name.c_str());
      }
      else if (major) {
        if (m_immutable_cache->memory_used()==0 && m_stores.size() <= (size_t)1)
          HT_THROW(Error::OK, "");
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
          if (m_immutable_cache->memory_used() == 0)
            HT_THROW(Error::OK, "");
          tableidx = m_stores.size();
          HT_INFOF("Starting Minor Compaction of %s(%s)",
                   m_range_name.c_str(), m_name.c_str());
        }
      }
    }

  }
  catch (Exception &e) {
    ScopedLock lock(m_mutex);
    merge_caches();
    if (m_earliest_cached_revision_saved != TIMESTAMP_NULL) {
      m_earliest_cached_revision = m_earliest_cached_revision_saved;
      m_earliest_cached_revision_saved = TIMESTAMP_NULL;
    }
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
                            m_next_cs_id++);

    cellstore = new CellStoreV0(Global::dfs);
    size_t max_num_entries = 0;


    {
      ScopedLock lock(m_mutex);
      ScanContextPtr scan_context = new ScanContext(m_schema);

      if (m_in_memory) {
        MergeScanner *mscanner = new MergeScanner(scan_context, false);
        mscanner->add_scanner(m_immutable_cache->create_scanner(
                              scan_context));
        max_num_entries = m_cell_cache->size();
        if (m_immutable_cache) {
          max_num_entries += m_immutable_cache->size();
        }
        if(m_immutable_cache )
        scanner = mscanner;
      }
      else if (major || tableidx < m_stores.size()) {
        bool return_everything = (major) ? false : (tableidx > 0);
        MergeScanner *mscanner = new MergeScanner(scan_context,
                                                  return_everything);
        mscanner->add_scanner(m_immutable_cache->create_scanner(
                              scan_context));
        max_num_entries = m_cell_cache->size();
        if (m_immutable_cache) {
          max_num_entries += m_immutable_cache->size();
        }
        for (size_t i=tableidx; i<m_stores.size(); i++) {
          mscanner->add_scanner(m_stores[i]->create_scanner(
              scan_context));
          max_num_entries += boost::any_cast<uint32_t>
              (m_stores[i]->get_trailer()->get("total_entries"));
        }

        scanner = mscanner;
      }
      else {
        scanner = m_immutable_cache->create_scanner(scan_context);
        max_num_entries = m_cell_cache->size();
        if (m_immutable_cache) {
          max_num_entries += m_immutable_cache->size();
        }
      }
    }

    cellstore->create(cs_file.c_str(), max_num_entries, m_cellstore_props);

    while (scanner->get(key, value)) {
      cellstore->add(key, value);
      scanner->forward();
    }

    cellstore->finalize(&m_identifier);

    /**
     * Install new CellCache and CellStore and update Live file tracker
     */
    {
      ScopedLock lock(m_mutex);

      m_file_tracker.clear_live();

      if (m_in_memory) {
        merge_caches();
        m_stores.clear();
      }
      else {

        for (size_t i=0; i<tableidx; i++)
          m_file_tracker.add_live(m_stores[i]->get_filename());

        m_immutable_cache = 0;

        /** Drop the compacted tables from the table vector **/
        if (tableidx < m_stores.size())
          m_stores.resize(tableidx);
      }

      /** Add the new cell store to the table vector, or delete it if
       * it contains no entries
       */
      if (cellstore->get_total_entries() > 0) {
        m_stores.push_back(cellstore);
        m_file_tracker.add_live(cellstore->get_filename());
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

    }

    m_file_tracker.update_files_column();

    m_earliest_cached_revision_saved = TIMESTAMP_NULL;

    HT_INFOF("Finished Compaction of %s(%s)", m_range_name.c_str(),
             m_name.c_str());

  }
  catch (Exception &e) {
    ScopedLock lock(m_mutex);
    HT_ERROR_OUT << m_range_name << "(" << m_name << ") " << e << HT_END;
    merge_caches();
    if (m_earliest_cached_revision_saved != TIMESTAMP_NULL) {
      m_earliest_cached_revision = m_earliest_cached_revision_saved;
      m_earliest_cached_revision_saved = TIMESTAMP_NULL;
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
  CellCachePtr old_cell_cache = m_cell_cache;
  CellCachePtr new_cell_cache;
  ScanContextPtr scan_context = new ScanContext(m_schema);
  CellListScannerPtr cell_cache_scanner;
  ByteString key;
  ByteString value;
  Key key_comps;
  std::vector<CellStorePtr> new_stores;
  CellStore *new_cell_store;
  uint64_t memory_added = 0;
  uint64_t items_added = 0;
  int cmp;

  m_earliest_cached_revision_saved = m_earliest_cached_revision;
  m_earliest_cached_revision = TIMESTAMP_MAX;

  try {

    if (drop_high) {
      m_end_row = split_row;
      m_next_cs_id = 0;
    }
    else
      m_start_row = split_row;

    m_range_name = m_table_name + "[" + m_start_row + ".." + m_end_row + "]";

    m_file_tracker.change_range(m_start_row, m_end_row);

    new_cell_cache = new CellCache();
    new_cell_cache->lock();

    m_cell_cache = new_cell_cache;

    cell_cache_scanner = old_cell_cache->create_scanner(scan_context);

    /**
     * Shrink the CellCache
     */
    while (cell_cache_scanner->get(key_comps, value)) {

      cmp = strcmp(key_comps.row, split_row.c_str());

      if ((cmp > 0 && !drop_high) || (cmp <= 0 && drop_high)) {
        if (key_comps.revision < m_earliest_cached_revision)
          m_earliest_cached_revision = key_comps.revision;
        add(key_comps, value);
        memory_added += key.length() + value.length();
        items_added++;
      }
      cell_cache_scanner->forward();
    }

    new_cell_cache->unlock();

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
  catch (Exception &e) {
    m_cell_cache = old_cell_cache;
    m_earliest_cached_revision = m_earliest_cached_revision_saved;
    m_earliest_cached_revision_saved = TIMESTAMP_NULL;
    HT_THROW2(e.code(), e, "shrink failed");
  }
}



/**
 */
void AccessGroup::release_files(const std::vector<String> &files) {
  m_file_tracker.remove_references(files);
  m_file_tracker.update_files_column();
}


void AccessGroup::initiate_compaction() {
  ScopedLock lock(m_mutex);
  HT_ASSERT(!m_immutable_cache);
  m_immutable_cache = m_cell_cache;
  m_immutable_cache->freeze();
  m_cell_cache = new CellCache();
  m_earliest_cached_revision_saved = m_earliest_cached_revision;
  m_earliest_cached_revision = TIMESTAMP_NULL;
}



/**
 * Assumes mutex is locked
 */
void AccessGroup::merge_caches() {

  if (!m_immutable_cache)
    return;

  if (m_immutable_cache->size() == 0) {
    m_immutable_cache = 0;
    return;
  }

  Key key;
  ByteString value;
  CellCachePtr merged_cache = new CellCache();
  ScanContextPtr scan_context = new ScanContext(m_schema);
  CellListScannerPtr scanner = m_immutable_cache->create_scanner(scan_context);
  while (scanner->get(key, value)) {
    merged_cache->add(key, value);
    scanner->forward();
  }

  // Add cell cache
  if (m_cell_cache->size() > 0) {
    scanner = m_cell_cache->create_scanner(scan_context);
    while (scanner->get(key, value)) {
      merged_cache->add(key, value);
      scanner->forward();
    }
  }
  m_immutable_cache = 0;
  m_cell_cache = merged_cache;
}

