/** -*- c++ -*-
 * Copyright (C) 2009 Hypertable, Inc.
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
#include <cassert>
#include <string>
#include <vector>

extern "C" {
#include <poll.h>
#include <string.h>
}

#include <boost/algorithm/string.hpp>

#include "Common/Config.h"
#include "Common/Error.h"
#include "Common/FailureInducer.h"
#include "Common/FileUtils.h"
#include "Common/md5.h"
#include "Common/StringExt.h"

#include "Hypertable/Lib/CommitLog.h"
#include "Hypertable/Lib/CommitLogReader.h"

#include "CellStoreFactory.h"
#include "Global.h"
#include "Location.h"
#include "MergeScanner.h"
#include "MetadataNormal.h"
#include "MetadataRoot.h"
#include "Range.h"

using namespace Hypertable;
using namespace std;


Range::Range(MasterClientPtr &master_client,
             const TableIdentifier *identifier, SchemaPtr &schema,
             const RangeSpec *range, RangeSet *range_set,
             const RangeState *state)
  : m_scans(0), m_cells_scanned(0), m_cells_returned(0), m_cells_written(0),
    m_updates(0), m_bytes_scanned(0), m_bytes_returned(0), m_bytes_written(0),
    m_master_client(master_client), 
    m_schema(schema), m_revision(TIMESTAMP_MIN), m_latest_revision(TIMESTAMP_MIN),
    m_split_off_high(false), m_added_inserts(0), m_range_set(range_set),
    m_error(Error::OK), m_dropped(false), m_capacity_exceeded_throttle(false),
    m_relinquish(false), m_maintenance_generation(0),
    m_load_metrics(identifier->id, range->start_row, range->end_row) {
  m_metalog_entity = new MetaLog::EntityRange(*identifier, *range, *state);
  initialize();
}

Range::Range(MasterClientPtr &master_client, SchemaPtr &schema,
             MetaLog::EntityRange *range_entity, RangeSet *range_set)
  : m_scans(0), m_cells_scanned(0), m_cells_returned(0), m_cells_written(0),
    m_updates(0), m_bytes_scanned(0), m_bytes_returned(0), m_bytes_written(0),
    m_master_client(master_client), m_metalog_entity(range_entity),
    m_schema(schema), m_revision(TIMESTAMP_MIN), m_latest_revision(TIMESTAMP_MIN),
    m_split_off_high(false), m_added_inserts(0), m_range_set(range_set),
    m_error(Error::OK), m_dropped(false), m_capacity_exceeded_throttle(false),
    m_relinquish(false), m_maintenance_generation(0),
    m_load_metrics(range_entity->table.id, range_entity->spec.start_row, range_entity->spec.end_row) {
  initialize();
}

void Range::initialize() {
  AccessGroup *ag;

  memset(m_added_deletes, 0, 3*sizeof(int64_t));

  if (m_metalog_entity->state.soft_limit == 0 || m_metalog_entity->state.soft_limit > (uint64_t)Global::range_split_size)
    m_metalog_entity->state.soft_limit = Global::range_split_size;

  /**
   * Determine split side
   */
  if (m_metalog_entity->state.state == RangeState::SPLIT_LOG_INSTALLED ||
      m_metalog_entity->state.state == RangeState::SPLIT_SHRUNK) {
    int cmp = strcmp(m_metalog_entity->state.split_point, m_metalog_entity->state.old_boundary_row);
    if (cmp < 0)
      m_split_off_high = true;
    else
      HT_ASSERT(cmp > 0);
  }
  else {
    String split_off = Config::get_str("Hypertable.RangeServer.Range.SplitOff");
    if (split_off == "high")
      m_split_off_high = true;
    else
      HT_ASSERT(split_off == "low");
  }

  if (m_metalog_entity->state.state == RangeState::SPLIT_LOG_INSTALLED)
    split_install_log_rollback_metadata();

  m_name = format("%s[%s..%s]", m_metalog_entity->table.id,
                  m_metalog_entity->spec.start_row,
                  m_metalog_entity->spec.end_row);

  m_is_root = (m_metalog_entity->table.is_metadata() &&
               (m_metalog_entity->spec.start_row==0 || *m_metalog_entity->spec.start_row==0) &&
               !strcmp(m_metalog_entity->spec.end_row, Key::END_ROOT_ROW));

  m_column_family_vector.resize(m_schema->get_max_column_family_id() + 1);

  foreach(Schema::AccessGroup *sag, m_schema->get_access_groups()) {
    ag = new AccessGroup(&m_metalog_entity->table, m_schema, sag, &m_metalog_entity->spec);
    m_access_group_map[sag->name] = ag;
    m_access_group_vector.push_back(ag);

    foreach(Schema::ColumnFamily *scf, sag->columns)
      m_column_family_vector[scf->id] = ag;
  }

  if (m_is_root) {
    MetadataRoot metadata(m_schema);
    load_cell_stores(&metadata);
  }
  else {
    MetadataNormal metadata(&m_metalog_entity->table, m_metalog_entity->spec.end_row);
    load_cell_stores(&metadata);
  }

  HT_DEBUG_OUT << "Range object for " << m_name << " constructed\n"
               << &m_metalog_entity->state << HT_END;
}


void Range::split_install_log_rollback_metadata() {

  try {
    String metadata_key_str;
    KeySpec key;

    TableMutatorPtr mutator = Global::metadata_table->create_mutator();

    // Reset start row
    metadata_key_str = String("") + m_metalog_entity->table.id + ":" + m_metalog_entity->spec.end_row;
    key.row = metadata_key_str.c_str();
    key.row_len = metadata_key_str.length();
    key.column_qualifier = 0;
    key.column_qualifier_len = 0;
    key.column_family = "StartRow";
    mutator->set(key, (uint8_t *)m_metalog_entity->spec.start_row,
                 strlen(m_metalog_entity->spec.start_row));

    // Get rid of new range
    metadata_key_str = format("%s:%s", m_metalog_entity->table.id, m_metalog_entity->state.split_point);
    key.row = metadata_key_str.c_str();
    key.row_len = metadata_key_str.length();
    key.column_qualifier = 0;
    key.column_qualifier_len = 0;
    key.column_family = 0;
    mutator->set_delete(key);

    mutator->flush();

  }
  catch (Hypertable::Exception &e) {
    // TODO: propagate exception
    HT_ERROR_OUT << "Problem rolling back Range from SPLIT_LOG_INSTALLED state " << e << HT_END;
    HT_ABORT;
  }

}


/**
 *
 */
void Range::update_schema(SchemaPtr &schema) {
  ScopedLock lock(m_schema_mutex);

  vector<Schema::AccessGroup*> new_access_groups;
  AccessGroup *ag;
  AccessGroupMap::iterator ag_iter;
  size_t max_column_family_id = schema->get_max_column_family_id();

  // only update schema if there is more recent version
  if(schema->get_generation() <= m_schema->get_generation())
    return;

  // resize column family vector if needed
  if (max_column_family_id > m_column_family_vector.size()-1)
    m_column_family_vector.resize(max_column_family_id+1);

  // update all existing access groups & create new ones as needed
  foreach(Schema::AccessGroup *s_ag, schema->get_access_groups()) {
    if( (ag_iter = m_access_group_map.find(s_ag->name)) !=
        m_access_group_map.end()) {
      ag_iter->second->update_schema(schema, s_ag);
      foreach(Schema::ColumnFamily *s_cf, s_ag->columns) {
        if (s_cf->deleted == false)
          m_column_family_vector[s_cf->id] = ag_iter->second;
      }
    }
    else {
      new_access_groups.push_back(s_ag);
    }
  }

  // create new access groups
  foreach(Schema::AccessGroup *s_ag, new_access_groups) {
    ag = new AccessGroup(&m_metalog_entity->table, schema, s_ag, &m_metalog_entity->spec);
    m_access_group_map[s_ag->name] = ag;
    m_access_group_vector.push_back(ag);

    foreach(Schema::ColumnFamily *s_cf, s_ag->columns) {
      if (s_cf->deleted == false)
        m_column_family_vector[s_cf->id] = ag;
    }
  }

  // TODO: remove deleted access groups
  m_schema = schema;
  return;
}

/**
 *
 */
void Range::load_cell_stores(Metadata *metadata) {
  AccessGroup *ag;
  CellStorePtr cellstore;
  const char *base, *ptr, *end;
  std::vector<String> csvec;
  String ag_name;
  String files;
  String file_str;
  bool need_update;

  metadata->reset_files_scan();

  while (metadata->get_next_files(ag_name, files)) {
    csvec.clear();
    need_update = false;

    if ((ag = m_access_group_map[ag_name]) == 0) {
      HT_ERRORF("Unrecognized access group name '%s' found in METADATA for "
                "table '%s'", ag_name.c_str(), m_metalog_entity->table.id);
      HT_ABORT;
    }

    ptr = base = (const char *)files.c_str();
    end = base + strlen(base);
    while (ptr < end) {

      while (*ptr != ';' && ptr < end)
        ptr++;

      file_str = String(base, ptr-base);
      boost::trim(file_str);

      if (!file_str.empty()) {
        if (file_str[0] == '#') {
          ++ptr;
          base = ptr;
          need_update = true;
          continue;
        }

        csvec.push_back(file_str);
      }
      ++ptr;
      base = ptr;
    }

    files = "";

    String file_basename = Global::toplevel_dir + "/tables/";

    bool skip_not_found = Config::properties->get_bool("Hypertable.RangeServer.CellStore.SkipNotFound");

    for (size_t i=0; i<csvec.size(); i++) {

      files += csvec[i] + ";\n";

      HT_INFOF("Loading CellStore %s", csvec[i].c_str());

      try {
        cellstore = CellStoreFactory::open(file_basename + csvec[i], m_metalog_entity->spec.start_row, m_metalog_entity->spec.end_row);
      }
      catch (Exception &e) {
        if (e.code() == Error::DFSBROKER_FILE_NOT_FOUND && skip_not_found) {
          HT_WARNF("CellStore file '%s' not found, skipping", csvec[i].c_str());
          continue;
        }
        HT_FATALF("Problem opening CellStore file '%s' - %s", csvec[i].c_str(),
                  Error::get_text(e.code()));
      }

      int64_t revision = boost::any_cast<int64_t>
        (cellstore->get_trailer()->get("revision"));
      if (revision > m_latest_revision)
        m_latest_revision = revision;

      ag->add_cell_store(cellstore);
    }

    /** this causes startup deadlock (and is not needed) ..
    if (need_update)
      metadata->write_files(ag_name, files);
    */

  }

}


/**
 * This method must not fail.  The caller assumes that it will succeed.
 */
void Range::add(const Key &key, const ByteString value) {
  HT_DEBUG_OUT <<"key="<< key <<" value='";
  const uint8_t *p;
  size_t len = value.decode_length(&p);
  _out_ << format_bytes(20, p, len) << HT_END;

  if (key.flag == FLAG_DELETE_ROW) {
    for (size_t i=0; i<m_access_group_vector.size(); ++i)
      m_access_group_vector[i]->add(key, value);
  }
  else
    m_column_family_vector[key.column_family_code]->add(key, value);

  if (key.flag == FLAG_INSERT)
    m_added_inserts++;
  else
    m_added_deletes[key.flag]++;

  if (key.revision > m_revision)
    m_revision = key.revision;
}


CellListScanner *Range::create_scanner(ScanContextPtr &scan_ctx) {
  bool return_deletes = scan_ctx->spec ? scan_ctx->spec->return_deletes : false;
  MergeScanner *mscanner = new MergeScanner(scan_ctx, return_deletes);
  AccessGroupVector  ag_vector(0);

  {
    ScopedLock lock(m_schema_mutex);
    ag_vector = m_access_group_vector;
    m_scans++;
  }

  try {
    for (size_t i=0; i<ag_vector.size(); ++i) {
      if (ag_vector[i]->include_in_scan(scan_ctx))
        mscanner->add_scanner(ag_vector[i]->create_scanner(scan_ctx));
    }
  }
  catch (Exception &e) {
    delete mscanner;
    HT_THROW2(e.code(), e, "");
  }

  // increment #scanners
  return mscanner;
}


uint64_t Range::disk_usage() {
  ScopedLock lock(m_schema_mutex);
  uint64_t usage = 0;
  for (size_t i=0; i<m_access_group_vector.size(); i++)
    usage += m_access_group_vector[i]->disk_usage();
  return usage;
}

bool Range::need_maintenance() {
  ScopedLock lock(m_schema_mutex);
  bool needed = false;
  int64_t mem, disk, disk_total = 0;
  for (size_t i=0; i<m_access_group_vector.size(); ++i) {
    m_access_group_vector[i]->space_usage(&mem, &disk);
    disk_total += disk;
    if (mem >= Global::access_group_max_mem)
      needed = true;
  }
  if (m_metalog_entity->table.is_metadata()) {
    if (Global::range_metadata_split_size != 0 &&
        disk_total >= (int64_t)Global::range_metadata_split_size)
      needed = true;
  }
  else if (disk_total >= Global::range_split_size) {
    needed = true;
  }
  return needed;
}


bool Range::cancel_maintenance() {
  return m_dropped ? true : false;
}


Range::MaintenanceData *Range::get_maintenance_data(ByteArena &arena, time_t now,
                                                    TableMutator *mutator) {
  MaintenanceData *mdata = (MaintenanceData *)arena.alloc( sizeof(MaintenanceData) );
  AccessGroup::MaintenanceData **tailp = 0;
  AccessGroupVector  ag_vector(0);
  uint64_t size=0;
  int64_t starting_maintenance_generation;

  memset(mdata, 0, sizeof(MaintenanceData));

  {
    ScopedLock lock(m_schema_mutex);
    ag_vector = m_access_group_vector;
    mdata->scans = m_scans;
    mdata->updates = m_updates;
    mdata->bytes_written = m_bytes_written;
    mdata->cells_written = m_cells_written;
  }

  mdata->range = this;
  mdata->table_id = m_metalog_entity->table.id;
  mdata->is_metadata = m_metalog_entity->table.is_metadata();
  mdata->is_system = m_metalog_entity->table.is_system();
  mdata->relinquish = m_relinquish;
  mdata->schema_generation = m_metalog_entity->table.generation;

  // record starting maintenance generation
  {
    ScopedLock lock(m_mutex);
    starting_maintenance_generation = m_maintenance_generation;
    mdata->cells_scanned = m_cells_scanned;
    mdata->cells_returned = m_cells_returned;
    mdata->bytes_scanned = m_bytes_scanned;
    mdata->bytes_returned = m_bytes_returned;
    mdata->state = m_metalog_entity->state.state;
  }

  for (size_t i=0; i<ag_vector.size(); i++) {
    if (mdata->agdata == 0) {
      mdata->agdata = ag_vector[i]->get_maintenance_data(arena, now);
      tailp = &mdata->agdata;
    }
    else {
      (*tailp)->next = ag_vector[i]->get_maintenance_data(arena, now);
      tailp = &(*tailp)->next;
    }
    size += (*tailp)->disk_estimate;
    mdata->disk_used += (*tailp)->disk_used;
    mdata->compression_ratio += (*tailp)->compression_ratio;
    mdata->disk_estimate += (*tailp)->disk_estimate;
    mdata->memory_used += (*tailp)->mem_used;
    mdata->memory_allocated += (*tailp)->mem_allocated;
    mdata->block_index_memory += (*tailp)->block_index_memory;
    mdata->bloom_filter_memory += (*tailp)->bloom_filter_memory;
    mdata->bloom_filter_accesses += (*tailp)->bloom_filter_accesses;
    mdata->bloom_filter_maybes += (*tailp)->bloom_filter_maybes;
    mdata->bloom_filter_fps += (*tailp)->bloom_filter_fps;
    mdata->shadow_cache_memory += (*tailp)->shadow_cache_memory;
    mdata->cell_count += (*tailp)->cell_count;
  }

  if (ag_vector.size() > 0)
    mdata->compression_ratio /= ag_vector.size();

  if (tailp)
    (*tailp)->next = 0;

  if (size > (uint64_t)Global::range_maximum_size) {
    ScopedLock lock(m_mutex);
    if (starting_maintenance_generation == m_maintenance_generation)
      m_capacity_exceeded_throttle = true;
  }

  mdata->busy = m_maintenance_guard.in_progress();

  if (mutator)
    m_load_metrics.compute_and_store(mutator, now,
                                     mdata->disk_used, mdata->memory_used,
                                     mdata->scans, mdata->updates,
                                     mdata->cells_scanned, mdata->cells_written,
                                     mdata->bytes_scanned, mdata->bytes_written);

  return mdata;
}


void Range::relinquish() {
  RangeMaintenanceGuard::Activator activator(m_maintenance_guard);

  try {
    switch (m_metalog_entity->state.state) {
    case (RangeState::STEADY):
      relinquish_install_log();
    case (RangeState::RELINQUISH_LOG_INSTALLED):
      relinquish_compact_and_finish();
    }
  }
  catch (Exception &e) {
    if (e.code() == Error::CANCELLED || cancel_maintenance())
      return;
    throw;
  }

  {
    ScopedLock lock(m_mutex);
    m_capacity_exceeded_throttle = false;
    m_maintenance_generation++;
  }

  HT_INFO("Relinquish Complete.");
}


void Range::relinquish_install_log() {
  char md5DigestStr[33];
  String logname;
  time_t now = 0;
  AccessGroupVector  ag_vector(0);

  {
    ScopedLock lock(m_schema_mutex);
    ag_vector = m_access_group_vector;
  }

  if (cancel_maintenance())
    HT_THROW(Error::CANCELLED, "");

  /**
   * Create transfer log
   */
  md5_trunc_modified_base64(m_metalog_entity->spec.end_row, md5DigestStr);
  md5DigestStr[16] = 0;

  try {

    do {
      if (now != 0)
        poll(0, 0, 1200);
      now = time(0);
      m_metalog_entity->state.set_transfer_log(Global::log_dir + "/" + m_metalog_entity->table.id + "/" + md5DigestStr + "-" + (int)now);
    }
    while (Global::log_dfs->exists(m_metalog_entity->state.transfer_log));

    Global::log_dfs->mkdirs(m_metalog_entity->state.transfer_log);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << "Problem creating transfer log directory '"
                 << m_metalog_entity->state.transfer_log << "' - " << e << HT_END;
    HT_ABORT;
  }

  /**
   * Write RelinquishStart MetaLog entry
   */
  m_metalog_entity->state.state = RangeState::RELINQUISH_LOG_INSTALLED;
  for (int i=0; true; i++) {
    try {
      /**  TBD **/
      //Global::range_log->log_relinquish_start(m_metalog_entity->table, m_metalog_entity->spec);
      break;
    }
    catch (Exception &e) {
      if (i<3) {
        HT_WARNF("%s - %s", Error::get_text(e.code()), e.what());
        poll(0, 0, 5000);
        continue;
      }
      HT_ERRORF("Problem writing RelinquishStart meta log entry for %s",
                m_name.c_str());
      HT_FATAL_OUT << e << HT_END;
    }
  }

  /**
   * Create and install the transfer log
   */
  {
    Barrier::ScopedActivator block_updates(m_update_barrier);
    ScopedLock lock(m_mutex);
    m_transfer_log = new CommitLog(Global::dfs, m_metalog_entity->state.transfer_log);
    for (size_t i=0; i<ag_vector.size(); i++)
      ag_vector[i]->stage_compaction();
  }

}


void Range::relinquish_compact_and_finish() {
  AccessGroupVector ag_vector(0);

  {
    ScopedLock lock(m_schema_mutex);
    ag_vector = m_access_group_vector;
  }

  if (cancel_maintenance())
    HT_THROW(Error::CANCELLED, "");

  /**
   * Perform major compactions
   */
  for (size_t i=0; i<ag_vector.size(); i++)
    ag_vector[i]->run_compaction(MaintenanceFlag::COMPACT_MINOR);

  // update the latest generation, this should probably be protected
  {
    ScopedLock lock(m_schema_mutex);
    m_metalog_entity->table.generation = m_schema->get_generation();
  }

  m_dropped = true;

  // Remove range from the system
  {
    Barrier::ScopedActivator block_updates(m_update_barrier);
    Barrier::ScopedActivator block_scans(m_scan_barrier);

    if (!m_range_set->remove(m_metalog_entity->spec.end_row)) {
      HT_ERROR_OUT << "Problem removing range " << m_name << HT_END;
      HT_ABORT;
    }
  }

  // Record "move" in sys/RS_METRICS
  if (Global::rs_metrics_table) {
    TableMutatorPtr mutator = Global::rs_metrics_table->create_mutator();
    KeySpec key;
    String row = Location::get() + ":" + m_metalog_entity->table.id;
    key.row = row.c_str();
    key.row_len = row.length();
    key.column_family = "range_move";
    key.column_qualifier = m_metalog_entity->spec.end_row;
    key.column_qualifier_len = strlen(m_metalog_entity->spec.end_row);
    try {
      mutator->set(key, 0, 0);
      mutator->flush();
    }
    catch (Exception &e) {
      HT_ERROR_OUT << "Problem updating sys/RS_METRICS - " << e << HT_END;
    }
  }

  HT_INFOF("Reporting relinquished range %s[%s..%s] to Master",
           m_metalog_entity->table.id, m_metalog_entity->spec.start_row,
           m_metalog_entity->spec.end_row);

  m_master_client->move_range(&m_metalog_entity->table, m_metalog_entity->spec,
                              m_metalog_entity->state.transfer_log,
                              m_metalog_entity->state.soft_limit, false);

  /**
   * Write RelinquishDone MetaLog entry
   */
  for (int i=0; true; i++) {
    try {
      // TBD
      //Global::range_log->log_relinquish_done(m_metalog_entity->table, m_metalog_entity->spec, m_metalog_entity->state);
      break;
    }
    catch (Exception &e) {
      if (i<2) {
        HT_ERRORF("%s - %s", Error::get_text(e.code()), e.what());
        poll(0, 0, 5000);
        continue;
      }
      HT_ERRORF("Problem writing RelinquishDone meta log entry for %s",
                m_name.c_str());
      HT_FATAL_OUT << e << HT_END;
    }
  }

  // Acknowledge write of the RelinquishDone RSML entry to the master
  try {
    m_master_client->relinquish_acknowledge(&m_metalog_entity->table, 
                                            m_metalog_entity->spec);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << "Master::relinquish_acknowledge() error - " << e << HT_END;
  }

}



void Range::split() {
  RangeMaintenanceGuard::Activator activator(m_maintenance_guard);
  String old_start_row;

  HT_ASSERT(!m_is_root);

  try {

    switch (m_metalog_entity->state.state) {

    case (RangeState::STEADY):
      split_install_log();

    case (RangeState::SPLIT_LOG_INSTALLED):
      split_compact_and_shrink();

    case (RangeState::SPLIT_SHRUNK):
      split_notify_master();

    }

  }
  catch (Exception &e) {
    if (e.code() == Error::CANCELLED || cancel_maintenance())
      return;
    throw;
  }

  {
    ScopedLock lock(m_mutex);
    m_capacity_exceeded_throttle = false;
    m_maintenance_generation++;
  }

  HT_INFOF("Split Complete.  New Range end_row=%s", m_metalog_entity->spec.start_row);
}



/**
 */
void Range::split_install_log() {
  std::vector<String> split_rows;
  char md5DigestStr[33];
  AccessGroupVector  ag_vector(0);

  {
    ScopedLock lock(m_schema_mutex);
    ag_vector = m_access_group_vector;
  }

  if (cancel_maintenance())
    HT_THROW(Error::CANCELLED, "");

  for (size_t i=0; i<ag_vector.size(); i++)
    ag_vector[i]->get_split_rows(split_rows, false);

  /**
   * If we didn't get at least one row from each Access Group, then try again
   * the hard way (scans CellCache for middle row)
   */

  if (split_rows.size() < ag_vector.size()) {
    for (size_t i=0; i<ag_vector.size(); i++)
      ag_vector[i]->get_split_rows(split_rows, true);
  }
  sort(split_rows.begin(), split_rows.end());

  //HT_INFO_OUT << "thelma Dumping split rows for range " << m_name << HT_END;
  //for (size_t i=0; i<split_rows.size(); i++)
  //  HT_INFO_OUT << "thelma Range::get_split_row [" << i << "] = " << split_rows[i]
  //              << HT_END;
  //HT_INFO_OUT << "thelma Done calculating split rows for range " << m_name << HT_END;

  /**
   * If we still didn't get a good split row, try again the *really* hard way
   * by collecting all of the cached rows, sorting them and then taking the
   * middle.
   */
  if (split_rows.size() > 0) {
    ScopedLock lock(m_mutex);
    m_split_row = split_rows[split_rows.size()/2];
    if (strcmp(m_split_row.c_str(), m_metalog_entity->spec.start_row) < 0 ||
        strcmp(m_split_row.c_str(), m_metalog_entity->spec.end_row) >= 0) {
      split_rows.clear();
      for (size_t i=0; i<ag_vector.size(); i++)
        ag_vector[i]->get_cached_rows(split_rows);
      if (split_rows.size() > 0) {
        sort(split_rows.begin(), split_rows.end());
        m_split_row = split_rows[split_rows.size()/2];
        if (strcmp(m_split_row.c_str(), m_metalog_entity->spec.start_row) < 0 ||
            strcmp(m_split_row.c_str(), m_metalog_entity->spec.end_row) >= 0) {
          m_error = Error::RANGESERVER_ROW_OVERFLOW;
          HT_THROWF(Error::RANGESERVER_ROW_OVERFLOW,
                    "(a) Unable to determine split row for range %s[%s..%s]",
                    m_metalog_entity->table.id, m_metalog_entity->spec.start_row, m_metalog_entity->spec.end_row);
        }
      }
      else {
        m_error = Error::RANGESERVER_ROW_OVERFLOW;
        HT_THROWF(Error::RANGESERVER_ROW_OVERFLOW,
                  "(b) Unable to determine split row for range %s[%s..%s]",
                   m_metalog_entity->table.id, m_metalog_entity->spec.start_row, m_metalog_entity->spec.end_row);
      }
    }
  }
  else {
    m_error = Error::RANGESERVER_ROW_OVERFLOW;
    HT_THROWF(Error::RANGESERVER_ROW_OVERFLOW,
              "(c) Unable to determine split row for range %s[%s..%s]",
              m_metalog_entity->table.id, m_metalog_entity->spec.start_row, m_metalog_entity->spec.end_row);
  }

  m_metalog_entity->state.set_split_point(m_split_row);

  /**
   * Create split (transfer) log
   */
  md5_trunc_modified_base64(m_metalog_entity->state.split_point, md5DigestStr);
  md5DigestStr[16] = 0;
  m_metalog_entity->state.set_transfer_log(Global::log_dir + "/" + m_metalog_entity->table.id + "/" + md5DigestStr);

  // Create transfer log dir
  try {
    Global::log_dfs->rmdir(m_metalog_entity->state.transfer_log);
    Global::log_dfs->mkdirs(m_metalog_entity->state.transfer_log);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << "Problem creating log directory '%s' - " << e << HT_END;
    HT_ABORT;
  }

  if (m_split_off_high)
    m_metalog_entity->state.set_old_boundary_row(m_metalog_entity->spec.end_row);
  else
    m_metalog_entity->state.set_old_boundary_row(m_metalog_entity->spec.start_row);

  /**
   * Write SPLIT_START MetaLog entry
   */
  m_metalog_entity->state.state = RangeState::SPLIT_LOG_INSTALLED;
  for (int i=0; true; i++) {
    try {
      //Global::range_log->log_split_start(m_metalog_entity->table, m_metalog_entity->spec, m_metalog_entity->state);
      Global::rsml_writer->record_state(m_metalog_entity.get());
      break;
    }
    catch (Exception &e) {
      if (i<3) {
        HT_WARNF("%s - %s", Error::get_text(e.code()), e.what());
        poll(0, 0, 5000);
        continue;
      }
      HT_ERRORF("Problem writing SplitStart meta log entry for %s "
                "split-point='%s'", m_name.c_str(), m_metalog_entity->state.split_point);
      HT_FATAL_OUT << e << HT_END;
    }
  }

  /**
   * Create and install the transfer log
   */
  {
    Barrier::ScopedActivator block_updates(m_update_barrier);
    ScopedLock lock(m_mutex);
    for (size_t i=0; i<ag_vector.size(); i++)
      ag_vector[i]->stage_compaction();
    m_transfer_log = new CommitLog(Global::dfs, m_metalog_entity->state.transfer_log);
  }

  HT_MAYBE_FAIL("split-1");
  HT_MAYBE_FAIL_X("metadata-split-1", m_metalog_entity->table.is_metadata());

}


void Range::split_compact_and_shrink() {
  int error;
  String old_start_row = m_metalog_entity->spec.start_row;
  String old_end_row = m_metalog_entity->spec.end_row;
  AccessGroupVector  ag_vector(0);

  {
    ScopedLock lock(m_schema_mutex);
    ag_vector = m_access_group_vector;
  }

  if (cancel_maintenance())
    HT_THROW(Error::CANCELLED, "");

  /**
   * Perform major compactions
   */
  for (size_t i=0; i<ag_vector.size(); i++)
    ag_vector[i]->run_compaction(MaintenanceFlag::COMPACT_MAJOR|MaintenanceFlag::SPLIT);

  try {
    String files;
    String metadata_key_str;
    KeySpec key;

    TableMutatorPtr mutator = Global::metadata_table->create_mutator();

    // For new range with existing end row, update METADATA entry with new
    // 'StartRow' column.
    metadata_key_str = String("") + m_metalog_entity->table.id + ":" + m_metalog_entity->spec.end_row;
    key.row = metadata_key_str.c_str();
    key.row_len = metadata_key_str.length();
    key.column_qualifier = 0;
    key.column_qualifier_len = 0;
    key.column_family = "StartRow";
    mutator->set(key, (uint8_t *)m_metalog_entity->state.split_point,
                 strlen(m_metalog_entity->state.split_point));
    if (m_split_off_high) {
      key.column_family = "Files";
      for (size_t i=0; i<ag_vector.size(); i++) {
        key.column_qualifier = ag_vector[i]->get_name();
        key.column_qualifier_len = strlen(ag_vector[i]->get_name());
        ag_vector[i]->get_file_list(files, false);
        if (files != "")
          mutator->set(key, (uint8_t *)files.c_str(), files.length());
      }
    }

    // For new range whose end row is the split point, create a new METADATA
    // entry
    metadata_key_str = format("%s:%s", m_metalog_entity->table.id, m_metalog_entity->state.split_point);
    key.row = metadata_key_str.c_str();
    key.row_len = metadata_key_str.length();
    key.column_qualifier = 0;
    key.column_qualifier_len = 0;

    key.column_family = "StartRow";
    mutator->set(key, old_start_row.c_str(), old_start_row.length());

    key.column_family = "Files";
    for (size_t i=0; i<ag_vector.size(); i++) {
      key.column_qualifier = ag_vector[i]->get_name();
      key.column_qualifier_len = strlen(ag_vector[i]->get_name());
      ag_vector[i]->get_file_list(files, m_split_off_high);
      if (files != "")
        mutator->set(key, (uint8_t *)files.c_str(), files.length());
    }
    if (m_split_off_high) {
      key.column_qualifier = 0;
      key.column_qualifier_len = 0;
      key.column_family = "Location";
      String location = Location::get();
      mutator->set(key, location.c_str(), location.length());
    }

    mutator->flush();

  }
  catch (Hypertable::Exception &e) {
    // TODO: propagate exception
    HT_ERROR_OUT <<"Problem updating METADATA after split (new_end="
        << m_metalog_entity->state.split_point <<", old_end="<< m_metalog_entity->spec.end_row <<") "<< e << HT_END;
    // need to unblock updates and then return error
    HT_ABORT;
  }

  /**
   *  Shrink the range
   */
  {
    Barrier::ScopedActivator block_updates(m_update_barrier);
    Barrier::ScopedActivator block_scans(m_scan_barrier);

    // Shrink access groups
    if (m_split_off_high) {
      if (!m_range_set->change_end_row(m_metalog_entity->spec.end_row, m_metalog_entity->state.split_point)) {
        HT_ERROR_OUT << "Problem changing end row of range " << m_name
                     << " to " << m_metalog_entity->state.split_point << HT_END;
        HT_ABORT;
      }
    }
    {
      ScopedLock lock(m_mutex);
      String split_row = m_metalog_entity->state.split_point;

      // Shrink access groups
      if (m_split_off_high)
        m_metalog_entity->spec.set_end_row(m_metalog_entity->state.split_point);
      else
        m_metalog_entity->spec.set_start_row(m_metalog_entity->state.split_point);

      m_load_metrics.change_rows(m_metalog_entity->spec.start_row, m_metalog_entity->spec.end_row);

      m_name = String(m_metalog_entity->table.id)+"["+m_metalog_entity->spec.start_row+".."+m_metalog_entity->spec.end_row+"]";
      m_split_row = "";
      for (size_t i=0; i<ag_vector.size(); i++)
        ag_vector[i]->shrink(split_row, m_split_off_high);

      // Close and uninstall split log
      if ((error = m_transfer_log->close()) != Error::OK) {
        HT_ERRORF("Problem closing split log '%s' - %s",
                  m_transfer_log->get_log_dir().c_str(), Error::get_text(error));
      }
      m_transfer_log = 0;
    }
  }

  /**
   * Write SplitShrunk MetaLog entry
   */
  m_metalog_entity->state.state = RangeState::SPLIT_SHRUNK;
  if (m_split_off_high) {
    /** Create DFS directories for this range **/
    {
      char md5DigestStr[33];
      String table_dir, range_dir;

      md5_trunc_modified_base64(m_metalog_entity->spec.end_row, md5DigestStr);
      md5DigestStr[16] = 0;
      table_dir = Global::toplevel_dir + "/tables/" + m_metalog_entity->table.id;

      {
        ScopedLock lock(m_schema_mutex);
        foreach(Schema::AccessGroup *ag, m_schema->get_access_groups()) {
          // notice the below variables are different "range" vs. "table"
          range_dir = table_dir + "/" + ag->name + "/" + md5DigestStr;
          Global::dfs->mkdirs(range_dir);
        }
      }
    }

  }

  for (int i=0; true; i++) {
    try {
      //Global::range_log->log_split_shrunk(m_metalog_entity->table, m_metalog_entity->spec, m_metalog_entity->state);
      Global::rsml_writer->record_state(m_metalog_entity.get());
      break;
    }
    catch (Exception &e) {
      if (i<3) {
        HT_ERRORF("%s - %s", Error::get_text(e.code()), e.what());
        poll(0, 0, 5000);
        continue;
      }
      HT_ERRORF("Problem writing SplitShrunk meta log entry for %s "
                "split-point='%s'", m_name.c_str(), m_metalog_entity->state.split_point);
      HT_FATAL_OUT << e << HT_END;
    }
  }

  HT_MAYBE_FAIL("split-2");
  HT_MAYBE_FAIL_X("metadata-split-2", m_metalog_entity->table.is_metadata());

}


void Range::split_notify_master() {
  RangeSpec range;
  int64_t soft_limit = (int64_t)m_metalog_entity->state.soft_limit;

  if (cancel_maintenance())
    HT_THROW(Error::CANCELLED, "");

  if (m_split_off_high) {
    range.start_row = m_metalog_entity->spec.end_row;
    range.end_row = m_metalog_entity->state.old_boundary_row;
  }
  else {
    range.start_row = m_metalog_entity->state.old_boundary_row;
    range.end_row = m_metalog_entity->spec.start_row;
  }

  // update the latest generation, this should probably be protected
  {
    ScopedLock lock(m_schema_mutex);
    m_metalog_entity->table.generation = m_schema->get_generation();
  }

  HT_INFOF("Reporting newly split off range %s[%s..%s] to Master",
           m_metalog_entity->table.id, range.start_row, range.end_row);

  if (soft_limit < Global::range_split_size) {
    soft_limit *= 2;
    if (soft_limit > Global::range_split_size)
      soft_limit = Global::range_split_size;
  }

  m_master_client->move_range(&m_metalog_entity->table, range,
                              m_metalog_entity->state.transfer_log,
                              soft_limit, true);

  /**
   * NOTE: try the following crash and make sure that the master does
   * not try to load the range twice.
   */

  HT_MAYBE_FAIL("split-3");
  HT_MAYBE_FAIL_X("metadata-split-3", m_metalog_entity->table.is_metadata());

  String split_point = m_metalog_entity->state.split_point;

  m_metalog_entity->state.clear();
  m_metalog_entity->state.soft_limit = soft_limit;

  /**
   * Write SplitDone MetaLog entry
   */
  for (int i=0; true; i++) {
    try {
      //Global::range_log->log_split_done(m_metalog_entity->table, m_metalog_entity->spec, m_metalog_entity->state);
      Global::rsml_writer->record_state(m_metalog_entity.get());
      break;
    }
    catch (Exception &e) {
      if (i<2) {
        HT_ERRORF("%s - %s", Error::get_text(e.code()), e.what());
        poll(0, 0, 5000);
        continue;
      }
      HT_ERRORF("Problem writing SplitDone meta log entry for %s "
                "split-point='%s'", m_name.c_str(), split_point.c_str());
      HT_FATAL_OUT << e << HT_END;
    }
  }

  // Acknowledge write of the SplitDone RSML entry to the master
  try {
    m_master_client->relinquish_acknowledge(&m_metalog_entity->table, range);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << "Master::relinquish_acknowledge() error - " << e << HT_END;
  }

  HT_MAYBE_FAIL("split-4");
  HT_MAYBE_FAIL_X("metadata-split-4", m_metalog_entity->table.is_metadata());

}


void Range::compact(MaintenanceFlag::Map &subtask_map) {
  RangeMaintenanceGuard::Activator activator(m_maintenance_guard);
  AccessGroupVector ag_vector(0);
  int flags;

  {
    ScopedLock lock(m_schema_mutex);
    ag_vector = m_access_group_vector;
  }

  try {

    // Initiate minor compactions (freeze cell cache)
    {
      Barrier::ScopedActivator block_updates(m_update_barrier);
      ScopedLock lock(m_mutex);
      for (size_t i=0; i<ag_vector.size(); i++) {
	if (subtask_map.minor_compaction(ag_vector[i].get()))
	  ag_vector[i]->stage_compaction();
      }
    }

    // do compactions
    for (size_t i=0; i<ag_vector.size(); i++) {
      flags = subtask_map.flags(ag_vector[i].get());
      if (flags & MaintenanceFlag::COMPACT) {
	try {
	  ag_vector[i]->run_compaction(flags);
	}
	catch (Exception &e) {
	  ag_vector[i]->unstage_compaction();
	}
      }
    }

  }
  catch (Exception &e) {
    if (e.code() == Error::CANCELLED || cancel_maintenance())
      return;
    throw;
  }

  {
    ScopedLock lock(m_mutex);
    m_capacity_exceeded_throttle = false;
    m_maintenance_generation++;
  }
}



void Range::purge_memory(MaintenanceFlag::Map &subtask_map) {
  RangeMaintenanceGuard::Activator activator(m_maintenance_guard);
  AccessGroupVector ag_vector(0);
  uint64_t memory_purged = 0;

  {
    ScopedLock lock(m_schema_mutex);
    ag_vector = m_access_group_vector;
  }

  try {
    for (size_t i=0; i<ag_vector.size(); i++) {
      if ( subtask_map.memory_purge(ag_vector[i].get()) )
	memory_purged += ag_vector[i]->purge_memory(subtask_map);
    }
  }
  catch (Exception &e) {
    if (e.code() == Error::CANCELLED || cancel_maintenance())
      return;
    throw;
  }

  {
    ScopedLock lock(m_mutex);
    m_maintenance_generation++;
  }

  HT_INFOF("Memory Purge complete for range %s.  Purged %llu bytes of memory",
	   m_name.c_str(), (Llu)memory_purged);

}


/**
 * This method is called when the range is offline so no locking is needed
 */
void Range::recovery_finalize() {

  if (m_metalog_entity->state.state == RangeState::SPLIT_LOG_INSTALLED ||
      m_metalog_entity->state.state == RangeState::RELINQUISH_LOG_INSTALLED) {
    CommitLogReaderPtr commit_log_reader =
      new CommitLogReader(Global::dfs, m_metalog_entity->state.transfer_log);

    replay_transfer_log(commit_log_reader.get());

    commit_log_reader = 0;

    m_transfer_log = new CommitLog(Global::dfs, m_metalog_entity->state.transfer_log);

    // re-initiate compaction
    for (size_t i=0; i<m_access_group_vector.size(); i++)
      m_access_group_vector[i]->stage_compaction();

    if (m_metalog_entity->state.state == RangeState::SPLIT_LOG_INSTALLED) {
      HT_INFOF("Restored range state to SPLIT_LOG_INSTALLED (split point='%s' "
               "xfer log='%s')", m_metalog_entity->state.split_point, m_metalog_entity->state.transfer_log);
      m_split_row = m_metalog_entity->state.split_point;
    }
    else
      HT_INFOF("Restored range state to RELINQUISH_LOG_INSTALLED (xfer log='%s')",
               m_metalog_entity->state.transfer_log);
  }

  for (size_t i=0; i<m_access_group_vector.size(); i++)
    m_access_group_vector[i]->recovery_finalize();
}


void Range::lock() {
  m_schema_mutex.lock();
  m_updates++;  // assumes this method is called for updates only
  for (size_t i=0; i<m_access_group_vector.size(); ++i)
    m_access_group_vector[i]->lock();
  m_revision = TIMESTAMP_MIN;
}


void Range::unlock() {

  // this needs to happen before the subsequent m_mutex lock
  // because the lock ordering is assumed to be Range::m_mutex -> AccessGroup::lock
  for (size_t i=0; i<m_access_group_vector.size(); ++i)
    m_access_group_vector[i]->unlock();

  {
    ScopedLock lock(m_mutex);
    if (m_revision > m_latest_revision)
      m_latest_revision = m_revision;
  }

  m_schema_mutex.unlock();
}


void Range::replay_transfer_log(CommitLogReader *commit_log_reader) {
  BlockCompressionHeaderCommitLog header;
  const uint8_t *base, *ptr, *end;
  size_t len;
  ByteString key, value;
  Key key_comps;
  size_t nblocks = 0;
  size_t count = 0;
  TableIdentifier table_id;

  m_revision = TIMESTAMP_MIN;

  try {

    while (commit_log_reader->next(&base, &len, &header)) {

      ptr = base;
      end = base + len;

      table_id.decode(&ptr, &len);

      if (strcmp(m_metalog_entity->table.id, table_id.id))
        HT_THROWF(Error::RANGESERVER_CORRUPT_COMMIT_LOG,
                  "Table name mis-match in split log replay \"%s\" != \"%s\"",
                  m_metalog_entity->table.id, table_id.id);

      while (ptr < end) {
        key.ptr = (uint8_t *)ptr;
        key_comps.load(key);
        ptr += key_comps.length;
        value.ptr = (uint8_t *)ptr;
        ptr += value.length();
        add(key_comps, value);
        count++;
      }
      nblocks++;
    }

    if (m_revision > m_latest_revision)
      m_latest_revision = m_revision;

    {
      ScopedLock lock(m_mutex);
      HT_INFOF("Replayed %d updates (%d blocks) from split log '%s' into "
               "%s[%s..%s]", (int)count, (int)nblocks,
               commit_log_reader->get_log_dir().c_str(),
               m_metalog_entity->table.id, m_metalog_entity->spec.start_row, m_metalog_entity->spec.end_row);
    }

    m_added_inserts = 0;
    memset(m_added_deletes, 0, 3*sizeof(int64_t));

  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << "Problem replaying split log - " << e << HT_END;
    if (m_revision > m_latest_revision)
      m_latest_revision = m_revision;
    throw;
  }
}


int64_t Range::get_scan_revision() {
  ScopedLock lock(m_mutex);
  return m_latest_revision;
}

