/** -*- c++ -*-
 * Copyright (C) 2010 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
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
#include "Common/Serialization.h"

#include "KeySpec.h"
#include "StatsRangeServer.h"

using namespace Hypertable;

namespace {
  enum Group {
    PRIMARY_GROUP = 0
  };
}

StatsRangeServer::StatsRangeServer() : StatsSerializable(RANGE_SERVER, 1), timestamp(TIMESTAMP_MIN) {
  group_ids[0] = PRIMARY_GROUP;
}


StatsRangeServer::StatsRangeServer(PropertiesPtr &props) : StatsSerializable(RANGE_SERVER, 1), timestamp(TIMESTAMP_MIN) {
  const char *base, *ptr;
  String datadirs = props->get_str("Hypertable.RangeServer.Monitoring.DataDirectories");
  String dir;
  std::vector<String> dirs;

  boost::trim_if(datadirs, boost::is_any_of(" \t\"'"));

  base = datadirs.c_str();
  while ((ptr = strchr(base, ',')) != 0) {
    dir = String(base, ptr-base);
    boost::trim(dir);
    dirs.push_back(dir);
    base = ptr+1;
  }
  dir = String(base);
  boost::trim(dir);
  dirs.push_back(dir);

  system.add_categories(StatsSystem::CPU|StatsSystem::LOADAVG|StatsSystem::MEMORY|
                        StatsSystem::DISK|StatsSystem::SWAP|StatsSystem::NET|
                        StatsSystem::PROC | StatsSystem::FS, dirs);
  group_ids[0] = PRIMARY_GROUP;
}

StatsRangeServer::StatsRangeServer(const StatsRangeServer &other) : StatsSerializable(other.id, other.group_count) {
  memcpy(group_ids, other.group_ids, group_count);
  location = other.location;
  timestamp = other.timestamp;
  range_count = other.range_count;
  scan_count = other.scan_count;
  scanned_cells = other.scanned_cells;
  scanned_bytes = other.scanned_bytes;
  update_count = other.update_count;
  updated_cells = other.updated_cells;
  updated_bytes = other.updated_bytes;
  sync_count = other.sync_count;
  query_cache_max_memory = other.query_cache_max_memory;
  query_cache_available_memory = other.query_cache_available_memory;
  query_cache_accesses = other.query_cache_accesses;
  query_cache_hits = other.query_cache_hits;
  block_cache_max_memory = other.block_cache_max_memory;
  block_cache_available_memory = other.block_cache_available_memory;
  block_cache_accesses = other.block_cache_accesses;
  block_cache_hits = other.block_cache_hits;
  system = other.system;
  tables = other.tables;
}

bool StatsRangeServer::operator==(const StatsRangeServer &other) const {
  if (location != other.location ||
      timestamp != other.timestamp ||
      range_count != other.range_count ||
      scan_count != other.scan_count ||
      scanned_cells != other.scanned_cells ||
      scanned_bytes != other.scanned_bytes ||
      update_count != other.update_count ||
      updated_cells != other.updated_cells ||
      updated_bytes != other.updated_bytes ||
      sync_count != other.sync_count ||
      query_cache_max_memory != other.query_cache_max_memory ||
      query_cache_available_memory != other.query_cache_available_memory ||
      query_cache_accesses != other.query_cache_accesses ||
      query_cache_hits != other.query_cache_hits ||
      block_cache_max_memory != other.block_cache_max_memory ||
      block_cache_available_memory != other.block_cache_available_memory ||
      block_cache_accesses != other.block_cache_accesses ||
      block_cache_hits != other.block_cache_hits ||
      system != other.system)
    return false;
  if (tables.size() != other.tables.size())
    return false;
  for (size_t i=0; i<tables.size(); i++) {
    if (tables[i] != other.tables[i])
      return false;
  }
  return true;
}



size_t StatsRangeServer::encoded_length_group(int group) const {
  if (group == PRIMARY_GROUP) {
    size_t len = Serialization::encoded_length_vstr(location) + 4 + 8*16 + \
      system.encoded_length() + \
      Serialization::encoded_length_vi32(tables.size());
    for (size_t i=0; i<tables.size(); i++)
      len += tables[i].encoded_length();
    return len;
  }
  else
    HT_FATALF("Invalid group number (%d)", group);
  return 0;
}

void StatsRangeServer::encode_group(int group, uint8_t **bufp) const {
  if (group == PRIMARY_GROUP) {
    Serialization::encode_vstr(bufp, location);
    Serialization::encode_i64(bufp, timestamp);
    Serialization::encode_i32(bufp, range_count);
    Serialization::encode_i64(bufp, scan_count);
    Serialization::encode_i64(bufp, scanned_cells);
    Serialization::encode_i64(bufp, scanned_bytes);
    Serialization::encode_i64(bufp, update_count);
    Serialization::encode_i64(bufp, updated_cells);
    Serialization::encode_i64(bufp, updated_bytes);
    Serialization::encode_i64(bufp, sync_count);
    Serialization::encode_i64(bufp, query_cache_max_memory);
    Serialization::encode_i64(bufp, query_cache_available_memory);
    Serialization::encode_i64(bufp, query_cache_accesses);
    Serialization::encode_i64(bufp, query_cache_hits);
    Serialization::encode_i64(bufp, block_cache_max_memory);
    Serialization::encode_i64(bufp, block_cache_available_memory);
    Serialization::encode_i64(bufp, block_cache_accesses);
    Serialization::encode_i64(bufp, block_cache_hits);
    system.encode(bufp);
    Serialization::encode_vi32(bufp, tables.size());
    for (size_t i=0; i<tables.size(); i++)
      tables[i].encode(bufp);
  }
  else
    HT_FATALF("Invalid group number (%d)", group);
}

void StatsRangeServer::decode_group(int group, uint16_t len, const uint8_t **bufp, size_t *remainp) {
  if (group == PRIMARY_GROUP) {
    location = Serialization::decode_vstr(bufp, remainp);
    timestamp = Serialization::decode_i64(bufp, remainp);
    range_count = Serialization::decode_i32(bufp, remainp);
    scan_count = Serialization::decode_i64(bufp, remainp);
    scanned_cells = Serialization::decode_i64(bufp, remainp);
    scanned_bytes = Serialization::decode_i64(bufp, remainp);
    update_count = Serialization::decode_i64(bufp, remainp);
    updated_cells = Serialization::decode_i64(bufp, remainp);
    updated_bytes = Serialization::decode_i64(bufp, remainp);
    sync_count = Serialization::decode_i64(bufp, remainp);
    query_cache_max_memory = Serialization::decode_i64(bufp, remainp);
    query_cache_available_memory = Serialization::decode_i64(bufp, remainp);
    query_cache_accesses = Serialization::decode_i64(bufp, remainp);
    query_cache_hits = Serialization::decode_i64(bufp, remainp);
    block_cache_max_memory = Serialization::decode_i64(bufp, remainp);
    block_cache_available_memory = Serialization::decode_i64(bufp, remainp);
    block_cache_accesses = Serialization::decode_i64(bufp, remainp);
    block_cache_hits = Serialization::decode_i64(bufp, remainp);
    system.decode(bufp, remainp);
    size_t table_count = Serialization::decode_vi32(bufp, remainp);
    tables.clear();
    for (size_t i=0; i<table_count; i++) {
      StatsTable table;
      table.decode(bufp, remainp);
      tables.push_back(table);
    }
  }
  else {
    HT_WARNF("Unrecognized StatsRangeServer group %d, skipping...", group);
    (*bufp) += len;
    (*remainp) -= len;
  }
}
