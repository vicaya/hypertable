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
#include "Common/Logger.h"
#include "Common/Serialization.h"

#include "StatsTable.h"

using namespace Hypertable;

namespace {
  enum Group {
    MAIN_GROUP = 0
  };
}

StatsTable::StatsTable() : StatsSerializable(TABLE, 1) { 
  clear(); 
  group_ids[0] = MAIN_GROUP;  
}


void StatsTable::clear() {
  table_id = "";
  range_count = 0;
  cell_count = 0;
  file_count = 0;
  scans = 0;
  cells_scanned = 0;
  cells_returned = 0;
  bytes_scanned = 0;
  bytes_returned = 0;
  updates = 0;
  cells_written = 0;
  bytes_written = 0;
  disk_used = 0;
  key_bytes = 0;
  value_bytes = 0;
  compression_ratio = 0.0;
  memory_used = 0;
  memory_allocated = 0;
  shadow_cache_memory = 0;
  block_index_memory = 0;
  bloom_filter_memory = 0;
  bloom_filter_accesses = 0;
  bloom_filter_maybes = 0;
}

size_t StatsTable::encoded_length_group(int group) const {

  if (group == MAIN_GROUP) {
    return Serialization::encoded_length_vstr(table_id) + \
      Serialization::encoded_length_vi32(range_count) + \
      Serialization::encoded_length_vi64(cell_count) + \
      Serialization::encoded_length_vi64(file_count) + \
      Serialization::encoded_length_vi64(scans) +       \
      Serialization::encoded_length_vi64(cells_scanned) + \
      Serialization::encoded_length_vi64(cells_returned) + \
      Serialization::encoded_length_vi64(bytes_scanned) + \
      Serialization::encoded_length_vi64(bytes_returned) + \
      Serialization::encoded_length_vi64(updates) +     \
      Serialization::encoded_length_vi64(cells_written) + \
      Serialization::encoded_length_vi64(bytes_written) + \
      Serialization::encoded_length_vi64(disk_used) + \
      Serialization::encoded_length_vi64(key_bytes) + \
      Serialization::encoded_length_vi64(value_bytes) + \
      Serialization::encoded_length_double() + \
      Serialization::encoded_length_vi64(memory_used) + \
      Serialization::encoded_length_vi64(memory_allocated) + \
      Serialization::encoded_length_vi64(shadow_cache_memory) + \
      Serialization::encoded_length_vi64(block_index_memory) + \
      Serialization::encoded_length_vi64(bloom_filter_memory) + \
      Serialization::encoded_length_vi64(bloom_filter_accesses) + \
      Serialization::encoded_length_vi64(bloom_filter_maybes);
  }
  else
    HT_FATALF("Invalid group number (%d)", group);
  return 0;
}

void StatsTable::encode_group(int group, uint8_t **bufp) const {
  if (group == MAIN_GROUP) {
    Serialization::encode_vstr(bufp, table_id);
    Serialization::encode_vi32(bufp, range_count);
    Serialization::encode_vi64(bufp, cell_count);
    Serialization::encode_vi64(bufp, file_count);
    Serialization::encode_vi64(bufp, scans);
    Serialization::encode_vi64(bufp, cells_scanned);
    Serialization::encode_vi64(bufp, cells_returned);
    Serialization::encode_vi64(bufp, bytes_scanned);
    Serialization::encode_vi64(bufp, bytes_returned);
    Serialization::encode_vi64(bufp, updates);
    Serialization::encode_vi64(bufp, cells_written);
    Serialization::encode_vi64(bufp, bytes_written);
    Serialization::encode_vi64(bufp, disk_used);
    Serialization::encode_vi64(bufp, key_bytes);
    Serialization::encode_vi64(bufp, value_bytes);
    Serialization::encode_double(bufp, compression_ratio);
    Serialization::encode_vi64(bufp, memory_used);
    Serialization::encode_vi64(bufp, memory_allocated);
    Serialization::encode_vi64(bufp, shadow_cache_memory);
    Serialization::encode_vi64(bufp, block_index_memory);
    Serialization::encode_vi64(bufp, bloom_filter_memory);
    Serialization::encode_vi64(bufp, bloom_filter_accesses);
    Serialization::encode_vi64(bufp, bloom_filter_maybes);
  }
  else
    HT_FATALF("Invalid group number (%d)", group);
}

void StatsTable::decode_group(int group, uint16_t len, const uint8_t **bufp, size_t *remainp) {
  if (group == MAIN_GROUP) {
    table_id = Serialization::decode_vstr(bufp, remainp);
    range_count = Serialization::decode_vi32(bufp, remainp);
    cell_count = Serialization::decode_vi64(bufp, remainp);
    file_count = Serialization::decode_vi64(bufp, remainp);
    scans = Serialization::decode_vi64(bufp, remainp);
    cells_scanned = Serialization::decode_vi64(bufp, remainp);
    cells_returned = Serialization::decode_vi64(bufp, remainp);
    bytes_scanned = Serialization::decode_vi64(bufp, remainp);
    bytes_returned = Serialization::decode_vi64(bufp, remainp);
    updates = Serialization::decode_vi64(bufp, remainp);
    cells_written = Serialization::decode_vi64(bufp, remainp);
    bytes_written = Serialization::decode_vi64(bufp, remainp);
    disk_used = Serialization::decode_vi64(bufp, remainp);
    key_bytes = Serialization::decode_vi64(bufp, remainp);
    value_bytes = Serialization::decode_vi64(bufp, remainp);
    compression_ratio = Serialization::decode_double(bufp, remainp);
    memory_used = Serialization::decode_vi64(bufp, remainp);
    memory_allocated = Serialization::decode_vi64(bufp, remainp);
    shadow_cache_memory = Serialization::decode_vi64(bufp, remainp);
    block_index_memory = Serialization::decode_vi64(bufp, remainp);
    bloom_filter_memory = Serialization::decode_vi64(bufp, remainp);
    bloom_filter_accesses = Serialization::decode_vi64(bufp, remainp);
    bloom_filter_maybes = Serialization::decode_vi64(bufp, remainp);
  }
  else
    HT_FATALF("Invalid group number (%d)", group);
}

bool StatsTable::operator==(const StatsTable &other) const {
  if (*((StatsSerializable *)this) != *((StatsSerializable *)&other))
    return false;
  if (table_id == other.table_id &&
      range_count == other.range_count &&
      cell_count == other.cell_count &&
      file_count == other.file_count &&
      scans == other.scans &&
      cells_scanned == other.cells_scanned &&
      cells_returned == other.cells_returned &&
      bytes_scanned == other.bytes_scanned &&
      bytes_returned == other.bytes_returned &&
      updates == other.updates &&
      cells_written == other.cells_written &&
      bytes_written == other.bytes_written &&
      disk_used == other.disk_used &&
      key_bytes == other.key_bytes &&
      value_bytes == other.value_bytes &&
      Serialization::equal(compression_ratio, other.compression_ratio) &&
      memory_used == other.memory_used &&
      memory_allocated == other.memory_allocated &&
      shadow_cache_memory == other.shadow_cache_memory &&
      block_index_memory == other.block_index_memory &&
      bloom_filter_memory == other.bloom_filter_memory &&
      bloom_filter_accesses == other.bloom_filter_accesses &&
      bloom_filter_maybes == other.bloom_filter_maybes)
    return true;
  return false;
}
