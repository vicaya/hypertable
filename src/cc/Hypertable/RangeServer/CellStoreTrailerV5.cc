/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
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
#include <cassert>
#include <iostream>

#include "Common/Filesystem.h"
#include "Common/Serialization.h"
#include "Common/Logger.h"

#include "Hypertable/Lib/KeySpec.h"
#include "Hypertable/Lib/Schema.h"

#include "CellStoreTrailerV5.h"

using namespace std;
using namespace Hypertable;
using namespace Serialization;


/**
 *
 */
CellStoreTrailerV5::CellStoreTrailerV5() {
  assert(sizeof(float) == 4);
  clear();
}


/**
 */
void CellStoreTrailerV5::clear() {
  fix_index_offset = 0;
  var_index_offset = 0;
  filter_offset = 0;
  replaced_files_offset = 0;
  index_entries = 0;
  total_entries = 0;
  filter_length = 0;
  filter_items_estimate = 0;
  filter_items_actual = 0;
  replaced_files_length = 0;
  replaced_files_entries = 0;
  blocksize = 0;
  revision = 0;
  timestamp_min = TIMESTAMP_MAX;
  timestamp_max = TIMESTAMP_MIN;
  expiration_time = TIMESTAMP_NULL;
  create_time = 0;
  expirable_data = 0;
  delete_count = 0;
  key_bytes = 0;
  value_bytes = 0;
  table_id = 0xffffffff;
  table_generation = 0;
  flags = 0;
  alignment = HT_DIRECT_IO_ALIGNMENT;
  compression_ratio = 0.0;
  compression_type = 0;
  key_compression_scheme = 0;
  bloom_filter_mode = BLOOM_FILTER_DISABLED;
  bloom_filter_hash_count = 0;
  version = 5;
}



/**
 */
void CellStoreTrailerV5::serialize(uint8_t *buf) {
  uint8_t *base = buf;
  encode_i64(&buf, fix_index_offset);
  encode_i64(&buf, var_index_offset);
  encode_i64(&buf, filter_offset);
  encode_i64(&buf, replaced_files_offset);
  encode_i64(&buf, index_entries);
  encode_i64(&buf, total_entries);
  encode_i64(&buf, filter_length);
  encode_i64(&buf, filter_items_estimate);
  encode_i64(&buf, filter_items_actual);
  encode_i64(&buf, replaced_files_length);
  encode_i32(&buf, replaced_files_entries);
  encode_i64(&buf, blocksize);
  encode_i64(&buf, revision);
  encode_i64(&buf, timestamp_min);
  encode_i64(&buf, timestamp_max);
  encode_i64(&buf, expiration_time);
  encode_i64(&buf, create_time);
  encode_i64(&buf, expirable_data);
  encode_i64(&buf, delete_count);
  encode_i64(&buf, key_bytes);
  encode_i64(&buf, value_bytes);
  encode_i32(&buf, table_id);
  encode_i32(&buf, table_generation);
  encode_i32(&buf, flags);
  encode_i32(&buf, alignment);
  encode_i32(&buf, compression_ratio_i32);
  encode_i16(&buf, compression_type);
  encode_i16(&buf, key_compression_scheme);
  encode_i8(&buf, bloom_filter_mode);
  encode_i8(&buf, bloom_filter_hash_count);
  encode_i16(&buf, version);
  assert(version == 5);
  assert((buf-base) == (int)CellStoreTrailerV5::size());
  (void)base;
}



/**
 */
void CellStoreTrailerV5::deserialize(const uint8_t *buf) {
  HT_TRY("deserializing cellstore trailer",
    size_t remaining = CellStoreTrailerV5::size();
    fix_index_offset = decode_i64(&buf, &remaining);
    var_index_offset = decode_i64(&buf, &remaining);
    filter_offset = decode_i64(&buf, &remaining);
    replaced_files_offset = decode_i64(&buf, &remaining);
    index_entries = decode_i64(&buf, &remaining);
    total_entries = decode_i64(&buf, &remaining);
    filter_length = decode_i64(&buf, &remaining);
    filter_items_estimate = decode_i64(&buf, &remaining);
    filter_items_actual = decode_i64(&buf, &remaining);
    replaced_files_length = decode_i64(&buf, &remaining);
    replaced_files_entries = decode_i32(&buf, &remaining);
    blocksize = decode_i64(&buf, &remaining);
    revision = decode_i64(&buf, &remaining);
    timestamp_min = decode_i64(&buf, &remaining);
    timestamp_max = decode_i64(&buf, &remaining);
    expiration_time = decode_i64(&buf, &remaining);
    create_time = decode_i64(&buf, &remaining);
    expirable_data = decode_i64(&buf, &remaining);
    delete_count = decode_i64(&buf, &remaining);
    key_bytes = decode_i64(&buf, &remaining);
    value_bytes = decode_i64(&buf, &remaining);
    table_id = decode_i32(&buf, &remaining);
    table_generation = decode_i32(&buf, &remaining);
    flags = decode_i32(&buf, &remaining);
    alignment = decode_i32(&buf, &remaining);
    compression_ratio_i32 = decode_i32(&buf, &remaining);
    compression_type = decode_i16(&buf, &remaining);
    key_compression_scheme = decode_i16(&buf, &remaining);
    bloom_filter_mode = decode_i8(&buf, &remaining);
    bloom_filter_hash_count = decode_i8(&buf, &remaining);
    version = decode_i16(&buf, &remaining));
}



/**
 */
void CellStoreTrailerV5::display(std::ostream &os) {
  os << "{CellStoreTrailerV5: ";
  os << "fix_index_offset=" << fix_index_offset;
  os << ", var_index_offset=" << var_index_offset;
  os << ", filter_offset=" << filter_offset;
  os << ", replaced_files_offset=" << replaced_files_offset;
  os << ", index_entries=" << index_entries;
  os << ", total_entries=" << total_entries;
  os << ", filter_length = " << filter_length;
  os << ", filter_items_estimate = " << filter_items_estimate;
  os << ", filter_items_actual = " << filter_items_actual;
  os << ", replaced_files_length=" << replaced_files_length;
  os << ", replaced_files_entries=" << replaced_files_entries;
  os << ", blocksize=" << blocksize;
  os << ", revision=" << revision;
  os << ", timestamp_min=" << timestamp_min;
  os << ", timestamp_max=" << timestamp_max;
  os << ", expiration_time=" << expiration_time;
  os << ", create_time=" << create_time;
  os << ", expirable_data=" << expirable_data;
  os << ", delete_count=" << delete_count;
  os << ", key_bytes=" << key_bytes;
  os << ", value_bytes=" << value_bytes;
  os << ", table_id=" << table_id;
  os << ", table_generation=" << table_generation;
  os << ", flags=" << flags << " (";
  if (flags & INDEX_64BIT)
    os << " 64BIT_INDEX";
  if (flags & MAJOR_COMPACTION)
    os << " MAJOR_COMPACTION";
  os << " )";
  os << ", alignment=" << alignment;
  os << ", compression_ratio=" << compression_ratio;
  os << ", compression_type=" << compression_type;
  os << ", key_compression_scheme=" << key_compression_scheme;
  if (bloom_filter_mode == BLOOM_FILTER_DISABLED)
    os << ", bloom_filter_mode=DISABLED";
  else if (bloom_filter_mode == BLOOM_FILTER_ROWS)
    os << ", bloom_filter_mode=ROWS";
  else if (bloom_filter_mode == BLOOM_FILTER_ROWS_COLS)
    os << ", bloom_filter_mode=ROWS_COLS";
  else
    os << ", bloom_filter_mode=?(" << bloom_filter_mode << ")";
  os << ", bloom_filter_hash_count=" << bloom_filter_hash_count;
  os << ", version=" << version << "}";
}

/**
 */
void CellStoreTrailerV5::display_multiline(std::ostream &os) {
  os << "[CellStoreTrailerV5]\n";
  os << "  fix_index_offset: " << fix_index_offset << "\n";
  os << "  var_index_offset: " << var_index_offset << "\n";
  os << "  filter_offset: " << filter_offset << "\n";
  os << "  replaced_files_offset: " << replaced_files_offset << "\n";
  os << "  index_entries: " << index_entries << "\n";
  os << "  total_entries: " << total_entries << "\n";
  os << "  filter_length: " << filter_length << "\n";
  os << "  filter_items_estimate: " << filter_items_estimate << "\n";
  os << "  filter_items_actual: " << filter_items_actual << "\n";
  os << "  replaced_files_length: " << replaced_files_length << "\n";
  os << "  replaced_files_entries: " << replaced_files_entries << "\n";
  os << "  blocksize: " << blocksize << "\n";
  os << "  revision: " << revision << "\n";
  os << "  timestamp_min: " << timestamp_min << "\n";
  os << "  timestamp_max: " << timestamp_max << "\n";
  os << "  expiration_time: " << expiration_time << "\n";
  os << "  create_time: " << create_time << "\n";
  os << "  expirable_data: " << expirable_data << "\n";
  os << "  delete_count: " << delete_count << "\n";
  os << "  key_bytes: " << key_bytes << "\n";
  os << "  value_bytes: " << value_bytes << "\n";
  os << "  table_id: " << table_id << "\n";
  os << "  table_generation: " << table_generation << "\n";
  if (flags & INDEX_64BIT)
    os << "  flags: 64BIT_INDEX\n";
  else
    os << "  flags=" << flags << "\n";
  os << "  alignment=" << alignment << "\n";
  os << "  compression_ratio: " << compression_ratio << "\n";
  os << "  compression_type: " << compression_type << "\n";
  os << "  key_compression_scheme: " << key_compression_scheme << "\n";
  if (bloom_filter_mode == BLOOM_FILTER_DISABLED)
    os << "  bloom_filter_mode=DISABLED\n";
  else if (bloom_filter_mode == BLOOM_FILTER_ROWS)
    os << "  bloom_filter_mode=ROWS\n";
  else if (bloom_filter_mode == BLOOM_FILTER_ROWS_COLS)
    os << "  bloom_filter_mode=ROWS_COLS\n";
  else
    os << "  bloom_filter_mode=?(" << bloom_filter_mode << ")\n";
  os << "  bloom_filter_hash_count=" << (int)bloom_filter_hash_count << "\n";
  os << "  version: " << version << std::endl;
}

