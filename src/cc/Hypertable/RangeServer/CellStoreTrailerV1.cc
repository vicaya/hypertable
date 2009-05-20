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
#include <cassert>
#include <iostream>

#include "Common/Serialization.h"
#include "Common/Logger.h"

#include "Hypertable/Lib/KeySpec.h"

#include "CellStoreTrailerV1.h"

using namespace std;
using namespace Hypertable;
using namespace Serialization;


/**
 *
 */
CellStoreTrailerV1::CellStoreTrailerV1() {
  assert(sizeof(float) == 4);
  clear();
}


/**
 */
void CellStoreTrailerV1::clear() {
  fix_index_offset = 0;
  var_index_offset = 0;
  filter_offset = 0;
  index_entries = 0;
  total_entries = 0;
  num_filter_items = 0;
  filter_false_positive_prob = 0.0;
  blocksize = 0;
  revision = 0;
  timestamp_min = TIMESTAMP_MAX;
  timestamp_max = TIMESTAMP_MIN;
  create_time = 0;
  table_id = 0xffffffff;
  table_generation = 0;
  flags = 0;
  compression_ratio = 0.0;
  compression_type = 0;
  version = 1;
}



/**
 */
void CellStoreTrailerV1::serialize(uint8_t *buf) {
  uint8_t *base = buf;
  encode_i64(&buf, fix_index_offset);
  encode_i64(&buf, var_index_offset);
  encode_i64(&buf, filter_offset);
  encode_i64(&buf, index_entries);
  encode_i64(&buf, total_entries);
  encode_i64(&buf, num_filter_items);
  encode_i32(&buf, filter_false_positive_prob_i32);
  encode_i64(&buf, blocksize);
  encode_i64(&buf, revision);
  encode_i64(&buf, timestamp_min);
  encode_i64(&buf, timestamp_max);
  encode_i64(&buf, create_time);
  encode_i32(&buf, table_id);
  encode_i32(&buf, table_generation);
  encode_i32(&buf, flags);
  encode_i32(&buf, compression_ratio_i32);
  encode_i16(&buf, compression_type);
  encode_i16(&buf, version);
  assert(version == 1);
  assert((buf-base) == (int)CellStoreTrailerV1::size());
  (void)base;
}



/**
 */
void CellStoreTrailerV1::deserialize(const uint8_t *buf) {
  HT_TRY("deserializing cellstore trailer",
    size_t remaining = CellStoreTrailerV1::size();
    fix_index_offset = decode_i64(&buf, &remaining);
    var_index_offset = decode_i64(&buf, &remaining);
    filter_offset = decode_i64(&buf, &remaining);
    index_entries = decode_i64(&buf, &remaining);
    total_entries = decode_i64(&buf, &remaining);
    num_filter_items = decode_i64(&buf, &remaining);
    filter_false_positive_prob_i32 = decode_i32(&buf, &remaining);
    blocksize = decode_i64(&buf, &remaining);
    revision = decode_i64(&buf, &remaining);
    timestamp_min = decode_i64(&buf, &remaining);
    timestamp_max = decode_i64(&buf, &remaining);
    create_time = decode_i64(&buf, &remaining);
    table_id = decode_i32(&buf, &remaining);
    table_generation = decode_i32(&buf, &remaining);
    flags = decode_i32(&buf, &remaining);
    compression_ratio_i32 = decode_i32(&buf, &remaining);
    compression_type = decode_i16(&buf, &remaining);
    version = decode_i16(&buf, &remaining));
}



/**
 */
void CellStoreTrailerV1::display(std::ostream &os) {
  os << "{CellStoreTrailerV1: ";
  os << "fix_index_offset=" << fix_index_offset;
  os << ", var_index_offset=" << var_index_offset;
  os << ", filter_offset=" << filter_offset;
  os << ", index_entries=" << index_entries;
  os << ", total_entries=" << total_entries;
  os << ", num_filter_items = " << num_filter_items;
  os << ", filter_false_positive_prob = "
     << filter_false_positive_prob;
  os << ", blocksize=" << blocksize;
  os << ", revision=" << revision;
  os << ", timestamp_min=" << timestamp_min;
  os << ", timestamp_max=" << timestamp_max;
  os << ", create_time=" << create_time;
  os << ", table_id=" << table_id;
  os << ", table_generation=" << table_generation;
  if (flags & INDEX_64BIT)
    os << ", flags=64BIT_INDEX";
  else
    os << ", flags=" << flags;
  os << ", compression_ratio=" << compression_ratio;
  os << ", compression_type=" << compression_type;
  os << ", version=" << version << "}";
}

/**
 */
void CellStoreTrailerV1::display_multiline(std::ostream &os) {
  os << "[CellStoreTrailerV1]\n";
  os << "  fix_index_offset: " << fix_index_offset << "\n";
  os << "  var_index_offset: " << var_index_offset << "\n";
  os << "  filter_offset: " << filter_offset << "\n";
  os << "  index_entries: " << index_entries << "\n";
  os << "  total_entries: " << total_entries << "\n";
  os << "  num_filter_items: " << num_filter_items << "\n";
  os << "  filter_false_positive_prob: "
     << filter_false_positive_prob << "\n";
  os << "  blocksize: " << blocksize << "\n";
  os << "  revision: " << revision << "\n";
  os << "  timestamp_min: " << timestamp_min << "\n";
  os << "  timestamp_max: " << timestamp_max << "\n";
  os << "  create_time: " << create_time << "\n";
  os << "  table_id: " << table_id << "\n";
  os << "  table_generation: " << table_generation << "\n";
  if (flags & INDEX_64BIT)
    os << "  flags: 64BIT_INDEX\n";
  else
    os << "  flags=" << flags << "\n";
  os << "  compression_ratio: " << compression_ratio << "\n";
  os << "  compression_type: " << compression_type << "\n";
  os << "  version: " << version << std::endl;
}

