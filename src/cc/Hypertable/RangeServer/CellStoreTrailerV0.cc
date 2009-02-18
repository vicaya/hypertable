/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#include "CellStoreTrailerV0.h"

using namespace std;
using namespace Hypertable;
using namespace Serialization;


/**
 *
 */
CellStoreTrailerV0::CellStoreTrailerV0() {
  assert(sizeof(float) == 4);
  clear();
}


/**
 */
void CellStoreTrailerV0::clear() {
  fix_index_offset = 0;
  var_index_offset = 0;
  filter_offset = 0;
  index_entries = 0;
  total_entries = 0;
  num_filter_items = 0;
  filter_false_positive_rate = 0.0;
  blocksize = 0;
  revision = 0;
  table_id = 0xffffffff;
  table_generation = 0;
  compression_ratio = 0.0;
  compression_type = 0;
  version = 0;
}



/**
 */
void CellStoreTrailerV0::serialize(uint8_t *buf) {
  uint8_t *base = buf;
  encode_i32(&buf, fix_index_offset);
  encode_i32(&buf, var_index_offset);
  encode_i32(&buf, filter_offset);
  encode_i32(&buf, index_entries);
  encode_i32(&buf, total_entries);
  encode_i32(&buf, num_filter_items);
  encode_i32(&buf, filter_false_positive_rate_i32);
  encode_i32(&buf, blocksize);
  encode_i64(&buf, revision);
  encode_i32(&buf, table_id);
  encode_i32(&buf, table_generation);
  encode_i32(&buf, compression_ratio_i32);
  encode_i16(&buf, compression_type);
  encode_i16(&buf, version);
  assert((buf-base) == (int)CellStoreTrailerV0::size());
  (void)base;
}



/**
 */
void CellStoreTrailerV0::deserialize(const uint8_t *buf) {
  HT_TRY("deserializing cellstore trailer",
    size_t remaining = CellStoreTrailerV0::size();
    fix_index_offset = decode_i32(&buf, &remaining);
    var_index_offset = decode_i32(&buf, &remaining);
    filter_offset = decode_i32(&buf, &remaining);
    index_entries = decode_i32(&buf, &remaining);
    total_entries = decode_i32(&buf, &remaining);
    num_filter_items = decode_i32(&buf, &remaining);
    filter_false_positive_rate_i32 = decode_i32(&buf, &remaining);
    blocksize = decode_i32(&buf, &remaining);
    revision = decode_i64(&buf, &remaining);
    table_id = decode_i32(&buf, &remaining);
    table_generation = decode_i32(&buf, &remaining);
    compression_ratio_i32 = decode_i32(&buf, &remaining);
    compression_type = decode_i16(&buf, &remaining);
    version = decode_i16(&buf, &remaining));
}



/**
 */
void CellStoreTrailerV0::display(std::ostream &os) {
  os << "{CellStoreTrailerV0: ";
  os << "fix_index_offset=" << fix_index_offset;
  os << ", var_index_offset=" << var_index_offset;
  os << ", filter_offset=" << filter_offset;
  os << ", index_entries=" << index_entries;
  os << ", total_entries=" << total_entries;
  os << ", num_filter_items = " << num_filter_items;
  os << ", filter_false_positive_rate = "
     << filter_false_positive_rate;
  os << ", blocksize=" << blocksize;
  os << ", revision=" << revision;
  os << ", table_id=" << table_id;
  os << ", table_generation=" << table_generation;
  os << ", compression_ratio=" << compression_ratio;
  os << ", compression_type=" << compression_type;
  os << ", version=" << version << "}";
}

