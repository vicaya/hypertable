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

#include <iostream>

#include "AsyncComm/Serialization.h"

#include "CellStoreTrailerV0.h"

using namespace Hypertable;
using namespace std;


/**
 *
 */
CellStoreTrailerV0::CellStoreTrailerV0() {
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
  blocksize = 0;
  timestamp.logical = 0;
  timestamp.real = 0;
  compression_ratio = 0.0;
  compression_type = 0;
  version = 0;
}



/**
 */
void CellStoreTrailerV0::serialize(uint8_t *buf) {
  uint8_t *base = buf;
  Serialization::encode_int(&buf, fix_index_offset);
  Serialization::encode_int(&buf, var_index_offset);
  Serialization::encode_int(&buf, filter_offset);
  Serialization::encode_int(&buf, index_entries);
  Serialization::encode_int(&buf, total_entries);
  Serialization::encode_int(&buf, blocksize);
  Serialization::encode_long(&buf, timestamp.logical);
  Serialization::encode_long(&buf, timestamp.real);
  Serialization::encode_int(&buf, *((uint32_t *)&compression_ratio));
  Serialization::encode_short(&buf, compression_type);
  Serialization::encode_short(&buf, version);
  assert((buf-base) == (int)CellStoreTrailerV0::size());
  (void)base;
}



/**
 */
void CellStoreTrailerV0::deserialize(uint8_t *buf) {
  size_t remaining = CellStoreTrailerV0::size();
  Serialization::decode_int(&buf, &remaining, &fix_index_offset);
  Serialization::decode_int(&buf, &remaining, &var_index_offset);
  Serialization::decode_int(&buf, &remaining, &filter_offset);
  Serialization::decode_int(&buf, &remaining, &index_entries);
  Serialization::decode_int(&buf, &remaining, &total_entries);
  Serialization::decode_int(&buf, &remaining, &blocksize);
  Serialization::decode_long(&buf, &remaining, &timestamp.logical);
  Serialization::decode_long(&buf, &remaining, &timestamp.real);
  Serialization::decode_int(&buf, &remaining, (uint32_t *)&compression_ratio);
  Serialization::decode_short(&buf, &remaining, &compression_type);
  Serialization::decode_short(&buf, &remaining, &version);
}



/**
 */
void CellStoreTrailerV0::display(std::ostream &os) {
  os << "fix_index_offset = " << fix_index_offset << endl;
  os << "var_index_offset = " << var_index_offset << endl;
  os << "filter_offset = " << filter_offset << endl;
  os << "index_entries = " << index_entries << endl;
  os << "total_entries = " << total_entries << endl;
  os << "blocksize = " << blocksize << endl;
  os << "timestamp logical = " << timestamp.logical << endl;
  os << "timestamp real = " << timestamp.real << endl;
  os << "compression_ratio = " << compression_ratio << endl;
  os << "compression_type = " << compression_type << endl;
  os << "version = " << version << endl;
}

