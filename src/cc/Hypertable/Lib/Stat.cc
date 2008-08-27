/** -*- c++ -*-
 * Copyright (C) 2008 Donald <donaldliew@gmail.com>
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
#include "Common/Serialization.h"
#include "Hypertable/Lib/Stat.h"

using namespace std;
using namespace Hypertable;
using namespace Serialization;

size_t RangeStat::encoded_length() const {
  return 64 + table_identifier.encoded_length() + range_spec.encoded_length();
}

void RangeStat::encode(uint8_t **bufp) const {
  table_identifier.encode(bufp);
  range_spec.encode(bufp);
  encode_i64(bufp, added_inserts);
  encode_i64(bufp, added_deletes[0]);
  encode_i64(bufp, added_deletes[1]);
  encode_i64(bufp, added_deletes[2]);
  encode_i64(bufp, cached_cells);
  encode_i64(bufp, collided_cells);
  encode_i64(bufp, disk_usage);
  encode_i64(bufp, memory_usage);
}

void RangeStat::decode(const uint8_t **bufp, size_t *remainp) {
  TableIdentifier tid(bufp, remainp);
  table_identifier = tid;

  RangeSpec spec(bufp, remainp);
  range_spec = spec;

  HT_TRY("decoding range statistics",
    added_inserts = decode_i64(bufp, remainp);
    added_deletes[0] = decode_i64(bufp, remainp);
    added_deletes[1] = decode_i64(bufp, remainp);
    added_deletes[2] = decode_i64(bufp, remainp);
    cached_cells = decode_i64(bufp, remainp);
    collided_cells = decode_i64(bufp, remainp);
    disk_usage = decode_i64(bufp, remainp);
    memory_usage = decode_i64(bufp, remainp));
}

size_t RangeServerStat::encoded_length() const {
  size_t length = 28;

  for (size_t i = 0; i < range_stats.size(); ++i) {
    length += range_stats[i].encoded_length();
  }

  return length;
}

void RangeServerStat::encode(uint8_t **bufp) const {
  encode_i32(bufp, range_stats.size());

  for (size_t i = 0; i < range_stats.size(); ++i) {
    range_stats[i].encode(bufp);
  }
}

void RangeServerStat::decode(const uint8_t **bufp, size_t *remainp) {
  size_t n;

  HT_TRY("decoding range statistics",
    n = decode_i32(bufp, remainp));

  for (size_t i = 0; i < n; ++i) {
    range_stats.push_back(RangeStat(bufp, remainp));
  }
}

ostream &Hypertable::operator<<(ostream &os, const RangeStat &stat) {
  os << " {" << endl
     << "  table =" << stat.table_identifier << endl
     << "  range_spec =" << stat.range_spec << endl
     << "  disk_usage = " << stat.disk_usage 
     << "  memory_usage = "<< stat.memory_usage << endl
     << "  added_inserts = " << stat.added_inserts 
     << "  cached_cells = " << stat.cached_cells 
     << "  collided_cells = " << stat.collided_cells << endl
     << "  added_row_deletes = " << stat.added_deletes[0] 
     << "  added_cf_deletes = " << stat.added_deletes[1] 
     << "  added_cell_deletes = " << stat.added_deletes[2] << endl
     << " }";
  return os;
}

ostream &Hypertable::operator<<(ostream &os, const RangeServerStat &stat) {
  os << "{RangeServerStat: range_stats_number = " << stat.range_stats.size() << endl;
  for (size_t i = 0; i < stat.range_stats.size(); ++i) {
    os << " range_stats[" << i << "] = " << stat.range_stats[i] << endl;
  }

  os << "}";

  return os;
}

