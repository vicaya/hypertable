/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
#include <cstring>
#include <iostream>

#include "AsyncComm/CommBuf.h"
#include "Common/Serialization.h"

#include "Types.h"

using namespace std;
using namespace Hypertable;
using namespace Serialization;

const uint64_t Hypertable::END_OF_TIME = (uint64_t)-1;

size_t TableIdentifier::encoded_length() const {
  return 8 + encoded_length_vstr(name);
}

void TableIdentifier::encode(uint8_t **bufp) const {
  encode_vstr(bufp, name);
  encode_i32(bufp, id);
  encode_i32(bufp, generation);
}

void TableIdentifier::decode(const uint8_t **bufp, size_t *remainp) {
  HT_TRY("decoding table identitier",
    name = decode_vstr(bufp, remainp);
    id = decode_i32(bufp, remainp);
    generation = decode_i32(bufp, remainp));
}

size_t RangeSpec::encoded_length() const {
  return encoded_length_vstr(start_row) + encoded_length_vstr(end_row);
}

void RangeSpec::encode(uint8_t **bufp) const {
  encode_vstr(bufp, start_row);
  encode_vstr(bufp, end_row);
}

void RangeSpec::decode(const uint8_t **bufp, size_t *remainp) {
  HT_TRY("decoding range spec",
    start_row = decode_vstr(bufp, remainp);
    end_row = decode_vstr(bufp, remainp));
}


ScanSpec::ScanSpec() : row_limit(0), max_versions(0), start_row(0),
    start_row_inclusive(true), end_row(0), end_row_inclusive(true),
    interval(0, END_OF_TIME), return_deletes(false) {
}

size_t ScanSpec::encoded_length() const {
  size_t len = encoded_length_vi32(row_limit) +
               encoded_length_vi32(max_versions) +
               encoded_length_vstr(start_row) + 1 +
               encoded_length_vstr(end_row) + 1 +
               encoded_length_vi32(columns.size());
  foreach(const char *c, columns) len += encoded_length_vstr(c);
  return len + 8 + 8 + 1;
}

void ScanSpec::encode(uint8_t **bufp) const {
  encode_vi32(bufp, row_limit);
  encode_vi32(bufp, max_versions);
  encode_vstr(bufp, start_row);
  encode_bool(bufp, start_row_inclusive);
  encode_vstr(bufp, end_row);
  encode_bool(bufp, end_row_inclusive);
  encode_vi32(bufp, columns.size());
  foreach(const char *c, columns) encode_vstr(bufp, c);
  encode_i64(bufp, interval.first);
  encode_i64(bufp, interval.second);
  encode_bool(bufp, return_deletes);
}

void ScanSpec::decode(const uint8_t **bufp, size_t *remainp) {
  HT_TRY("decoding scan spec",
    row_limit = decode_vi32(bufp, remainp);
    max_versions = decode_vi32(bufp, remainp);
    start_row = decode_vstr(bufp, remainp);
    start_row_inclusive = decode_i8(bufp, remainp);
    end_row = decode_vstr(bufp, remainp);
    end_row_inclusive = decode_i8(bufp, remainp);
    for (size_t nc = decode_vi32(bufp, remainp); nc--;)
      columns.push_back(decode_vstr(bufp, remainp));
    interval.first = decode_i64(bufp, remainp);
    interval.second = decode_i64(bufp, remainp);
    return_deletes = decode_i8(bufp, remainp));
}


ostream &Hypertable::operator<<(ostream &os, const TableIdentifier &tid) {
  os << "Table Name = " << tid.name << endl;
  os << "Table ID   = " << tid.id << endl;
  os << "Generation = " << tid.generation << endl;
  return os;
}

ostream &Hypertable::operator<<(ostream &os, const RangeSpec &range) {
  if (range.start_row == 0)
    os << "StartRow = [NULL]" << endl;
  else
    os << "StartRow = \"" << range.start_row << "\"" << endl;
  if (range.end_row == 0)
    os << "EndRow   = [NULL]" << endl;
  else
    os << "EndRow   = \"" << range.end_row << "\"" << endl;
  return os;
}

ostream &Hypertable::operator<<(ostream &os, const ScanSpec &scan_spec) {
  os << "RowLimit    = " << scan_spec.row_limit << endl;
  os << "MaxVersions = " << scan_spec.max_versions << endl;
  os << "Columns     = ";
  foreach (const char *c, scan_spec.columns)
    os << c << " ";
  os << endl;
  if (scan_spec.start_row)
    os << "StartRow  = " << scan_spec.start_row << endl;
  else
    os << "StartRow  = " << endl;
  os << "StartRowInclusive = " << scan_spec.start_row_inclusive << endl;
  if (scan_spec.end_row)
    os << "EndRow    = " << scan_spec.end_row << endl;
  else
    os << "EndRow    = " << endl;
  os << "EndRowInclusive = " << scan_spec.end_row_inclusive << endl;
  os << "MinTime     = " << scan_spec.interval.first << endl;
  os << "MaxTime     = " << scan_spec.interval.second << endl;
  return os;
}
