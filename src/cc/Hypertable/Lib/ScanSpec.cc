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

#include "Common/Serialization.h"

#include "ScanSpec.h"

using namespace std;
using namespace Hypertable;
using namespace Serialization;

const int64_t Hypertable::BEGINNING_OF_TIME = std::numeric_limits<int64_t>::min();
const int64_t Hypertable::END_OF_TIME = std::numeric_limits<int64_t>::max();

RowInterval::RowInterval() : start(0), start_inclusive(true),
			     end(0), end_inclusive(true) { }

size_t RowInterval::encoded_length() const {
  return 2 + encoded_length_vstr(start) + encoded_length_vstr(end);
}

void RowInterval::encode(uint8_t **bufp) const {
  encode_vstr(bufp, start);
  encode_bool(bufp, start_inclusive);
  encode_vstr(bufp, end);
  encode_bool(bufp, end_inclusive);
}


void RowInterval::decode(const uint8_t **bufp, size_t *remainp) {
  HT_TRY("decoding row interval",
    start = decode_vstr(bufp, remainp);
    start_inclusive = decode_bool(bufp, remainp);
    end = decode_vstr(bufp, remainp);
    end_inclusive = decode_bool(bufp, remainp));
}

ScanSpec::ScanSpec() : row_limit(0), max_versions(0),
       time_interval(0, END_OF_TIME), return_deletes(false) {
}

size_t ScanSpec::encoded_length() const {
  size_t len = encoded_length_vi32(row_limit) +
               encoded_length_vi32(max_versions) +
               encoded_length_vi32(columns.size()) +
               encoded_length_vi32(row_intervals.size());
  foreach(const char *c, columns) len += encoded_length_vstr(c);
  foreach(const RowInterval &ri, row_intervals) len += ri.encoded_length();
  return len + 8 + 8 + 1;
}

void ScanSpec::encode(uint8_t **bufp) const {
  encode_vi32(bufp, row_limit);
  encode_vi32(bufp, max_versions);
  encode_vi32(bufp, columns.size());
  foreach(const char *c, columns) encode_vstr(bufp, c);
  encode_vi32(bufp, row_intervals.size());
  foreach(const RowInterval &ri, row_intervals) ri.encode(bufp);
  encode_i64(bufp, time_interval.first);
  encode_i64(bufp, time_interval.second);
  encode_bool(bufp, return_deletes);
}

void ScanSpec::decode(const uint8_t **bufp, size_t *remainp) {
  RowInterval ri;
  HT_TRY("decoding scan spec",
    row_limit = decode_vi32(bufp, remainp);
    max_versions = decode_vi32(bufp, remainp);
    for (size_t nc = decode_vi32(bufp, remainp); nc--;)
      columns.push_back(decode_vstr(bufp, remainp));
    for (size_t nri = decode_vi32(bufp, remainp); nri--;) {
      ri.decode(bufp, remainp);
      row_intervals.push_back(ri);
    }
    time_interval.first = decode_i64(bufp, remainp);
    time_interval.second = decode_i64(bufp, remainp);
    return_deletes = decode_i8(bufp, remainp));
}


ostream &Hypertable::operator<<(ostream &os, const RowInterval &ri) {
  os <<"{RowInterval: ";
  if (ri.start)
    os << "\"" << ri.start << "\"";
  else
    os << "NULL";
  if (ri.start_inclusive)
    os << " <= row ";
  else
    os << " < row ";
  if (ri.end_inclusive)
    os << "<= ";
  else
    os << "< ";
  if (ri.end)
    os << "\"" << ri.end << "\"";
  else
    os << "0xff 0xff";
  os << "}";
  return os;
}


ostream &Hypertable::operator<<(ostream &os, const ScanSpec &scan_spec) {
  os <<"\n{ScanSpec: row_limit="<< scan_spec.row_limit
     <<" max_versions="<< scan_spec.max_versions
     <<"\n rows=";

  foreach(const RowInterval &ri, scan_spec.row_intervals)
    os << " " << ri;

  os << "\n columns=(";

  foreach (const char *c, scan_spec.columns)
    os <<"'"<< c << "' ";

  os <<')';

  os <<"\n time_interval=(" << scan_spec.time_interval.first <<", "
     << scan_spec.time_interval.second <<")\n}\n";
  return os;
}
