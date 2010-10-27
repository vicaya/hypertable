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

#include <boost/algorithm/string.hpp>

#include <cstring>
#include <iostream>

#include "Common/Serialization.h"

#include "KeySpec.h"
#include "ScanSpec.h"

using namespace std;
using namespace Hypertable;
using namespace Serialization;

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

size_t CellInterval::encoded_length() const {
  return 2 + encoded_length_vstr(start_row) + encoded_length_vstr(start_column)
      + encoded_length_vstr(end_row) + encoded_length_vstr(end_column);
}

void CellInterval::encode(uint8_t **bufp) const {
  encode_vstr(bufp, start_row);
  encode_vstr(bufp, start_column);
  encode_bool(bufp, start_inclusive);
  encode_vstr(bufp, end_row);
  encode_vstr(bufp, end_column);
  encode_bool(bufp, end_inclusive);
}


void CellInterval::decode(const uint8_t **bufp, size_t *remainp) {
  HT_TRY("decoding cell interval",
    start_row = decode_vstr(bufp, remainp);
    start_column = decode_vstr(bufp, remainp);
    start_inclusive = decode_bool(bufp, remainp);
    end_row = decode_vstr(bufp, remainp);
    end_column = decode_vstr(bufp, remainp);
    end_inclusive = decode_bool(bufp, remainp));
}

size_t ScanSpec::encoded_length() const {
  size_t len = encoded_length_vi32(row_limit) +
               encoded_length_vi32(cell_limit) +
               encoded_length_vi32(max_versions) +
               encoded_length_vi32(columns.size()) +
               encoded_length_vi32(row_intervals.size()) +
               encoded_length_vi32(cell_intervals.size()) +
               encoded_length_vstr(row_regexp) +
               encoded_length_vstr(value_regexp);

  foreach(const char *c, columns) len += encoded_length_vstr(c);
  foreach(const RowInterval &ri, row_intervals) len += ri.encoded_length();
  foreach(const CellInterval &ci, cell_intervals) len += ci.encoded_length();

  return len + 8 + 8 + 2;
}

void ScanSpec::encode(uint8_t **bufp) const {
  encode_vi32(bufp, row_limit);
  encode_vi32(bufp, cell_limit);
  encode_vi32(bufp, max_versions);
  encode_vi32(bufp, columns.size());
  foreach(const char *c, columns) encode_vstr(bufp, c);
  encode_vi32(bufp, row_intervals.size());
  foreach(const RowInterval &ri, row_intervals) ri.encode(bufp);
  encode_vi32(bufp, cell_intervals.size());
  foreach(const CellInterval &ci, cell_intervals) ci.encode(bufp);
  encode_i64(bufp, time_interval.first);
  encode_i64(bufp, time_interval.second);
  encode_bool(bufp, return_deletes);
  encode_bool(bufp, keys_only);
  encode_vstr(bufp, row_regexp);
  encode_vstr(bufp, value_regexp);
}

void ScanSpec::decode(const uint8_t **bufp, size_t *remainp) {
  RowInterval ri;
  CellInterval ci;
  HT_TRY("decoding scan spec",
    row_limit = decode_vi32(bufp, remainp);
    cell_limit = decode_vi32(bufp, remainp);
    max_versions = decode_vi32(bufp, remainp);
    for (size_t nc = decode_vi32(bufp, remainp); nc--;)
      columns.push_back(decode_vstr(bufp, remainp));
    for (size_t nri = decode_vi32(bufp, remainp); nri--;) {
      ri.decode(bufp, remainp);
      row_intervals.push_back(ri);
    }
    for (size_t nci = decode_vi32(bufp, remainp); nci--;) {
      ci.decode(bufp, remainp);
      cell_intervals.push_back(ci);
    }
    time_interval.first = decode_i64(bufp, remainp);
    time_interval.second = decode_i64(bufp, remainp);
    return_deletes = decode_i8(bufp, remainp);
    keys_only = decode_i8(bufp, remainp));
    row_regexp = decode_vstr(bufp, remainp);
    value_regexp = decode_vstr(bufp, remainp);
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

ostream &Hypertable::operator<<(ostream &os, const CellInterval &ci) {
  os <<"{CellInterval: ";
  if (ci.start_row)
    os << "\"" << ci.start_row << "\",\"" << ci.start_column << "\"";
  else
    os << "NULL";
  if (ci.start_inclusive)
    os << " <= cell ";
  else
    os << " < cell ";
  if (ci.end_inclusive)
    os << "<= ";
  else
    os << "< ";
  if (ci.end_row)
    os << "\"" << ci.end_row << "\",\"" << ci.end_column << "\"";
  else
    os << "0xff 0xff";
  os << "}";
  return os;
}


ostream &Hypertable::operator<<(ostream &os, const ScanSpec &scan_spec) {
  os <<"\n{ScanSpec: row_limit="<< scan_spec.row_limit
     <<" cell_limit="<< scan_spec.cell_limit
     <<" max_versions="<< scan_spec.max_versions
     <<" return_deletes="<< scan_spec.return_deletes
     <<" keys_only="<< scan_spec.keys_only;
  if (scan_spec.row_regexp)
    os << " row_regexp=" << scan_spec.row_regexp;
  if (scan_spec.value_regexp)
    os << " value_regexp=" << scan_spec.value_regexp;

  if (!scan_spec.row_intervals.empty()) {
    os << "\n rows=";
    foreach(const RowInterval &ri, scan_spec.row_intervals)
      os << " " << ri;
  }
  if (!scan_spec.cell_intervals.empty()) {
    os << "\n cells=";
    foreach(const CellInterval &ci, scan_spec.cell_intervals)
      os << " " << ci;
  }
  if (!scan_spec.columns.empty()) {
    os << "\n columns=(";
    foreach (const char *c, scan_spec.columns)
      os <<"'"<< c << "' ";
    os <<')';
  }
  os <<"\n time_interval=(" << scan_spec.time_interval.first <<", "
     << scan_spec.time_interval.second <<")\n}\n";

  return os;
}


ScanSpec::ScanSpec(CharArena &arena, const ScanSpec &ss)
  : row_limit(ss.row_limit), cell_limit(ss.cell_limit), max_versions(ss.max_versions),
    columns(CstrAlloc(arena)), row_intervals(RowIntervalAlloc(arena)),
    cell_intervals(CellIntervalAlloc(arena)),
    time_interval(ss.time_interval.first, ss.time_interval.second),
    return_deletes(ss.return_deletes), keys_only(ss.keys_only) {
  columns.reserve(ss.columns.size());
  row_intervals.reserve(ss.row_intervals.size());
  cell_intervals.reserve(ss.cell_intervals.size());

  foreach(const char *c, ss.columns)
    add_column(arena, c);

  foreach(const RowInterval &ri, ss.row_intervals)
    add_row_interval(arena, ri.start, ri.start_inclusive,
                     ri.end, ri.end_inclusive);

  foreach(const CellInterval &ci, ss.cell_intervals)
    add_cell_interval(arena, ci.start_row, ci.start_column, ci.start_inclusive,
                      ci.end_row, ci.end_column, ci.end_inclusive);
}

void ScanSpec::parse_column(const char *column_str, String &family, String &qualifier,
    bool *regexp)
{
  String column = column_str;
  size_t pos = column.find_first_of(':');
  qualifier.clear();
  *regexp = false;

  if (pos == String::npos) {
    family = column;
  }
  else {
    family = column.substr(0, pos);
    if (column.length() > pos+1) {
      // has qualifier
      qualifier = column.substr(pos+1);
      if (column[pos+1] == '/') {
        *regexp = true;
        boost::trim_if(qualifier, boost::is_any_of("/"));
      }
      else if (column[pos+1] == '\"' || column[pos+1] == '\'') {
        boost::trim_if(qualifier, boost::is_any_of("\""));
      }
    }
  }
}

