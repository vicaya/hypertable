/** -*- C++ -*-
 * Copyright (C) 2008  Luke Lu (Zvents, Inc.)
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#include "Common/Compat.h"
#include "Common/TimeInline.h"

#include <time.h>
#include <iostream>

#include "ThriftHelper.h"

namespace Hypertable { namespace ThriftGen {

std::ostream &operator<<(std::ostream &out, const RowInterval &ri) {
  out <<"{RowInterval:";

  if (ri.__isset.start_row)
    out <<" start='"<< ri.start_row <<"'";

  if (ri.__isset.start_inclusive)
    out <<" start_inclusive="<< ri.start_inclusive;

  if (ri.__isset.end_row)
    out <<" end='"<< ri.end_row <<"'";

  if (ri.__isset.end_inclusive)
    out <<" end_inclusive="<< ri.end_inclusive;

  return out <<"}";
}

std::ostream &operator<<(std::ostream &out, const Key &key) {
  out <<"{Key:";

  if (key.__isset.row)
    out <<" row='"<< key.row <<"'";

  if (key.__isset.column_family)
    out <<" cf='"<< key.column_family <<"'";

  if (key.__isset.column_qualifier)
    out <<" cq='"<< key.column_qualifier <<"'";

  if (key.__isset.timestamp)
    out << " ts="<< key.timestamp;

  if (key.__isset.revision)
    out <<" rev="<< key.revision;

  if (key.__isset.flag)
    out << " flag="<< key.flag;

  return out <<"}";
}

std::ostream &operator<<(std::ostream &out, const Cell &cell) {
  out <<"{Cell:";

  out << " " << cell.key;

  if (cell.__isset.value)
    out <<" value='"<< cell.value <<"'";

  return out <<"}";
}

std::ostream &operator<<(std::ostream &out, const CellAsArray &cell) {
  size_t len = cell.size();

  out <<"{CellAsArray:";

  if (len > 0)
    out <<" row='"<< cell[0] <<"'";

  if (len > 1)
    out <<" cf='"<< cell[1] <<"'";

  if (len > 2)
    out <<" cq='"<< cell[2] <<"'";

  if (len > 3)
    out <<" value='"<< cell[3] <<"'";

  if (len > 4)
    out << " ts="<< cell[4];

  if (len > 5)
    out <<" rev="<< cell[5];

  if (len > 6)
    out << " flag="<< cell[6];

  return out <<"}";
}

std::ostream &operator<<(std::ostream &out, const CellInterval &ci) {
  out <<"{CellInterval:";

  if (ci.__isset.start_row)
    out <<" start_row='"<< ci.start_row <<"'";

  if (ci.__isset.start_column)
    out <<" start_column='"<< ci.start_column <<"'";

  if (ci.__isset.start_inclusive)
    out <<" start_inclusive="<< ci.start_inclusive;

  if (ci.__isset.end_row)
    out <<" end_row='"<< ci.end_row <<"'";

  if (ci.__isset.end_column)
    out <<" end_column='"<< ci.end_column <<"'";

  if (ci.__isset.end_inclusive)
    out <<" end_inclusive="<< ci.end_inclusive;

  return out <<"}";
}

std::ostream &operator<<(std::ostream &out, const ScanSpec &ss) {
  out <<"{ScanSpec:";

  if (ss.__isset.row_intervals) {
    out <<" rows=[\n";
    foreach(const RowInterval &ri, ss.row_intervals)
      out <<"  "<< ri <<"\n";
    out <<"  ]\n";
  }
  if (ss.__isset.cell_intervals) {
    out <<" cells=[\n";
    foreach(const CellInterval &ci, ss.cell_intervals)
      out <<"  "<< ci <<"\n";
    out <<"  ]\n";
  }
  if (ss.__isset.columns) {
    out <<" columns=[\n";
    foreach(const std::string &col, ss.columns)
      out <<"  "<< col <<"\n";
    out <<"  ]\n";
  }
  if (ss.__isset.row_limit)
    out <<" row_limit="<< ss.row_limit;

  if (ss.__isset.revs)
    out <<" revs="<< ss.revs;

  if (ss.__isset.return_deletes)
    out <<" return_deletes="<< ss.return_deletes;

  if (ss.__isset.keys_only)
    out <<" keys_only="<< ss.keys_only;

  if (ss.__isset.row_regexp)
    out <<" row_regexp="<< ss.row_regexp;

  if (ss.__isset.value_regexp)
    out <<" value_regexp="<< ss.value_regexp;

  if (ss.__isset.start_time)
    out <<" start_time="<< ss.start_time;

  if (ss.__isset.end_time)
    out <<" end_time="<< ss.end_time;

  if (ss.__isset.scan_and_filter_rows)
    out <<" scan_and_filter_rows="<< ss.scan_and_filter_rows;

  return out <<'}';
}

std::ostream &operator<<(std::ostream &out, const HqlResult &hr) {
  out <<"{HqlResult:";

  if (hr.__isset.results) {
    out <<" results=[";
    foreach(const std::string &s, hr.results)
      out <<"  '"<< s <<"'\n";
    out <<"  ]\n";
  }
  if (hr.__isset.cells) {
    out <<" cells=[\n";
    foreach(const Cell &cell, hr.cells)
      out <<"  "<< cell <<"\n";
    out <<"  ]\n";
  }
  if (hr.__isset.scanner)
    out <<" scanner="<< hr.scanner;

  if (hr.__isset.mutator)
    out <<" mutator="<< hr.mutator;

  return out <<'}';
}

std::ostream &operator<<(std::ostream &out, const HqlResult2 &hr) {
  out <<"{HqlResult2:";

  if (hr.__isset.results) {
    out <<" results=[";
    foreach(const std::string &s, hr.results)
      out <<"  '"<< s <<"'\n";
    out <<"  ]\n";
  }
  if (hr.__isset.cells) {
    out <<" cells=[\n";
    foreach(const CellAsArray &cell, hr.cells)
      out <<"  "<< cell <<"\n";
    out <<"  ]\n";
  }
  if (hr.__isset.scanner)
    out <<" scanner="<< hr.scanner;

  if (hr.__isset.mutator)
    out <<" mutator="<< hr.mutator;

  return out <<'}';
}

std::ostream &operator<<(std::ostream &out, const ClientException &e) {
  return out <<"{ClientException: code="<< e.code
             <<" message='"<< e.message <<"'}";
}

// must be synced with AUTO_ASSIGN in Hypertable/Lib/KeySpec.h
const int64_t AUTO_ASSIGN = INT64_MIN + 2;

Cell
make_cell(const char *row, const char *cf, const char *cq,
          const std::string &value, int64_t ts, int64_t rev,
          KeyFlag::type flag) {
  Cell cell;

  cell.key.row = row;
  cell.key.column_family = cf;
  cell.key.timestamp = ts;
  cell.key.revision = rev;
  cell.key.flag = flag;
  cell.key.__isset.row = cell.key.__isset.column_family = cell.key.__isset.timestamp
      = cell.key.__isset.revision = cell.key.__isset.flag = true;

  if (cq) {
    cell.key.column_qualifier = cq;
    cell.key.__isset.column_qualifier = true;
  }
  if (value.size()) {
    cell.value = value;
    cell.__isset.value = true;
  }
  return cell;
}

Cell
make_cell(const char *row, const char *cf, const char *cq,
          const std::string &value, const char *ts, const char *rev,
          KeyFlag::type flag) {
  int64_t revn = rev ? atoll(rev) : AUTO_ASSIGN;

  if (ts)
    return make_cell(row, cf, cq, value, parse_ts(ts), revn, flag);
  else
    return make_cell(row, cf, cq, value, AUTO_ASSIGN, revn, flag);
}

}} // namespace Hypertable::Thrift

