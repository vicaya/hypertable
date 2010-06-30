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

#ifndef HYPERTABLE_SCANSPEC_H
#define HYPERTABLE_SCANSPEC_H

#include <boost/noncopyable.hpp>

#include <vector>

#include "Common/PageArenaAllocator.h"
#include "KeySpec.h"

namespace Hypertable {

/**
 * Represents a row interval.  c-string data members are not managed
 * so caller must handle (de)allocation.
 */
class RowInterval {
public:
  RowInterval() : start(0), start_inclusive(true), end(0),
      end_inclusive(true) { }
  RowInterval(const char *start_row, bool start_row_inclusive,
              const char *end_row, bool end_row_inclusive)
    : start(start_row), start_inclusive(start_row_inclusive),
      end(end_row), end_inclusive(end_row_inclusive) { }
  RowInterval(const uint8_t **bufp, size_t *remainp) {
    decode(bufp, remainp);
  }

  size_t encoded_length() const;
  void encode(uint8_t **bufp) const;
  void decode(const uint8_t **bufp, size_t *remainp);

  const char *start;
  bool start_inclusive;
  const char *end;
  bool end_inclusive;
};


/**
 * Represents a cell interval.  c-string data members are not managed
 * so caller must handle (de)allocation.
 */
class CellInterval {
public:
  CellInterval() : start_row(0), start_column(0), start_inclusive(true),
      end_row(0), end_column(0), end_inclusive(true) { }
  CellInterval(const char *start_row, const char *start_column,
               bool start_inclusive, const char *end_row,
               const char *end_column, bool end_inclusive)
    : start_row(start_row), start_column(start_column),
      start_inclusive(start_inclusive), end_row(end_row),
      end_column(end_column), end_inclusive(end_inclusive) { }
  CellInterval(const uint8_t **bufp, size_t *remainp) {
    decode(bufp, remainp);
  }

  size_t encoded_length() const;
  void encode(uint8_t **bufp) const;
  void decode(const uint8_t **bufp, size_t *remainp);

  const char *start_row;
  const char *start_column;
  bool start_inclusive;
  const char *end_row;
  const char *end_column;
  bool end_inclusive;
};

typedef PageArenaAllocator<RowInterval> RowIntervalAlloc;
typedef std::vector<RowInterval, RowIntervalAlloc> RowIntervals;

typedef PageArenaAllocator<CellInterval> CellIntervalAlloc;
typedef std::vector<CellInterval, CellIntervalAlloc> CellIntervals;

typedef PageArenaAllocator<const char *> CstrAlloc;
typedef std::vector<const char *, CstrAlloc> CstrColumns;

/**
 * Represents a scan predicate.
 */
class ScanSpec {
public:
  ScanSpec()
    : row_limit(0), cell_limit(0), max_versions(0),
      time_interval(TIMESTAMP_MIN, TIMESTAMP_MAX),
      return_deletes(false), keys_only(false) { }
  ScanSpec(CharArena &arena)
    : row_limit(0), cell_limit(0), max_versions(0), columns(CstrAlloc(arena)),
      row_intervals(RowIntervalAlloc(arena)),
      cell_intervals(CellIntervalAlloc(arena)),
      time_interval(TIMESTAMP_MIN, TIMESTAMP_MAX),
      return_deletes(false), keys_only(false) { }
  ScanSpec(CharArena &arena, const ScanSpec &);
  ScanSpec(const uint8_t **bufp, size_t *remainp) { decode(bufp, remainp); }

  size_t encoded_length() const;
  void encode(uint8_t **bufp) const;
  void decode(const uint8_t **bufp, size_t *remainp);

  void clear() {
    row_limit = 0;
    cell_limit = 0;
    max_versions = 0;
    columns.clear();
    row_intervals.clear();
    cell_intervals.clear();
    time_interval.first = TIMESTAMP_MIN;
    time_interval.second = TIMESTAMP_MAX;
    keys_only = false;
    return_deletes = false;
  }

  /** Initialize 'other' ScanSpec with this copy sans the intervals */
  void base_copy(ScanSpec &other) const {
    other.row_limit = row_limit;
    other.cell_limit = cell_limit;
    other.max_versions = max_versions;
    other.columns = columns;
    other.time_interval = time_interval;
    other.keys_only = keys_only;
    other.return_deletes = return_deletes;
    other.row_intervals.clear();
    other.cell_intervals.clear();
  }

  bool cacheable() {
    if (row_intervals.size() == 1) {
      HT_ASSERT(row_intervals[0].start && row_intervals[0].end);
      if (!strcmp(row_intervals[0].start, row_intervals[0].end))
	return true;
    }
    else if (cell_intervals.size() == 1) {
      HT_ASSERT(cell_intervals[0].start_row && cell_intervals[0].end_row);
      if (!strcmp(cell_intervals[0].start_row, cell_intervals[0].end_row))
	return true;
    }
    return false;
  }

  const char *cache_key() const {
    if (!row_intervals.empty())
      return row_intervals[0].start;
    else if (!cell_intervals.empty())
      return cell_intervals[0].start_row;
    HT_ASSERT(!"cache key not found");
    return 0;
  }

  void add_column(CharArena &arena, const char *str) {
    columns.push_back(arena.dup(str));
  }

  void add_row(CharArena &arena, const char *str) {
    if (cell_intervals.size())
      HT_THROW(Error::BAD_SCAN_SPEC, "cell spec excludes rows");

    RowInterval ri;
    ri.start = ri.end = arena.dup(str);
    ri.start_inclusive = ri.end_inclusive = true;
    row_intervals.push_back(ri);
  }

  void add_row_interval(CharArena &arena,
                        const char *start, bool start_inclusive,
                        const char *end, bool end_inclusive) {
    if (cell_intervals.size())
      HT_THROW(Error::BAD_SCAN_SPEC, "cell spec excludes rows");

    RowInterval ri;
    ri.start = arena.dup(start);
    ri.start_inclusive = start_inclusive;
    ri.end = arena.dup(end);
    ri.end_inclusive = end_inclusive;
    row_intervals.push_back(ri);
  }

  void add_cell(CharArena &arena, const char *row, const char *column) {
    if (row_intervals.size())
      HT_THROW(Error::BAD_SCAN_SPEC, "row spec excludes cells");

    CellInterval ci;
    ci.start_row = ci.end_row = arena.dup(row);
    ci.start_column = ci.end_column = arena.dup(column);
    ci.start_inclusive = ci.end_inclusive = true;
    cell_intervals.push_back(ci);
  }

  void add_cell_interval(CharArena &arena,
                         const char *start_row, const char *start_column,
                         bool start_inclusive, const char *end_row,
                         const char *end_column, bool end_inclusive) {
    if (row_intervals.size())
      HT_THROW(Error::BAD_SCAN_SPEC, "row spec excludes cells");

    CellInterval ci;
    ci.start_row = arena.dup(start_row);
    ci.start_column = arena.dup(start_column);
    ci.start_inclusive = start_inclusive;
    ci.end_row = arena.dup(end_row);
    ci.end_column = arena.dup(end_column);
    ci.end_inclusive = end_inclusive;
    cell_intervals.push_back(ci);
  }

  void set_time_interval(int64_t start, int64_t end) {
    time_interval.first = start;
    time_interval.second = end;
  }

  void set_start_time(int64_t start) {
    time_interval.first = start;
  }

  void set_end_time(int64_t end) {
    time_interval.second = end;
  }

  int32_t row_limit;
  int32_t cell_limit;
  uint32_t max_versions;
  CstrColumns columns;
  RowIntervals row_intervals;
  CellIntervals cell_intervals;
  std::pair<int64_t,int64_t> time_interval;
  bool return_deletes;
  bool keys_only;
};

/**
 * Helper class for building a ScanSpec.  This class manages the allocation
 * of all string members.
 */
class ScanSpecBuilder : boost::noncopyable {
public:
  ScanSpecBuilder() : m_scan_spec(m_arena) { }
  /** Copy construct from a ScanSpec */
  ScanSpecBuilder(const ScanSpec &ss) : m_scan_spec(m_arena, ss) {}

  /**
   * Sets the maximum number of rows to return in the scan.
   *
   * @param n row limit
   */
  void set_row_limit(int32_t n) { m_scan_spec.row_limit = n; }

  /**
   * Sets the maximum number of cells to return per column family, per row
   *
   * @param n cell limit
   */
  void set_cell_limit(int32_t n) { m_scan_spec.cell_limit = n; }

  /**
   * Sets the maximum number of revisions of each cell to return in the scan.
   *
   * @param n maximum revisions
   */
  void set_max_versions(uint32_t n) { m_scan_spec.max_versions = n; }

  /**
   * Adds a column family to be returned by the scan.
   *
   * @param str column family name
   */
  void add_column(const char *str) {
    m_scan_spec.add_column(m_arena, str);
  }

  void reserve_columns(size_t s) { m_scan_spec.columns.reserve(s); }

  /**
   * Adds a row to be returned in the scan
   *
   * @param str row key
   */
  void add_row(const char *str) {
    m_scan_spec.add_row(m_arena, str);
  }

  void reserve_rows(size_t s) { m_scan_spec.row_intervals.reserve(s); }

  /**
   * Adds a row interval to be returned in the scan.
   *
   * @param start start row
   * @param start_inclusive true if interval should include start row
   * @param end end row
   * @param end_inclusive true if interval should include end row
   */
  void add_row_interval(const char *start, bool start_inclusive,
                        const char *end, bool end_inclusive) {
    m_scan_spec.add_row_interval(m_arena, start, start_inclusive,
                                 end, end_inclusive);
  }

  /**
   * Adds a cell to be returned in the scan
   *
   * @param row row key
   * @param column column spec of the form &lt;family&gt;[:&lt;qualifier&gt;]
   */
  void add_cell(const char *row, const char *column) {
    m_scan_spec.add_cell(m_arena, row, column);
  }

  void reserve_cells(size_t s) { m_scan_spec.cell_intervals.reserve(s); }

  /**
   * Adds a cell interval to be returned in the scan.
   *
   * @param start_row start row
   * @param start_column start column spec of the form
   *        &lt;family&gt;[:&lt;qualifier&gt;]
   * @param start_inclusive true if interval should include start row
   * @param end_row end row
   * @param end_column end column spec of the form
   *        &lt;family&gt;[:&lt;qualifier&gt;]
   * @param end_inclusive true if interval should include end row
   */
  void add_cell_interval(const char *start_row, const char *start_column,
                         bool start_inclusive, const char *end_row,
                         const char *end_column, bool end_inclusive) {
    m_scan_spec.add_cell_interval(m_arena, start_row, start_column,
        start_inclusive, end_row, end_column, end_inclusive);
  }

  /**
   * Sets the time interval of the scan.  Time values represent number of
   * nanoseconds from 1970-01-00 00:00:00.000000000.
   *
   * @param start start time in nanoseconds
   * @param end end time in nanoseconds
   */
  void set_time_interval(int64_t start, int64_t end) {
    m_scan_spec.set_time_interval(start, end);
  }

  void set_start_time(int64_t start) {
    m_scan_spec.time_interval.first = start;
  }

  void set_end_time(int64_t end) {
    m_scan_spec.time_interval.second = end;
  }

  /**
   * Return only keys (no values)
   */
  void set_keys_only(bool val) {
    m_scan_spec.keys_only = val;
  }

  /**
   * Internal use only.
   */
  void set_return_deletes(bool val) {
    m_scan_spec.return_deletes = val;
  }

  /**
   * Clears the state.
   */
  void clear() {
    m_scan_spec.clear();
    // Don't call m_arena.free() here, as for stl containers (vector etc.),
    // clear() assumes underlying storage is still intact!
  }

  /**
   * Returns the built ScanSpec object
   *
   * @return reference to built ScanSpec object
   */
  ScanSpec &get() { return m_scan_spec; }

private:
  CharArena m_arena;
  ScanSpec m_scan_spec;
};

std::ostream &operator<<(std::ostream &os, const RowInterval &ri);

std::ostream &operator<<(std::ostream &os, const CellInterval &ci);

std::ostream &operator<<(std::ostream &os, const ScanSpec &scan_spec);

} // namespace Hypertable

#endif // HYPERTABLE_SCANSPEC_H
