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

#include "Common/CharArena.h"
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


  /**
   * Represents a scan predicate.
   */
  class ScanSpec {
  public:
    ScanSpec() : row_limit(0), max_versions(0),
        time_interval(TIMESTAMP_MIN, TIMESTAMP_MAX), return_deletes(false) { }
    ScanSpec(const uint8_t **bufp, size_t *remainp) { decode(bufp, remainp); }

    size_t encoded_length() const;
    void encode(uint8_t **bufp) const;
    void decode(const uint8_t **bufp, size_t *remainp);

    void clear() {
      row_limit = 0;
      max_versions = 0;
      columns.clear();
      row_intervals.clear();
      cell_intervals.clear();
      time_interval.first = TIMESTAMP_MIN;
      time_interval.second = TIMESTAMP_MAX;
      return_deletes = 0;
    }

    /** Initialize 'other' ScanSpec with this copy sans the intervals */
    void base_copy(ScanSpec &other) const {
      other.row_limit = row_limit;
      other.max_versions = max_versions;
      other.columns = columns;
      other.time_interval = time_interval;
      other.return_deletes = return_deletes;
      other.row_intervals.clear();
      other.cell_intervals.clear();
    }

    void swap(ScanSpec &ss) {
      std::swap(row_limit, ss.row_limit);
      std::swap(max_versions, ss.max_versions);
      row_intervals.swap(ss.row_intervals);
      cell_intervals.swap(ss.cell_intervals);
      std::swap(time_interval, ss.time_interval);
      std::swap(return_deletes, ss.return_deletes);
    }

    int32_t row_limit;
    uint32_t max_versions;
    std::vector<const char *> columns;
    std::vector<RowInterval> row_intervals;
    std::vector<CellInterval> cell_intervals;
    std::pair<int64_t,int64_t> time_interval;
    bool return_deletes;
  };

  /**
   * Helper class for building a ScanSpec.  This class manages the allocation
   * of all string members.
   */
  class ScanSpecBuilder : boost::noncopyable {
  public:
    ScanSpecBuilder() { }
    /** Copy construct from a ScanSpec */
    ScanSpecBuilder(const ScanSpec &);

    /**
     * Sets the maximum number of rows to return in the scan.
     *
     * @param n row limit
     */
    void set_row_limit(int32_t n) { m_scan_spec.row_limit = n; }

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
      m_scan_spec.columns.push_back(m_alloc.dup(str));
    }

    /**
     * Adds a row to be returned in the scan
     *
     * @param str row key
     */
    void add_row(const char *str) {
      if (m_scan_spec.cell_intervals.size())
        HT_THROW(Error::BAD_SCAN_SPEC, "cell spec excludes rows");
      RowInterval ri;
      ri.start = ri.end = m_alloc.dup(str);
      ri.start_inclusive = ri.end_inclusive = true;
      m_scan_spec.row_intervals.push_back(ri);
    }

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
      if (m_scan_spec.cell_intervals.size())
        HT_THROW(Error::BAD_SCAN_SPEC, "cell spec excludes rows");
      RowInterval ri;
      ri.start = m_alloc.dup(start);
      ri.start_inclusive = start_inclusive;
      ri.end = m_alloc.dup(end);
      ri.end_inclusive = end_inclusive;
      m_scan_spec.row_intervals.push_back(ri);
    }

    /**
     * Adds a cell to be returned in the scan
     *
     * @param str row key
     */
    void add_cell(const char *row, const char *column) {
      if (m_scan_spec.row_intervals.size())
        HT_THROW(Error::BAD_SCAN_SPEC, "row spec excludes cells");
      CellInterval ci;
      ci.start_row = ci.end_row = m_alloc.dup(row);
      ci.start_column = ci.end_column = m_alloc.dup(column);
      ci.start_inclusive = ci.end_inclusive = true;
      m_scan_spec.cell_intervals.push_back(ci);
    }

    /**
     * Adds a cell interval to be returned in the scan.
     *
     * @param start_row start row
     * @param start_column start column
     * @param start_inclusive true if interval should include start row
     * @param end_row end row
     * @param end_column end column
     * @param end_inclusive true if interval should include end row
     */
    void add_cell_interval(const char *start_row, const char *start_column,
                           bool start_inclusive, const char *end_row,
                           const char *end_column, bool end_inclusive) {
      if (m_scan_spec.row_intervals.size())
        HT_THROW(Error::BAD_SCAN_SPEC, "row spec excludes cells");
      CellInterval ci;
      ci.start_row = m_alloc.dup(start_row);
      ci.start_column = m_alloc.dup(start_column);
      ci.start_inclusive = start_inclusive;
      ci.end_row = m_alloc.dup(end_row);
      ci.end_column = m_alloc.dup(end_column);
      ci.end_inclusive = end_inclusive;
      m_scan_spec.cell_intervals.push_back(ci);
    }

    /**
     * Sets the time interval of the scan.  Time values represent number of
     * nanoseconds from 1970-01-00 00:00:00.000000000.
     *
     * @param start start time in nanoseconds
     * @param end end time in nanoseconds
     */
    void set_time_interval(int64_t start, int64_t end) {
      m_scan_spec.time_interval.first = start;
      m_scan_spec.time_interval.second = end;
    }

    void set_start_time(int64_t start) {
      m_scan_spec.time_interval.first = start;
    }

    void set_end_time(int64_t end) {
      m_scan_spec.time_interval.second = end;
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
      m_alloc.free();
    }

    /**
     * Returns the built ScanSpec object
     *
     * @return reference to built ScanSpec object
     */
    ScanSpec &get() { return m_scan_spec; }

    /**
     * An O(1) swap operation
     */
    void swap(ScanSpecBuilder &ssb) {
      m_alloc.swap(ssb.m_alloc);
      m_scan_spec.swap(ssb.m_scan_spec);
    }

  private:
    CharArena m_alloc;
    ScanSpec m_scan_spec;
  };

  std::ostream &operator<<(std::ostream &os, const RowInterval &);

  std::ostream &operator<<(std::ostream &os, const CellInterval &);

  std::ostream &operator<<(std::ostream &os, const ScanSpec &);

} // namespace Hypertable

#endif // HYPERTABLE_SCANSPEC_H
