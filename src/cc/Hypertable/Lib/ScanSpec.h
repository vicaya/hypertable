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

#ifndef HYPERTABLE_SCANSPEC_H
#define HYPERTABLE_SCANSPEC_H

#include <vector>

namespace Hypertable {

  /**
   * Represents a row interval.  c-string data members are not managed
   * so caller must handle deallocation.
   */
  class RowInterval {
  public:
    RowInterval();
    RowInterval(const uint8_t **bufp, size_t *remainp) { decode(bufp, remainp); }

    size_t encoded_length() const;
    void encode(uint8_t **bufp) const;
    void decode(const uint8_t **bufp, size_t *remainp);

    const char *start;
    bool start_inclusive;
    const char *end;
    bool end_inclusive;
  };
  

  /**
   * Represents a scan predicate.
   */
  class ScanSpec {
  public:
    ScanSpec();
    ScanSpec(const uint8_t **bufp, size_t *remainp) { decode(bufp, remainp); }

    size_t encoded_length() const;
    void encode(uint8_t **bufp) const;
    void decode(const uint8_t **bufp, size_t *remainp);

    void clear() {
      row_limit = 0;
      max_versions = 0;
      columns.clear();
      row_intervals.clear();
      time_interval.first = time_interval.second = 0;
      return_deletes = 0;
    }

    void base_copy(ScanSpec &other) {
      other.row_limit = row_limit;
      other.max_versions = max_versions;
      other.columns = columns;
      other.time_interval = time_interval;
      other.return_deletes = return_deletes;
      other.row_intervals.clear();
    }

    int32_t row_limit;
    uint32_t max_versions;
    std::vector<const char *> columns;
    std::vector<RowInterval> row_intervals;
    std::pair<int64_t,int64_t> time_interval;
    bool return_deletes;
  };

  /**
   * Helper class for building a ScanSpec.  This class manages the allocation
   * of all string members.
   */
  class ScanSpecBuilder : public ScanSpec {
  public:
    /**
     * Sets the maximum number of rows to return in the scan.
     *
     * @param n row limit
     */
    void set_row_limit(int32_t n) { row_limit = n; }

    /**
     * Sets the maximum number of revisions of each cell to return in the scan.
     *
     * @param n maximum revisions
     */
    void set_max_versions(uint32_t n) { max_versions = n; }

    /**
     * Adds a column family to be returned by the scan.
     *
     * @param str column family name
     */
    void add_column(const String &str) {
      m_strings.push_back(str);
      columns.push_back(m_strings.back().c_str());
    }

    /**
     * Adds a row to be returned in the scan
     *
     * @param str row key
     */
    void add_row(const String &str) {
      RowInterval ri;
      m_strings.push_back(str);
      ri.start = ri.end = m_strings.back().c_str();
      ri.start_inclusive = ri.end_inclusive = true;
      row_intervals.push_back(ri);
    }

    /**
     * Adds a row interval to be returned in the scan.
     *
     * @param start start row
     * @param start_inclusive true if interval should include start row
     * @param end end row
     * @param end_inclusive true if interval should include end row
     */
    void add_row_interval(const String &start, bool start_inclusive,
			  const String &end, bool end_inclusive) {
      RowInterval ri;
      m_strings.push_back(start);
      ri.start = m_strings.back().c_str();
      ri.start_inclusive = start_inclusive;
      m_strings.push_back(end);
      ri.end = m_strings.back().c_str();
      ri.end_inclusive = end_inclusive;
      row_intervals.push_back(ri);      
    }

    /**
     * Sets the time interval of the scan.  Time values represent number of
     * nanoseconds from 1970-01-00 00:00:00.000000000.
     *
     * @param start start time in nanoseconds
     * @param end end time in nanoseconds
     */
    void set_time_interval(int64_t start, int64_t end) {
      time_interval.first = start;
      time_interval.second = end;
    }

    /**
     * Internal use only.
     */
    void set_return_deletes(bool val) {
      return_deletes = val;
    }

    /**
     * Clears the state.
     */
    void clear() {
      ScanSpec::clear();
      m_strings.clear();
    }
    
  private:
    std::vector<String> m_strings;
  };

  extern const int64_t BEGINNING_OF_TIME;
  extern const int64_t END_OF_TIME;

  std::ostream &operator<<(std::ostream &os, const RowInterval &);

  std::ostream &operator<<(std::ostream &os, const ScanSpec &);

} // namespace Hypertable


#endif // HYPERTABLE_SCANSPEC_H
