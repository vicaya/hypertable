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

#ifndef HYPERTABLE_TYPES_H
#define HYPERTABLE_TYPES_H

#include <utility>
#include <vector>

#include "Common/ByteString.h"
#include "Common/String.h"

namespace Hypertable {

  /** Identifies a specific table and generation */
  class TableIdentifier {
  public:
    TableIdentifier() : name(0), id(0), generation(0) { return; }
    explicit TableIdentifier(const char *s) : name(s), id(0), generation(0) {}
    TableIdentifier(const uint8_t **bufp, size_t *remainp) {
      decode(bufp, remainp);
    }

    size_t encoded_length() const;
    void encode(uint8_t **bufp) const;
    void decode(const uint8_t **bufp, size_t *remainp);

    const char *name;
    uint32_t id;
    uint32_t generation;
  };

  /** Wrapper for TableIdentifier.  Handles name allocation */
  class TableIdentifierManaged : public TableIdentifier {
  public:
    TableIdentifierManaged(const TableIdentifier &identifier) {
      operator=(identifier);
    }
    TableIdentifierManaged &operator=(const TableIdentifier &identifier) {
      id = identifier.id;
      generation = identifier.generation;

      if (identifier.name) {
        m_name = identifier.name;
        name = m_name.c_str();
      }
      else
        name = 0;
      return *this;
    }

  private:
    String m_name;
  };

  /** Identifies a range */
  class RangeSpec {
  public:
    RangeSpec() : start_row(0), end_row(0) { return; }
    RangeSpec(const char *start, const char *end)
        : start_row(start), end_row(end) {}
    RangeSpec(const uint8_t **bufp, size_t *remainp) { decode(bufp, remainp); }

    size_t encoded_length() const;
    void encode(uint8_t **bufp) const;
    void decode(const uint8_t **bufp, size_t *remainp);

    const char *start_row;
    const char *end_row;
  };

  /** RangeSpec with storage */
  class RangeSpecManaged : public RangeSpec {
  public:
    RangeSpecManaged(const RangeSpec &range) { operator=(range); }

    RangeSpecManaged &operator=(const RangeSpec &range) {
      if (range.start_row) {
        m_start = range.start_row;
        start_row = m_start.c_str();
      }
      else
        start_row = 0;

      if (range.end_row) {
        m_end = range.end_row;
        end_row = m_end.c_str();
      }
      else
        end_row = 0;
      return *this;
    }

  private:
    String m_start, m_end;
  };

  /** Scan specification */
  class ScanSpec {
  public:
    ScanSpec();
    ScanSpec(const uint8_t **bufp, size_t *remainp) { decode(bufp, remainp); }

    size_t encoded_length() const;
    void encode(uint8_t **bufp) const;
    void decode(const uint8_t **bufp, size_t *remainp);

    uint32_t row_limit;
    uint32_t max_versions;
    std::vector<const char *> columns;
    const char *start_row;
    bool start_row_inclusive;
    const char *end_row;
    bool end_row_inclusive;
    std::pair<uint64_t,uint64_t> interval;
    bool return_deletes;
  };

  extern const uint64_t END_OF_TIME;

  std::ostream &operator<<(std::ostream &os, const TableIdentifier &);

  std::ostream &operator<<(std::ostream &os, const RangeSpec &);

  std::ostream &operator<<(std::ostream &os, const ScanSpec &);


} // namespace Hypertable


#endif // HYPERTABLE_REQUEST_H
