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

#ifndef HYPERTABLE_TYPES_H
#define HYPERTABLE_TYPES_H

#include <utility>
#include <vector>

#include "Common/ByteString.h"
#include "Common/MurmurHash.h"
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
    TableIdentifierManaged() { name = NULL; id = 0; generation = 0; }
    TableIdentifierManaged(const TableIdentifierManaged &identifier) {
      operator=(identifier);
    }
    TableIdentifierManaged(const TableIdentifier &identifier) {
      operator=(identifier);
    }
    TableIdentifierManaged &operator=(const TableIdentifierManaged &other) {
      const TableIdentifier *otherp = &other;
      return operator=(*otherp);
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

    void set_name(const char *new_name) {
      m_name = new_name;
      name = m_name.c_str();
    }

    String get_name() const {
      return m_name;
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
    RangeSpecManaged() { start_row = end_row = 0; }
    RangeSpecManaged(const RangeSpecManaged &range) { operator=(range); }
    RangeSpecManaged(const RangeSpec &range) { operator=(range); }

    RangeSpecManaged &operator=(const RangeSpecManaged &other) {
      const RangeSpec *otherp = &other;
      return operator=(*otherp);
    }
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


  /** RangeSpec with table id */
  class QualifiedRangeSpec {
  public:
    QualifiedRangeSpec(const TableIdentifier &tid, const RangeSpec &rs)
      : table(tid), range(rs) {}
    TableIdentifier table;
    RangeSpec range;
  };

  class QualifiedRangeHash {
  public:
    size_t operator()(const QualifiedRangeSpec &spec) const {
      return murmurhash2(spec.range.start_row, strlen(spec.range.start_row),
                         murmurhash2(spec.range.end_row,
                                     strlen(spec.range.end_row),
                                     murmurhash2(&spec.table.id, 4, 0)));
    }
  };

  struct QualifiedRangeEqual {
    bool
    operator()(const QualifiedRangeSpec &x, const QualifiedRangeSpec &y) const {
      return x.table.id == y.table.id
             && !strcmp(x.range.start_row, y.range.start_row)
             && !strcmp(x.range.end_row, y.range.end_row);
    }
  };

  std::ostream &operator<<(std::ostream &os, const TableIdentifier &tid);

  std::ostream &operator<<(std::ostream &os, const RangeSpec &range);


} // namespace Hypertable


#endif // HYPERTABLE_REQUEST_H
