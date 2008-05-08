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

extern "C" {
#include <stddef.h>
#include <stdint.h>
}

#include "Common/ByteString.h"
#include "Common/String.h"

namespace Hypertable {

  /** Identifies a specific table and generation */
  class TableIdentifier {
  public:
    TableIdentifier() : name(0), id(0), generation(0) { return; }
    size_t encoded_length();
    void encode(uint8_t **bufPtr);
    bool decode(uint8_t **bufPtr, size_t *remainingPtr);
    const char *name;
    uint32_t id;
    uint32_t generation;
  };

  /** Wrapper for TableIdentifier.  Handles name allocation */
  class TableIdentifierCopy : public TableIdentifier {
  public:
    TableIdentifierCopy(const TableIdentifier *identifier) : m_name(identifier->name) {
      id = identifier->id;
      generation = identifier->generation;
      name = m_name.c_str();
    }
    TableIdentifierCopy(const TableIdentifier &identifier) : m_name(identifier.name) {
      id = identifier.id;
      generation = identifier.generation;
      name = m_name.c_str();
    }
    TableIdentifierCopy &operator=(const TableIdentifier &identifier) {
      m_name = identifier.name;
      id = identifier.id;
      generation = identifier.generation;
      name = m_name.c_str();
      return *this;
    }

  private:
    String m_name;
  };

  /** Identifies a range */
  class RangeSpec {
  public:
    RangeSpec() : start_row(0), end_row(0) { return; }
    size_t encoded_length();
    void encode(uint8_t **bufPtr);
    bool decode(uint8_t **bufPtr, size_t *remainingPtr);
    const char *start_row;
    const char *end_row;
  };

  /** Scan specification */
  class ScanSpec {
  public:
    ScanSpec();
    size_t encoded_length();
    void encode(uint8_t **bufPtr);
    bool decode(uint8_t **bufPtr, size_t *remainingPtr);
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

  std::ostream &operator<<(std::ostream &os, const TableIdentifier &table_identifier);

  std::ostream &operator<<(std::ostream &os, const RangeSpec &range);

  std::ostream &operator<<(std::ostream &os, const ScanSpec &scannerSpec);

}


#endif // HYPERTABLE_REQUEST_H
