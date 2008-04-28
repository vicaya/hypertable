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
  class TableIdentifierWrapper {
  public:
    TableIdentifierWrapper(TableIdentifier *identifier) : m_name(identifier->name) {
      memcpy(&m_identifier, identifier, sizeof(TableIdentifier));
      m_identifier.name = m_name.c_str();
    }
    TableIdentifier *operator-> () { return &m_identifier; }
    operator TableIdentifier *() { return &m_identifier; }

  private:
    String m_name;
    TableIdentifier m_identifier;
  };

  /** Identifies a range */
  class RangeSpec {
  public:
    RangeSpec() : startRow(0), endRow(0) { return; }
    size_t encoded_length();
    void encode(uint8_t **bufPtr);
    bool decode(uint8_t **bufPtr, size_t *remainingPtr);
    const char *startRow;
    const char *endRow;
  };

  /** Scan specification */
  class ScanSpec {
  public:
    ScanSpec();
    size_t encoded_length();
    void encode(uint8_t **bufPtr);
    bool decode(uint8_t **bufPtr, size_t *remainingPtr);
    uint32_t rowLimit;
    uint32_t max_versions;
    std::vector<const char *> columns;
    const char *startRow;
    bool startRowInclusive;
    const char *endRow;
    bool endRowInclusive;
    std::pair<uint64_t,uint64_t> interval;
    bool return_deletes;
  };

  extern const uint64_t END_OF_TIME;

  typedef struct Buffer {
    uint8_t *buf;
    int32_t len;
  } BufferT;

  void Copy(TableIdentifier &src, TableIdentifier &dst);
  inline void Free(TableIdentifier &identifier) { delete [] identifier.name; }

  /** Returns encoded (serialized) length of the given TableIdentifier.
   *
   * @param table_identifier table identifier structure
   * @return encoded length of table identifier
   */
  size_t EncodedLengthTableIdentifier(TableIdentifier &table_identifier);

  /** Encodes a TableIdentifier into the given buffer. */
  void EncodeTableIdentifier(uint8_t **bufPtr, TableIdentifier &table_identifier);

  /** Decodes a TableIdentifier from the given buffer */
  bool DecodeTableIdentifier(uint8_t **bufPtr, size_t *remainingPtr, TableIdentifier *table_identifier);

  /** Returns encoded (serialized) length of a RangeSpec */
  size_t EncodedLengthRange(RangeSpec &range);

  /** Encodes a RangeSpec into the given buffer. */
  void EncodeRange(uint8_t **bufPtr, RangeSpec &range);

  /** Decodes a RangeSpec from the given buffer */
  bool DecodeRange(uint8_t **bufPtr, size_t *remainingPtr, RangeSpec *range);

  /** Returns encoded (serialized) length of a ScanSpec structure. */
  size_t EncodedLengthScanSpecification(ScanSpec &scanSpec);

  /** Encodes a ScanSpec structure to the given buffer. */
  void EncodeScanSpecification(uint8_t **bufPtr, ScanSpec &scanSpec);

  /** Decodes a ScanSpec structure from the given buffer. */
  bool DecodeScanSpecification(uint8_t **bufPtr, size_t *remainingPtr, ScanSpec *scanSpec);

  std::ostream &operator<<(std::ostream &os, const TableIdentifier &table_identifier);

  std::ostream &operator<<(std::ostream &os, const RangeSpec &range);

  std::ostream &operator<<(std::ostream &os, const ScanSpec &scannerSpec);

}


#endif // HYPERTABLE_REQUEST_H
