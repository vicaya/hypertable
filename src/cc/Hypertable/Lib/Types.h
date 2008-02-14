/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
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
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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

namespace Hypertable {

  /** Identifies a specific table and generation */
  typedef struct {
    const char *name;
    uint32_t id;
    uint32_t generation;
  } TableIdentifierT;

  /** Identifies a range */
  typedef struct {
    const char *startRow;
    const char *endRow;
  } RangeT;

  /** Scan specification */
  typedef struct {
    uint32_t rowLimit;
    uint32_t max_versions;
    std::vector<const char *> columns;
    const char *startRow;
    bool startRowInclusive;
    const char *endRow;
    bool endRowInclusive;
    std::pair<uint64_t,uint64_t> interval;
    bool return_deletes;
  } ScanSpecificationT;

  typedef struct {
    uint8_t *buf;
    int32_t len;
  } BufferT;

  void Copy(TableIdentifierT &src, TableIdentifierT &dst);
  inline void Free(TableIdentifierT &identifier) { delete [] identifier.name; }

  /** Returns encoded (serialized) length of the given TableIdentifierT.
   *
   * @param table_identifier table identifier structure
   * @return encoded length of table identifier
   */
  size_t EncodedLengthTableIdentifier(TableIdentifierT &table_identifier);

  /** Encodes a TableIdentifierT into the given buffer. */
  void EncodeTableIdentifier(uint8_t **bufPtr, TableIdentifierT &table_identifier);

  /** Decodes a TableIdentifierT from the given buffer */
  bool DecodeTableIdentifier(uint8_t **bufPtr, size_t *remainingPtr, TableIdentifierT *table_identifier);

  /** Returns encoded (serialized) length of a RangeT */
  size_t EncodedLengthRange(RangeT &range);

  /** Encodes a RangeT into the given buffer. */
  void EncodeRange(uint8_t **bufPtr, RangeT &range);

  /** Decodes a RangeT from the given buffer */
  bool DecodeRange(uint8_t **bufPtr, size_t *remainingPtr, RangeT *range);

  /** Returns encoded (serialized) length of a ScanSpecificationT structure. */
  size_t EncodedLengthScanSpecification(ScanSpecificationT &scanSpec);

  /** Encodes a ScanSpecificationT structure to the given buffer. */
  void EncodeScanSpecification(uint8_t **bufPtr, ScanSpecificationT &scanSpec);

  /** Decodes a ScanSpecificationT structure from the given buffer. */
  bool DecodeScanSpecification(uint8_t **bufPtr, size_t *remainingPtr, ScanSpecificationT *scanSpec);

  std::ostream &operator<<(std::ostream &os, const TableIdentifierT &table_identifier);

  std::ostream &operator<<(std::ostream &os, const RangeT &range);

  std::ostream &operator<<(std::ostream &os, const ScanSpecificationT &scannerSpec);

}


#endif // HYPERTABLE_REQUEST_H
