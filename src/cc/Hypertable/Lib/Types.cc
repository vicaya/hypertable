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

#include <cstring>
#include <iostream>

#include "AsyncComm/CommBuf.h"
#include "AsyncComm/Serialization.h"

#include "Types.h"

using namespace std;

namespace Hypertable {

  void Copy(TableIdentifierT &src, TableIdentifierT &dst) {
    memcpy(&dst, &src, sizeof(TableIdentifierT));
    dst.name = new char [ strlen(src.name) + 1 ];
    strcpy((char *)dst.name, src.name);
  }

  /** Returns encoded (serialized) length of the given TableIdentifierT.
   */
  size_t EncodedLengthTableIdentifier(TableIdentifierT &table_identifier) {
    return 8 + Serialization::encoded_length_string(table_identifier.name);
  }

  /** Encodes a TableIdentifierT into the given buffer. */
  void EncodeTableIdentifier(uint8_t **bufPtr, TableIdentifierT &table_identifier) {
    Serialization::encode_string(bufPtr, table_identifier.name);
    Serialization::encode_int(bufPtr, table_identifier.id);
    Serialization::encode_int(bufPtr, table_identifier.generation);
  }

  /** Decodes a TableIdentifierT from the given buffer */
  bool DecodeTableIdentifier(uint8_t **bufPtr, size_t *remainingPtr, TableIdentifierT *table_identifier) {
    memset(table_identifier, 0, sizeof(TableIdentifierT));
    if (!Serialization::decode_string(bufPtr, remainingPtr, &table_identifier->name))
      return false;
    if (!Serialization::decode_int(bufPtr, remainingPtr, &table_identifier->id))
      return false;
    if (!Serialization::decode_int(bufPtr, remainingPtr, &table_identifier->generation))
      return false;
    return true;
  }

  /** Returns encoded (serialized) length of a RangeT */
  size_t EncodedLengthRange(RangeT &range) {
    return Serialization::encoded_length_string(range.startRow) + Serialization::encoded_length_string(range.endRow);
  }

  /** Encodes a RangeT into the given buffer. */
  void EncodeRange(uint8_t **bufPtr, RangeT &range) {
    Serialization::encode_string(bufPtr, range.startRow);
    Serialization::encode_string(bufPtr, range.endRow);
  }

  /** Decodes a RangeT from the given buffer */
  bool DecodeRange(uint8_t **bufPtr, size_t *remainingPtr, RangeT *range) {
    memset(range, 0, sizeof(RangeT));
    if (!Serialization::decode_string(bufPtr, remainingPtr, &range->startRow))
      return false;
    if (!Serialization::decode_string(bufPtr, remainingPtr, &range->endRow))
      return false;
    return true;
  }


  /**
   *
   */
  size_t EncodedLengthScanSpecification(ScanSpecificationT &scanSpec) {
    size_t len = 29;
    len += Serialization::encoded_length_string(scanSpec.startRow);
    len += Serialization::encoded_length_string(scanSpec.endRow);
    for (size_t i=0; i<scanSpec.columns.size(); i++)
      len += Serialization::encoded_length_string(scanSpec.columns[i]);
    return len;
  }


  void EncodeScanSpecification(uint8_t **bufPtr, ScanSpecificationT &scanSpec) {
    Serialization::encode_int(bufPtr, scanSpec.rowLimit);
    Serialization::encode_int(bufPtr, scanSpec.max_versions);
    Serialization::encode_string(bufPtr, scanSpec.startRow);
    *(*bufPtr)++ = (uint8_t)scanSpec.startRowInclusive;
    Serialization::encode_string(bufPtr, scanSpec.endRow);
    *(*bufPtr)++ = (uint8_t)scanSpec.endRowInclusive;
    Serialization::encode_short(bufPtr, (short)scanSpec.columns.size());
    for (size_t i=0; i<scanSpec.columns.size(); i++)
      Serialization::encode_string(bufPtr, scanSpec.columns[i]);
    Serialization::encode_long(bufPtr, scanSpec.interval.first);
    Serialization::encode_long(bufPtr, scanSpec.interval.second);
    Serialization::encode_bool(bufPtr, scanSpec.return_deletes);
  }


  bool DecodeScanSpecification(uint8_t **bufPtr, size_t *remainingPtr, ScanSpecificationT *scanSpec) {
    uint16_t columnCount;
    const char *column;
    uint8_t inclusiveByte;

    if (!Serialization::decode_int(bufPtr, remainingPtr, &scanSpec->rowLimit))
      return false;
    if (!Serialization::decode_int(bufPtr, remainingPtr, &scanSpec->max_versions))
      return false;
    if (!Serialization::decode_string(bufPtr, remainingPtr, &scanSpec->startRow))
      return false;
    if (!Serialization::decode_byte(bufPtr, remainingPtr, &inclusiveByte))
      return false;
    scanSpec->startRowInclusive = inclusiveByte ? true : false;
    if (!Serialization::decode_string(bufPtr, remainingPtr, &scanSpec->endRow))
      return false;
    if (!Serialization::decode_byte(bufPtr, remainingPtr, &inclusiveByte))
      return false;
    scanSpec->endRowInclusive = inclusiveByte ? true : false;
    if (!Serialization::decode_short(bufPtr, remainingPtr, &columnCount))
      return false;
    for (short i=0; i<columnCount; i++) {
    if (!Serialization::decode_string(bufPtr, remainingPtr, &column))
      return false;
      scanSpec->columns.push_back(column);
    }
    if (!Serialization::decode_long(bufPtr, remainingPtr, &scanSpec->interval.first))
      return false;
    if (!Serialization::decode_long(bufPtr, remainingPtr, &scanSpec->interval.second))
      return false;
    if (!Serialization::decode_bool(bufPtr, remainingPtr, &scanSpec->return_deletes))
      return false;
  
    return true;
  }


  std::ostream &operator<<(std::ostream &os, const TableIdentifierT &table_identifier) {
    os << "Table Name = " << table_identifier.name << endl;
    os << "Table ID   = " << table_identifier.id << endl;
    os << "Generation = " << table_identifier.generation << endl;
    return os;
  }

  std::ostream &operator<<(std::ostream &os, const RangeT &range) {
    if (range.startRow == 0)
      os << "StartRow = [NULL]" << endl;
    else
      os << "StartRow = \"" << range.startRow << "\"" << endl;
    if (range.endRow == 0)
      os << "EndRow   = [NULL]" << endl;
    else
      os << "EndRow   = \"" << range.endRow << "\"" << endl;
    return os;
  }

  std::ostream &operator<<(std::ostream &os, const ScanSpecificationT &scanSpec) {
    os << "RowLimit    = " << scanSpec.rowLimit << endl;
    os << "MaxVersions = " << scanSpec.max_versions << endl;
    os << "Columns     = ";
    for (std::vector<const char *>::const_iterator iter = scanSpec.columns.begin(); iter != scanSpec.columns.end(); iter++)
      os << *iter << " ";
    os << endl;
    if (scanSpec.startRow)
      os << "StartRow  = " << scanSpec.startRow << endl;
    else
      os << "StartRow  = " << endl;
    os << "StartRowInclusive = " << scanSpec.startRowInclusive << endl;
    if (scanSpec.endRow)
      os << "EndRow    = " << scanSpec.endRow << endl;
    else
      os << "EndRow    = " << endl;
    os << "EndRowInclusive = " << scanSpec.endRowInclusive << endl;
    os << "MinTime     = " << scanSpec.interval.first << endl;
    os << "MaxTime     = " << scanSpec.interval.second << endl;
    return os;
  }

}
