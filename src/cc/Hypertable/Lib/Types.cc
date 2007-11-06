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

namespace hypertable {

  /**
   * 
   */
  size_t EncodedLengthRangeSpecification(RangeSpecificationT &rangeSpec) {
    return 4 + Serialization::encoded_length_string(rangeSpec.tableName) + Serialization::encoded_length_string(rangeSpec.startRow) + Serialization::encoded_length_string(rangeSpec.endRow);
  }


  /**
   *
   */
  void EncodeRangeSpecification(uint8_t **bufPtr, RangeSpecificationT &rangeSpec) {
    Serialization::encode_int(bufPtr, rangeSpec.generation);
    Serialization::encode_string(bufPtr, rangeSpec.tableName);
    Serialization::encode_string(bufPtr, rangeSpec.startRow);
    Serialization::encode_string(bufPtr, rangeSpec.endRow);
  }


  /**
   *
   */
  bool DecodeRangeSpecification(uint8_t **bufPtr, size_t *remainingPtr, RangeSpecificationT *rangeSpec) {

    memset(rangeSpec, 0, sizeof(RangeSpecificationT));

    if (!Serialization::decode_int(bufPtr, remainingPtr, &rangeSpec->generation))
      return false;
    if (!Serialization::decode_string(bufPtr, remainingPtr, &rangeSpec->tableName))
      return false;
    if (!Serialization::decode_string(bufPtr, remainingPtr, &rangeSpec->startRow))
      return false;
    if (!Serialization::decode_string(bufPtr, remainingPtr, &rangeSpec->endRow))
      return false;

    return true;
  }

  /**
   *
   */
  size_t EncodedLengthScanSpecification(ScanSpecificationT &scanSpec) {
    size_t len = 28;
    len += Serialization::encoded_length_string(scanSpec.startRow);
    len += Serialization::encoded_length_string(scanSpec.endRow);
    for (int i=0; i<scanSpec.columns.size(); i++)
      len += Serialization::encoded_length_string(scanSpec.columns[i]);
    return len;
  }


  void EncodeScanSpecification(uint8_t **bufPtr, ScanSpecificationT &scanSpec) {
    Serialization::encode_int(bufPtr, scanSpec.rowLimit);
    Serialization::encode_int(bufPtr, scanSpec.cellLimit);
    Serialization::encode_string(bufPtr, scanSpec.startRow);
    *(*bufPtr)++ = (uint8_t)scanSpec.startRowInclusive;
    Serialization::encode_string(bufPtr, scanSpec.endRow);
    *(*bufPtr)++ = (uint8_t)scanSpec.endRowInclusive;
    Serialization::encode_short(bufPtr, (short)scanSpec.columns.size());
    for (int i=0; i<scanSpec.columns.size(); i++)
      Serialization::encode_string(bufPtr, scanSpec.columns[i]);
    Serialization::encode_long(bufPtr, scanSpec.interval.first);
    Serialization::encode_long(bufPtr, scanSpec.interval.second);
  }


  bool DecodeScanSpecification(uint8_t **bufPtr, size_t *remainingPtr, ScanSpecificationT *scanSpec) {
    uint16_t columnCount;
    const char *column;
    uint8_t inclusiveByte;

    if (!Serialization::decode_int(bufPtr, remainingPtr, &scanSpec->rowLimit))
      return false;
    if (!Serialization::decode_int(bufPtr, remainingPtr, &scanSpec->cellLimit))
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
  
    return true;
  }


  std::ostream &operator<<(std::ostream &os, const RangeSpecificationT &rangeSpec) {
    os << "TableName  = " << rangeSpec.tableName << endl;
    os << "Generation = " << rangeSpec.generation << endl;
    if (rangeSpec.startRow == 0)
      os << "StartRow = [NULL]" << endl;
    else
      os << "StartRow = \"" << rangeSpec.startRow << "\"" << endl;
    if (rangeSpec.endRow == 0)
      os << "EndRow   = [NULL]" << endl;
    else
      os << "EndRow   = \"" << rangeSpec.endRow << "\"" << endl;
    return os;
  }

  std::ostream &operator<<(std::ostream &os, const ScanSpecificationT &scanSpec) {
    os << "RowLimit  = " << scanSpec.rowLimit << endl;
    os << "CellLimit = " << scanSpec.rowLimit << endl;
    os << "Columns   = ";
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
      os << "EndRow  = " << endl;
    os << "EndRowInclusive = " << scanSpec.endRowInclusive << endl;
    os << "MinTime   = " << scanSpec.interval.first << endl;
    os << "MaxTime   = " << scanSpec.interval.second << endl;
    return os;
  }

}
