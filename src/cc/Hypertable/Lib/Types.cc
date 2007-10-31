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
    return 4 + Serialization::EncodedLengthString(rangeSpec.tableName) + Serialization::EncodedLengthString(rangeSpec.startRow) + Serialization::EncodedLengthString(rangeSpec.endRow);
  }


  /**
   *
   */
  void EncodeRangeSpecification(uint8_t **bufPtr, RangeSpecificationT &rangeSpec) {
    Serialization::EncodeInt(bufPtr, rangeSpec.generation);
    Serialization::EncodeString(bufPtr, rangeSpec.tableName);
    Serialization::EncodeString(bufPtr, rangeSpec.startRow);
    Serialization::EncodeString(bufPtr, rangeSpec.endRow);
  }


  /**
   *
   */
  bool DecodeRangeSpecification(uint8_t **bufPtr, size_t *remainingPtr, RangeSpecificationT *rangeSpec) {

    memset(rangeSpec, 0, sizeof(RangeSpecificationT));

    if (!Serialization::DecodeInt(bufPtr, remainingPtr, &rangeSpec->generation))
      return false;
    if (!Serialization::DecodeString(bufPtr, remainingPtr, &rangeSpec->tableName))
      return false;
    if (!Serialization::DecodeString(bufPtr, remainingPtr, &rangeSpec->startRow))
      return false;
    if (!Serialization::DecodeString(bufPtr, remainingPtr, &rangeSpec->endRow))
      return false;

    return true;
  }

  /**
   *
   */
  size_t EncodedLengthScanSpecification(ScanSpecificationT &scanSpec) {
    size_t len = 28;
    len += Serialization::EncodedLengthString(scanSpec.startRow);
    len += Serialization::EncodedLengthString(scanSpec.endRow);
    for (int i=0; i<scanSpec.columns.size(); i++)
      len += Serialization::EncodedLengthString(scanSpec.columns[i]);
    return len;
  }


  void EncodeScanSpecification(uint8_t **bufPtr, ScanSpecificationT &scanSpec) {
    Serialization::EncodeInt(bufPtr, scanSpec.rowLimit);
    Serialization::EncodeInt(bufPtr, scanSpec.cellLimit);
    Serialization::EncodeString(bufPtr, scanSpec.startRow);
    *(*bufPtr)++ = (uint8_t)scanSpec.startRowInclusive;
    Serialization::EncodeString(bufPtr, scanSpec.endRow);
    *(*bufPtr)++ = (uint8_t)scanSpec.endRowInclusive;
    Serialization::EncodeShort(bufPtr, (short)scanSpec.columns.size());
    for (int i=0; i<scanSpec.columns.size(); i++)
      Serialization::EncodeString(bufPtr, scanSpec.columns[i]);
    Serialization::EncodeLong(bufPtr, scanSpec.interval.first);
    Serialization::EncodeLong(bufPtr, scanSpec.interval.second);
  }


  bool DecodeScanSpecification(uint8_t **bufPtr, size_t *remainingPtr, ScanSpecificationT *scanSpec) {
    uint16_t columnCount;
    const char *column;
    uint8_t inclusiveByte;

    if (!Serialization::DecodeInt(bufPtr, remainingPtr, &scanSpec->rowLimit))
      return false;
    if (!Serialization::DecodeInt(bufPtr, remainingPtr, &scanSpec->cellLimit))
      return false;
    if (!Serialization::DecodeString(bufPtr, remainingPtr, &scanSpec->startRow))
      return false;
    if (!Serialization::DecodeByte(bufPtr, remainingPtr, &inclusiveByte))
      return false;
    scanSpec->startRowInclusive = inclusiveByte ? true : false;
    if (!Serialization::DecodeString(bufPtr, remainingPtr, &scanSpec->endRow))
      return false;
    if (!Serialization::DecodeByte(bufPtr, remainingPtr, &inclusiveByte))
      return false;
    scanSpec->endRowInclusive = inclusiveByte ? true : false;
    if (!Serialization::DecodeShort(bufPtr, remainingPtr, &columnCount))
      return false;
    for (short i=0; i<columnCount; i++) {
    if (!Serialization::DecodeString(bufPtr, remainingPtr, &column))
      return false;
      scanSpec->columns.push_back(column);
    }
    if (!Serialization::DecodeLong(bufPtr, remainingPtr, &scanSpec->interval.first))
      return false;
    if (!Serialization::DecodeLong(bufPtr, remainingPtr, &scanSpec->interval.second))
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
