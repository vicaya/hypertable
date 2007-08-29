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

#include "Types.h"

using namespace std;

namespace hypertable {

  size_t DeserializeRangeSpecification(uint8_t *ptr, size_t remaining, RangeSpecificationT *rangeSpec) {
    size_t skip;
    uint8_t *base = ptr;

    memset(rangeSpec, 0, sizeof(RangeSpecificationT));
  
    /** Generation **/
    if (remaining < sizeof(int32_t))
      return 0;
    memcpy(&rangeSpec->generation, ptr, sizeof(int32_t));
    ptr += sizeof(int32_t);
    remaining -= sizeof(int32_t);

    /** Table name **/
    if ((skip = CommBuf::DecodeString(ptr, remaining, &rangeSpec->tableName)) == 0)
      return 0;
    ptr += skip;
    remaining -= skip;

    /** Start row **/
    if ((skip = CommBuf::DecodeString(ptr, remaining, &rangeSpec->startRow)) == 0)
      return 0;
    ptr += skip;
    remaining -= skip;

    /** End row **/
    if ((skip = CommBuf::DecodeString(ptr, remaining, &rangeSpec->endRow)) == 0)
      return 0;
    ptr += skip;

    return ptr-base;
  }

  size_t DeserializeScanSpecification(uint8_t *ptr, size_t remaining, ScanSpecificationT *scanSpec) {
    size_t skip;
    uint8_t *base = ptr;
    short columnCount;
    const char *column;
  
    /** rowLimit **/
    if (remaining < 4)
      return 0;
    memcpy(&scanSpec->rowLimit, ptr, 4);
    ptr += 4;
    remaining -= 4;

    /** cellLimit **/
    if (remaining < 4)
      return 0;
    memcpy(&scanSpec->cellLimit, ptr, 4);
    ptr += 4;
    remaining -= 4;

    /** Start Row **/
    if ((skip = CommBuf::DecodeString(ptr, remaining, &scanSpec->startRow)) == 0)
      return 0;
    ptr += skip;
    remaining -= skip;

    /** End Row **/
    if ((skip = CommBuf::DecodeString(ptr, remaining, &scanSpec->endRow)) == 0)
      return 0;
    ptr += skip;
    remaining -= skip;

    /** columnCount **/
    if (remaining < 2)
      return 0;
    memcpy(&columnCount, ptr, 2);
    ptr += 2;
    remaining -= 2;

    /** columns **/
    for (short i=0; i<columnCount; i++) {
      if ((skip = CommBuf::DecodeString(ptr, remaining, &column)) == 0)
	return 0;
      ptr += skip;
      remaining -= skip;
      scanSpec->columns.push_back(column);
    }

    /** Time Interval  **/
    if (remaining < 2*sizeof(uint64_t))
      return 0;

    memcpy(&scanSpec->interval.first, ptr, sizeof(int64_t));
    ptr += sizeof(int64_t);

    memcpy(&scanSpec->interval.second, ptr, sizeof(int64_t));
    ptr += sizeof(int64_t);
  
    return ptr-base;
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
    os << "StartRow  = " << scanSpec.startRow << endl;
    os << "EndRow    = " << scanSpec.endRow << endl;
    os << "MinTime   = " << scanSpec.interval.first << endl;
    os << "MaxTime   = " << scanSpec.interval.second << endl;
    return os;
  }

}
