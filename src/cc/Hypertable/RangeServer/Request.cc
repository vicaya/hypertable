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

#include "Request.h"

using namespace std;

namespace hypertable {

  const uint16_t SCAN_FLAGS_ONLY_LATEST_CELLS = 0x01;

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
    if (*rangeSpec->startRow == 0)
      rangeSpec->startRow = 0;

    /** End row **/
    if ((skip = CommBuf::DecodeString(ptr, remaining, &rangeSpec->endRow)) == 0)
      return 0;
    ptr += skip;
    if (*rangeSpec->endRow == 0)
      rangeSpec->endRow = 0;

    return ptr-base;
  }

  size_t DeserializeScanSpecification(uint8_t *ptr, size_t remaining, ScanSpecificationT *scanSpec) {
    size_t skip;
    uint8_t *base = ptr;
  
    memset(scanSpec, 0, sizeof(ScanSpecificationT));

    /** flags **/
    if (remaining < sizeof(int16_t))
      return 0;
    memcpy(&scanSpec->flags, ptr, sizeof(int16_t));
    ptr += sizeof(int16_t);
    remaining -= sizeof(int16_t);

    /** cellCount **/
    if (remaining < sizeof(int32_t))
      return 0;
    memcpy(&scanSpec->cellCount, ptr, sizeof(int32_t));
    ptr += sizeof(int32_t);
    remaining -= sizeof(int32_t);

    /** Columns **/
    if ((skip = CommBuf::DecodeByteArray(ptr, remaining, &scanSpec->columns)) == 0)
      return 0;
    ptr += skip;
    remaining -= skip;

    /** Start Row **/
    if ((skip = CommBuf::DecodeByteArray(ptr, remaining, &scanSpec->startRow)) == 0)
      return 0;
    ptr += skip;
    remaining -= skip;

    /** End Row **/
    if ((skip = CommBuf::DecodeByteArray(ptr, remaining, &scanSpec->endRow)) == 0)
      return 0;
    ptr += skip;
    remaining -= skip;

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
    os << "Flags    = 0x" << hex << scanSpec.flags << dec << endl;
    if (scanSpec.startRow->len != 0)
      os << "StartRow = " << (const char *)scanSpec.startRow->data << endl;
    else
      os << "StartRow = [NULL]" << endl;
    if (scanSpec.endRow->len != 0)
      os << "EndRow   = " << (const char *)scanSpec.endRow->data << endl;
    else
      os << "EndRow   = [NULL]" << endl;
    os << "MinTime  = " << scanSpec.interval.first << endl;
    os << "MaxTime  = " << scanSpec.interval.second << endl;
    for (int32_t i=0; i<scanSpec.columns->len; i++)
      os << "Column   = " << (int)scanSpec.columns->data[i] << endl;
    return os;
  }

}
