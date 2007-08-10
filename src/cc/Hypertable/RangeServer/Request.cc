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

  size_t DeserializeTabletIdentifier(uint8_t *ptr, size_t remaining, TabletIdentifierT *tabletIdPtr) {
    size_t skip;
    uint8_t *base = ptr;

    memset(tabletIdPtr, 0, sizeof(TabletIdentifierT));
  
    /** Generation **/
    if (remaining < sizeof(int32_t))
      return 0;
    memcpy(&tabletIdPtr->generation, ptr, sizeof(int32_t));
    ptr += sizeof(int32_t);
    remaining -= sizeof(int32_t);

    /** Table name **/
    if ((skip = CommBuf::DecodeString(ptr, remaining, &tabletIdPtr->tableName)) == 0)
      return 0;
    ptr += skip;
    remaining -= skip;

    /** Start row **/
    if ((skip = CommBuf::DecodeString(ptr, remaining, &tabletIdPtr->startRow)) == 0)
      return 0;
    ptr += skip;
    remaining -= skip;
    if (*tabletIdPtr->startRow == 0)
      tabletIdPtr->startRow = 0;

    /** End row **/
    if ((skip = CommBuf::DecodeString(ptr, remaining, &tabletIdPtr->endRow)) == 0)
      return 0;
    ptr += skip;
    if (*tabletIdPtr->endRow == 0)
      tabletIdPtr->endRow = 0;

    return ptr-base;
  }

  size_t DeserializeScannerSpec(uint8_t *ptr, size_t remaining, ScannerSpecT *scannerSpecPtr) {
    size_t skip;
    uint8_t *base = ptr;
  
    memset(scannerSpecPtr, 0, sizeof(ScannerSpecT));

    /** flags **/
    if (remaining < sizeof(int16_t))
      return 0;
    memcpy(&scannerSpecPtr->flags, ptr, sizeof(int16_t));
    ptr += sizeof(int16_t);
    remaining -= sizeof(int16_t);

    /** Columns **/
    if ((skip = CommBuf::DecodeByteArray(ptr, remaining, &scannerSpecPtr->columns)) == 0)
      return 0;
    ptr += skip;
    remaining -= skip;

    /** Start Row **/
    if ((skip = CommBuf::DecodeByteArray(ptr, remaining, &scannerSpecPtr->startRow)) == 0)
      return 0;
    ptr += skip;
    remaining -= skip;

    /** End Row **/
    if ((skip = CommBuf::DecodeByteArray(ptr, remaining, &scannerSpecPtr->endRow)) == 0)
      return 0;
    ptr += skip;
    remaining -= skip;

    /** Time Interval  **/
    if (remaining < 2*sizeof(uint64_t))
      return 0;

    memcpy(&scannerSpecPtr->interval.first, ptr, sizeof(int64_t));
    ptr += sizeof(int64_t);

    memcpy(&scannerSpecPtr->interval.second, ptr, sizeof(int64_t));
    ptr += sizeof(int64_t);
  
    return ptr-base;
  }

  std::ostream &operator<<(std::ostream &os, const TabletIdentifierT &tablet) {
    os << "Table name = " << tablet.tableName << endl;
    os << "Table generation = " << tablet.generation << endl;
    if (tablet.startRow == 0)
      os << "Start row = [NULL]" << endl;
    else
      os << "Start row = \"" << tablet.startRow << "\"" << endl;
    if (tablet.endRow == 0)
      os << "End row = [NULL]" << endl;
    else
      os << "End row = \"" << tablet.endRow << "\"" << endl;
    return os;
  }

  std::ostream &operator<<(std::ostream &os, const ScannerSpecT &scannerSpec) {
    os << "Flags = 0x" << hex << scannerSpec.flags << dec << endl;
    if (scannerSpec.startRow->len != 0)
      os << "Start Row = " << (const char *)scannerSpec.startRow->data << endl;
    else
      os << "Start Key = [NULL]" << endl;
    if (scannerSpec.endRow != 0)
      os << "End Row = " << (const char *)scannerSpec.endRow->data << endl;
    else
      os << "End Key = [NULL]" << endl;
    os << "Start Time = " << scannerSpec.interval.first << endl;
    os << "End Time = " << scannerSpec.interval.second << endl;
    for (int32_t i=0; i<scannerSpec.columns->len; i++)
      os << "Column = " << (int)scannerSpec.columns->data[i] << endl;
    return os;
  }

}
