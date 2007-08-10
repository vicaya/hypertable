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

#ifndef HYPERTABLE_REQUEST_H
#define HYPERTABLE_REQUEST_H


#include <utility>

extern "C" {
#include <stddef.h>
#include <stdint.h>
}

#include "Common/ByteString.h"

namespace hypertable {

  typedef struct {
    int32_t generation;
    const char *tableName;
    const char *startRow;
    const char *endRow;
  } TabletIdentifierT;

  typedef struct {
    uint16_t flags;
    ByteString32T *columns;
    ByteString32T *startRow;
    ByteString32T *endRow;
    std::pair<uint64_t,uint64_t> interval;
  } ScannerSpecT;

  typedef struct {
    const uint8_t *buf;
    int32_t len;
  } BufferT;

  size_t DeserializeTabletIdentifier(uint8_t *ptr, size_t remaining, TabletIdentifierT *tabletIdPtr);

  size_t DeserializeScannerSpec(uint8_t *ptr, size_t remaining, ScannerSpecT *scannerSpecPtr);

  std::ostream &operator<<(std::ostream &os, const TabletIdentifierT &tablet);

  std::ostream &operator<<(std::ostream &os, const ScannerSpecT &scannerSpec);

}


#endif // HYPERTABLE_REQUEST_H
