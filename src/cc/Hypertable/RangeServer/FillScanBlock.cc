/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

extern "C" {
#include <stdint.h>
}

#include "FillScanBlock.h"

namespace hypertable {

  bool FillScanBlock(CellListScannerPtr &scannerPtr, uint8_t *dst, size_t dstLen, uint32_t *lenp) {
    uint8_t *ptr = dst;
    uint8_t *end = dst + dstLen;
    ByteString32T *key;
    ByteString32T *value;
    bool more = true;

    while ((more = scannerPtr->Get(&key, &value))) {
      if ((size_t)(end-ptr) >= Length(key) + Length(value)) {
	memcpy(ptr, key, Length(key));
	ptr += Length(key);
	memcpy(ptr, value, Length(value));
	ptr += Length(value);
	scannerPtr->Forward();
      }
      else
	break;
    }

    *lenp = ptr - dst;

    return more;
  }

}
