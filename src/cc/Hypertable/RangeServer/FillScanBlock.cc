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

extern "C" {
#include <stdint.h>
}

#include "FillScanBlock.h"

namespace Hypertable {

  bool FillScanBlock(CellListScannerPtr &scannerPtr, uint8_t *dst, size_t dstLen, uint32_t *lenp) {
    uint8_t *ptr = dst;
    uint8_t *end = dst + dstLen;
    ByteString key;
    ByteString value;
    size_t key_len, value_len;
    bool more = true;

    while ((more = scannerPtr->get(key, value))) {
      key_len = key.length();
      value_len = value.length();
      if ((size_t)(end-ptr) >= key_len + value_len) {
	memcpy(ptr, key.ptr, key_len);
	ptr += key_len;
	memcpy(ptr, value.ptr, value_len);
	ptr += value_len;
	scannerPtr->forward();
      }
      else
	break;
    }

    *lenp = ptr - dst;

    return more;
  }

}
