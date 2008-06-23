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

#include "Common/Compat.h"
#include "FillScanBlock.h"
#include "Hypertable/Lib/Defaults.h"

namespace Hypertable {

  bool FillScanBlock(CellListScannerPtr &scanner, DynamicBuffer &dbuf) {
    ByteString key;
    ByteString value;
    size_t key_len, value_len;
    bool more = true;
    size_t limit = HYPERTABLE_DATA_TRANSFER_BLOCKSIZE;
    size_t remaining = HYPERTABLE_DATA_TRANSFER_BLOCKSIZE;
    uint8_t *ptr;

    assert(dbuf.base == 0);

    while ((more = scanner->get(key, value))) {
      key_len = key.length();
      value_len = value.length();
      if (dbuf.base == 0) {
        if (key_len + value_len > limit) {
          limit = key_len + value_len;
          remaining = limit;
        }
        dbuf.reserve(limit+4);
        // skip encoded length
        dbuf.ptr = dbuf.base + 4;
      }
      if (key_len + value_len <= remaining) {
        dbuf.add_unchecked(key.ptr, key_len);
        dbuf.add_unchecked(value.ptr, value_len);
        remaining -= (key_len + value_len);
        scanner->forward();
      }
      else
        break;
    }

    if (dbuf.base == 0) {
      dbuf.reserve(limit + 4);
      dbuf.ptr = dbuf.base + 4;
    }

    ptr = dbuf.base;
    Serialization::encode_i32(&ptr, dbuf.fill() - 4);

    return more;
  }

}
