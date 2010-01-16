/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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

  bool
  FillScanBlock(CellListScannerPtr &scanner, DynamicBuffer &dbuf,
                size_t *countp) {
    Key key;
    ByteString value;
    size_t value_len;
    bool more = true;
    size_t limit = DATA_TRANSFER_BLOCKSIZE;
    size_t remaining = DATA_TRANSFER_BLOCKSIZE;
    uint8_t *ptr;

    assert(dbuf.base == 0);

    *countp = 0;

    while ((more = scanner->get(key, value))) {
      value_len = value.length();
      if (dbuf.base == 0) {
        if (key.length + value_len > limit) {
          limit = key.length + value_len;
          remaining = limit;
        }
        dbuf.reserve(4 + limit);
        // skip encoded length
        dbuf.ptr = dbuf.base + 4;
      }
      if (key.length + value_len <= remaining) {
        dbuf.add_unchecked(key.serial.ptr, key.length);
        dbuf.add_unchecked(value.ptr, value_len);
        remaining -= (key.length + value_len);
        scanner->forward();
        (*countp)++;
      }
      else
        break;
    }

    if (dbuf.base == 0) {
      dbuf.reserve(4);
      dbuf.ptr = dbuf.base + 4;
    }

    ptr = dbuf.base;
    Serialization::encode_i32(&ptr, dbuf.fill() - 4);

    return more;
  }

}
