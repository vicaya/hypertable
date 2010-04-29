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

namespace Hypertable {

  bool
  FillScanBlock(CellListScannerPtr &scanner, DynamicBuffer &dbuf,
                int64_t buffer_size, size_t *countp) {
    Key key, last_key;
    ByteString value;
    size_t value_len;
    bool more = true;
    size_t limit = buffer_size;
    size_t remaining = buffer_size;
    uint8_t *ptr;
    ScanContext *scan_context = scanner->scan_context();
    bool return_all = (scan_context->spec->return_deletes) ? true : false;

    assert(dbuf.base == 0);

    memset(&last_key, 0, sizeof(last_key));

    *countp = 0;

    while ((more = scanner->get(key, value))) {

      if (!return_all) {
        // drop duplicates
        if (key.timestamp == last_key.timestamp &&
            key.row_len == last_key.row_len &&
            key.column_family_code == last_key.column_family_code &&
            key.column_qualifier_len == last_key.column_qualifier_len &&
            !strcmp(key.row, last_key.row) &&
            !strcmp(key.column_qualifier, last_key.column_qualifier)) {
          scanner->forward();
          continue;
        }
        memcpy(&last_key, &key, sizeof(Key));
      }
      
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
