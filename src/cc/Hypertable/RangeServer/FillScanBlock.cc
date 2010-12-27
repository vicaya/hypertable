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
  FillScanBlock(CellListScannerPtr &scanner, DynamicBuffer &dbuf, int64_t buffer_size) {
    Key key, last_key;
    ByteString value;
    size_t value_len;
    bool more = true;
    size_t limit = buffer_size;
    size_t remaining = buffer_size;
    uint8_t *ptr;
    ScanContext *scan_context = scanner->scan_context();
    bool return_all = (scan_context->spec->return_deletes) ? true : false;
    char numbuf[17];
    DynamicBuffer counter_value;
    bool counter;

    assert(dbuf.base == 0);

    memset(&last_key, 0, sizeof(last_key));

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

      counter = scan_context->family_info[key.column_family_code].counter &&
          (key.flag == FLAG_INSERT);

      if (counter) {
        const uint8_t *decode;
        uint64_t count;
        size_t remain = value.decode_length(&decode);
        // value must be encoded 64 bit int
        if (remain != 8)
          HT_FATAL_OUT << "Expected counter to be encoded 64 bit int but remain=" << remain
                       << " ,key=" << key << " ,value="<< value.str() << HT_END;

        count = Serialization::decode_i64(&decode, &remain);
        //convert counter to ascii
        sprintf(numbuf, "%llu", (Llu) count);
        value_len = strlen(numbuf);
        counter_value.clear();
        append_as_byte_string(counter_value, numbuf, value_len);
        value_len = counter_value.fill();
      }
      else
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
        uint8_t *base = dbuf.ptr;

        dbuf.add_unchecked(key.serial.ptr, key.length);

        last_key.row = (const char *)base + (key.row - (const char *)key.serial.ptr);
        last_key.column_qualifier = (const char *)base + (key.column_qualifier - (const char *)key.serial.ptr);

        if (counter) {
          dbuf.add_unchecked(counter_value.base, value_len);
        }
        else
          dbuf.add_unchecked(value.ptr, value_len);
        remaining -= (key.length + value_len);
        scanner->forward();
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
