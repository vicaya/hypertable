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

#include <cassert>
#include <iostream>

#include "Common/ByteOrder.h"

#include "Key.h"

using namespace Hypertable;
using namespace std;

namespace {
  const char end_row_chars[3] = { (char)0xff, (char)0xff, 0 };
  const char end_root_row_chars[5] = { '0', ':', (char)0xff, (char)0xff, 0 };

  size_t write_key(uint8_t *buf, uint8_t flag, const char *row, uint8_t column_family_code, const char *column_qualifier, uint64_t timestamp) {
    uint8_t *ptr = buf;
    strcpy((char *)ptr, row);
    ptr += strlen(row) + 1;
    *ptr++ = column_family_code;
    if (column_qualifier != 0) {
      strcpy((char *)ptr, column_qualifier);
      ptr += strlen(column_qualifier) + 1;
    }
    else
      *ptr++ = 0;
    *ptr++ = flag;
    if (timestamp == 0) {
      memset(ptr, 0xFF, sizeof(int64_t));
      ptr += sizeof(int64_t);
    }
    else {
      timestamp = ~timestamp;
      uint8_t *tsPtr = (uint8_t *)&timestamp;
      tsPtr += sizeof(int64_t);
      for (size_t i=0; i<sizeof(int64_t); i++) {
	tsPtr--;
	*ptr++ = *tsPtr;
      }
    }
    return ptr-buf;
  }

}



namespace Hypertable {

  const char *Key::END_ROW_MARKER = (const char *)end_row_chars;
  const char *Key::END_ROOT_ROW   = (const char *)end_root_row_chars;

  ByteString create_key(uint8_t flag, const char *row, uint8_t column_family_code, const char *column_qualifier, uint64_t timestamp) {
    size_t len = strlen(row) + 4 + sizeof(int64_t);
    if (column_qualifier != 0)
      len += strlen(column_qualifier);
    uint8_t *ptr = new uint8_t [ len + Serialization::encoded_length_vi32(len) ];  // !!! could probably just make this 6
    ByteString bs(ptr);
    Serialization::encode_vi32(&ptr, len);
    write_key(ptr, flag, row, column_family_code, column_qualifier, timestamp);
    return bs;
  }

  void create_key_and_append(DynamicBuffer &dst_buf, uint8_t flag, const char *row, uint8_t column_family_code, const char *column_qualifier, uint64_t timestamp) {
    size_t len = strlen(row) + 4 + sizeof(int64_t);
    if (column_qualifier != 0)
      len += strlen(column_qualifier);
    dst_buf.ensure(len + Serialization::encoded_length_vi32(len));  // !!! could probably just make this 6
    Serialization::encode_vi32(&dst_buf.ptr, len);
    dst_buf.ptr += write_key(dst_buf.ptr, flag, row, column_family_code, column_qualifier, timestamp);
  }

  Key::Key(ByteString key) {
    load(key);
  }

  /**
   * TODO: Re-implement below function in terms of this function
   */
  bool Key::load(ByteString key) {
    size_t len = key.decode_length((uint8_t **)&row);
    const uint8_t *endptr = (const uint8_t *)row + len;

    while (key.ptr < endptr && *key.ptr != 0)
      key.ptr++;
    key.ptr++;
    if (key.ptr >= endptr) {
      cerr << "row decode overrun" << endl;
      return false;
    }

    column_family_code = *key.ptr++;
    column_qualifier = (const char *)key.ptr;

    while (key.ptr < endptr && *key.ptr != 0)
      key.ptr++;
    key.ptr++;
    if (key.ptr >= endptr) {
      cerr << "qualifier decode overrun" << endl;
      return false;
    }

    if ((endptr - key.ptr) != 9) {
      cerr << "timestamp decode overrun " << (endptr-key.ptr) << endl;
      return false;
    }

    flag = *key.ptr++;
    timestamp_ptr = (uint8_t *)key.ptr;
    memcpy(&timestamp, key.ptr, sizeof(uint64_t));
    end_ptr = key.ptr + sizeof(uint64_t);

    timestamp = ByteOrderSwapInt64(timestamp);
    timestamp = ~timestamp;

    return true;
  }

  void Key::updateTimestamp(uint64_t ts) {
    timestamp = ts;
    ts = ByteOrderSwapInt64(ts);
    ts = ~ts;
    memcpy(timestamp_ptr, &ts, sizeof(int64_t));
  }



  std::ostream &operator<<(std::ostream &os, const Key &keyComps) {
    os << "row='" << keyComps.row << "' ";
    if (keyComps.flag == FLAG_DELETE_ROW)
      os << "ts=" << keyComps.timestamp << " DELETE";
    else {
      os << "family=" << (int)keyComps.column_family_code;
      if (keyComps.column_qualifier) 
	os << " qualifier='" << keyComps.column_qualifier << "'";
      os << " ts=" << keyComps.timestamp;
      if (keyComps.flag == FLAG_DELETE_CELL)
	os << " DELETE";
      else if (keyComps.flag == FLAG_DELETE_COLUMN_FAMILY)
	os << " DELETE_COLUMN_FAMILY";
    }
    return os;
  }


}

