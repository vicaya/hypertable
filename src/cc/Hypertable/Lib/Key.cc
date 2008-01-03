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

#include <cassert>
#include <iostream>

#include "Common/ByteOrder.h"

#include "Key.h"

using namespace Hypertable;
using namespace std;

namespace {
  const char end_row_chars[3] = { (char)0xff, (char)0xff, 0 };
  const char end_root_row_chars[5] = { '0', ':', (char)0xff, (char)0xff, 0 };
}


namespace Hypertable {

  const char *Key::END_ROW_MARKER = (const char *)end_row_chars;
  const char *Key::END_ROOT_ROW   = (const char *)end_root_row_chars;

  void CreateKey(ByteString32T *key, uint8_t flag, const char *row, uint8_t column_family_code, const char *column_qualifier, uint64_t timestamp) {
    size_t len = strlen(row) + 4 + sizeof(int64_t);
    if (column_qualifier != 0)
      len += strlen(column_qualifier);
    uint8_t *ptr = key->data;
    key->len = len;
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
    assert((ptr-key->data) == (int32_t)key->len);
  }

  ByteString32T *CreateKey(uint8_t flag, const char *row, uint8_t column_family_code, const char *column_qualifier, uint64_t timestamp) {
    size_t len = strlen(row) + 4 + sizeof(int64_t);
    if (column_qualifier != 0)
      len += strlen(column_qualifier);
    ByteString32T *key = (ByteString32T *)new uint8_t [ len + sizeof(int32_t) ];
    CreateKey(key, flag, row, column_family_code, column_qualifier, timestamp);
    return key;
  }

  void CreateKeyAndAppend(DynamicBuffer &dst_buf, uint8_t flag, const char *row, uint8_t column_family_code, const char *column_qualifier, uint64_t timestamp) {
    size_t len = strlen(row) + 4 + sizeof(int64_t);
    if (column_qualifier != 0)
      len += strlen(column_qualifier);
    dst_buf.ensure(len + 4);
    CreateKey((ByteString32T *)dst_buf.ptr, flag, row, column_family_code, column_qualifier, timestamp);    
    dst_buf.ptr += Length((ByteString32T *)dst_buf.ptr);
  }

  Key::Key(const ByteString32T *key) {
    load(key);
  }

  /**
   * TODO: Re-implement below function in terms of this function
   */
  bool Key::load(const ByteString32T *key) {
    const uint8_t *ptr = key->data;
    const uint8_t *endptr = key->data + key->len;

    row = (const char *)ptr;

    while (ptr < endptr && *ptr != 0)
      ptr++;
    ptr++;
    if (ptr >= endptr) {
      cerr << "row decode overrun" << endl;
      return false;
    }

    column_family_code = *ptr++;
    column_qualifier = (const char *)ptr;

    while (ptr < endptr && *ptr != 0)
      ptr++;
    ptr++;
    if (ptr >= endptr) {
      cerr << "qualifier decode overrun" << endl;
      return false;
    }

    if ((endptr - ptr) != 9) {
      cerr << "timestamp decode overrun " << (endptr-ptr) << endl;
      return false;
    }

    flag = *ptr++;
    timestamp_ptr = (uint8_t *)ptr;
    memcpy(&timestamp, ptr, sizeof(uint64_t));
    end_ptr = ptr + sizeof(uint64_t);

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

