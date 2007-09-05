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

using namespace hypertable;
using namespace std;

namespace hypertable {

  ByteString32T *CreateKey(uint8_t flag, const char *rowKey, uint8_t columnFamily, const char *columnQualifier, uint64_t timestamp) {
    size_t len = strlen(rowKey) + 4 + sizeof(int64_t);
    if (columnQualifier != 0)
      len += strlen(columnQualifier);
    ByteString32T *key = (ByteString32T *)new uint8_t [ len + sizeof(int32_t) ];
    uint8_t *ptr = key->data;
    key->len = len;
    strcpy((char *)ptr, rowKey);
    ptr += strlen(rowKey) + 1;
    *ptr++ = columnFamily;
    if (columnQualifier != 0) {
      strcpy((char *)ptr, columnQualifier);
      ptr += strlen(columnQualifier) + 1;
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
    return key;
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

    rowKey = (const char *)ptr;

    while (ptr < endptr && *ptr != 0)
      ptr++;
    ptr++;
    if (ptr >= endptr) {
      cerr << "row decode overrun" << endl;
      return false;
    }

    columnFamily = *ptr++;
    columnQualifier = (const char *)ptr;

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
    memcpy(&timestamp, ptr, sizeof(uint64_t));
    endPtr = ptr + sizeof(uint64_t);

    timestamp = ByteOrderSwapInt64(timestamp);
    timestamp = ~timestamp;

    return true;
  }


  std::ostream &operator<<(std::ostream &os, const Key &keyComps) {
    os << "row='" << keyComps.rowKey << "' ";
    if (keyComps.flag == FLAG_DELETE_ROW)
      os << "ts=" << keyComps.timestamp << " DELETE";
    else {
      os << "family=" << (int)keyComps.columnFamily;
      os << " qualifier='" << keyComps.columnQualifier << "' ts=" << keyComps.timestamp;
      if (keyComps.flag == FLAG_DELETE_CELL)
	os << " DELETE";
    }
    return os;
  }


}

