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


  KeyComponentsT::KeyComponentsT(const ByteString32T *key) {
    Load(key, *this);
  }

  std::ostream &operator<<(std::ostream &os, const KeyComponentsT &keyComps) {
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

  /**
   * TODO: Re-implement below function in terms of this function
   */
  bool Load(const ByteString32T *key, KeyComponentsT &comps) {
    const uint8_t *ptr = key->data;
    const uint8_t *endptr = key->data + key->len;

    comps.rowKey = (const char *)ptr;

    while (ptr < endptr && *ptr != 0)
      ptr++;
    ptr++;
    if (ptr >= endptr)
      return false;

    comps.columnFamily = *ptr++;
    comps.columnQualifier = (const char *)ptr;

    while (ptr < endptr && *ptr != 0)
      ptr++;
    ptr++;
    if (ptr >= endptr)
      return false;

    if ((endptr - ptr) != 9)
      return false;

    comps.flag = *ptr++;
    memcpy(&comps.timestamp, ptr, sizeof(uint64_t));
    comps.endPtr = ptr + sizeof(uint64_t);

    comps.timestamp = ByteOrderSwapInt64(comps.timestamp);
    comps.timestamp = ~comps.timestamp;

    return true;
  }

#if 0
  std::ostream &operator <<(std::ostream &os, const KeyT &key) {
    uint64_t timestamp;
    uint8_t *tsPtr = (uint8_t *)&timestamp;
    const uint8_t *ptr = key.data + (key.len - sizeof(int64_t));
    uint8_t flag = *(ptr-1);

    // decode timestamp
    tsPtr += sizeof(int64_t);
    for (size_t i=0; i<sizeof(int64_t); i++) {
      tsPtr--;
      *tsPtr = *ptr++;
    }
    timestamp = ~timestamp;

    os << "row='" << (const char *)key.data << "' ";
    if (flag == FLAG_DELETE_ROW)
      os << "ts=" << timestamp << " DELETE";
    else {
      ptr = key.data + strlen((char *)key.data) + 1;
      os << "family=" << (int)*ptr;
      ptr++;
      os << " qualifier='" << (const char *)ptr << "' ts=" << timestamp;
      if (flag == FLAG_DELETE_CELL)
	os << " DELETE";
    }

    return os;
  }
#endif


}

