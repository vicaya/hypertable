/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cassert>
#include <iostream>

#include "Common/ByteOrder.h"

#include "Key.h"

using namespace hypertable;
using namespace std;

namespace hypertable {

  KeyT *CreateKey(uint8_t flag, const char *rowKey, uint8_t columnFamily, const char *columnQualifier, uint64_t timestamp) {
    size_t len = strlen(rowKey) + 4 + sizeof(int64_t);
    if (columnQualifier != 0)
      len += strlen(columnQualifier);
    KeyT *key = (KeyT *)new uint8_t [ len + sizeof(int32_t) ];
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

  /**
   * TODO: Re-implement above function in terms of this function
   */
  bool Load(const KeyT *key, KeyComponentsT &comps) {
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
}

