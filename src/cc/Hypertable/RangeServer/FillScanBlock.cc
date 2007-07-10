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

extern "C" {
#include <stdint.h>
}

#include "FillScanBlock.h"

namespace hypertable {

  bool FillScanBlock(CellListScannerPtr &scannerPtr, uint8_t *dst, size_t dstLen, uint32_t *lenp) {
    uint8_t *ptr = dst;
    uint8_t *end = dst + dstLen;
    KeyT          *key;
    ByteString32T *value;
    bool more = true;

    while ((more = scannerPtr->Get(&key, &value))) {
      if ((size_t)(end-ptr) >= Length(key) + Length(value)) {
	memcpy(ptr, key, Length(key));
	ptr += Length(key);
	memcpy(ptr, value, Length(value));
	ptr += Length(value);
	scannerPtr->Forward();
      }
      else
	break;
    }

    *lenp = ptr - dst;

    return more;
  }

}
