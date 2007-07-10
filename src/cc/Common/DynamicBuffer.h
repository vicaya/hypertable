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


#ifndef HYPERTABLE_DYNAMICBUFFER_H
#define HYPERTABLE_DYNAMICBUFFER_H

extern "C" {
#include <stdint.h>
#include <string.h>
}

namespace hypertable {

  class DynamicBuffer {

  public:

    DynamicBuffer(size_t initialSize) : buf(0), ptr(0), size(initialSize), checksum(0) {
      if (initialSize == 0)
	ptr = buf = 0;
      else
	buf = ptr = new uint8_t [ size ];
    }

    ~DynamicBuffer() { delete [] buf; }

    size_t remaining() { return size - (ptr-buf); }

    size_t fill() { return ptr-buf; }

    void ensure(size_t len) {
      if (len > remaining())
	grow((size_t)(1.5 * (fill()+len)));
    }

    void reserve(size_t len) {
      if (len > remaining())
	grow(fill() + len);
    }

    uint8_t *addNoCheck(const void *data, size_t len) {
      if (data == 0)
	return 0;
      uint8_t *rptr = ptr;
      memcpy(ptr, data, len);
      for (size_t i=0; i<len; i++)
	checksum += (uint32_t)((uint8_t *)data)[i];
      ptr += len;
      return rptr;
    }

    uint8_t *add(const void *data, size_t len) {
      if (len > remaining())
	grow((size_t)(1.5 * (size+len)));
      return addNoCheck(data, len);
    }

    void set(const void *data, size_t len) {
      ptr = buf;
      checksum = 0;
      add(data, len);
    }

    void clear() {
      ptr = buf;
      checksum = 0;
    }

    void free() {
      delete [] buf;
      buf = ptr = 0;
      size = 0;
    }

    uint8_t *release(size_t *lenp=0) {
      uint8_t *rbuf = buf;
      if (lenp)
	*lenp = fill();
      ptr = buf = 0;
      size = 0;
      checksum = 0;
      return rbuf;
    }

    void grow(size_t newSize) {
      uint8_t *newBuf = new uint8_t [ newSize ];
      if (buf != 0)
	memcpy(newBuf, buf, ptr-buf);
      ptr = newBuf + (ptr-buf);
      delete [] buf;
      buf = newBuf;
      size = newSize;
    }

    uint8_t *buf;
    uint8_t *ptr;
    size_t   size;
    uint16_t checksum;
  };

}

#endif // HYPERTABLE_DYNAMICBUFFER_H
