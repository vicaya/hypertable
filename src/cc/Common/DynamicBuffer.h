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
#ifndef HYPERTABLE_DYNAMICBUFFER_H
#define HYPERTABLE_DYNAMICBUFFER_H

extern "C" {
#include <stdint.h>
#include <string.h>
}

namespace Hypertable {

  class DynamicBuffer {

  public:

    DynamicBuffer(size_t initialSize) : buf(0), ptr(0), size(initialSize) {
      if (initialSize == 0)
	ptr = buf = 0;
      else
	buf = ptr = new uint8_t [ size ];
    }

    ~DynamicBuffer() { delete [] buf; }

    size_t remaining() const { return size - (ptr-buf); }

    size_t fill() const { return ptr-buf; }

    void ensure(size_t len) {
      if (len > remaining())
	grow((size_t)((fill()+len) * 3 / 2));
    }

    void reserve(size_t len, bool nocopy = false) {
      if (len > remaining())
	grow(fill() + len, nocopy);
    }

    uint8_t *addNoCheck(const void *data, size_t len) {
      if (data == 0)
	return 0;
      uint8_t *rptr = ptr;
      memcpy(ptr, data, len);
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
      add(data, len);
    }

    void clear() {
      ptr = buf;
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
      return rbuf;
    }

    void grow(size_t newSize, bool nocopy = false) {
      uint8_t *newBuf = new uint8_t [ newSize ];

      if (!nocopy && buf)
	memcpy(newBuf, buf, ptr-buf);

      ptr = newBuf + (ptr-buf);
      delete [] buf;
      buf = newBuf;
      size = newSize;
    }

    uint8_t *buf;
    uint8_t *ptr;
    size_t   size;
  };

}

#endif // HYPERTABLE_DYNAMICBUFFER_H
