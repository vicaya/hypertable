/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */
#ifndef HYPERTABLE_DYNAMICBUFFER_H
#define HYPERTABLE_DYNAMICBUFFER_H

extern "C" {
#include <stdint.h>
#include <string.h>
}

#include "Buffer.h"

namespace Hypertable {

  class DynamicBuffer : public Buffer {

  public:

    DynamicBuffer(size_t initial_size, bool own_buffer=true) {
      size = initial_size;
      own = own_buffer;
      if (size == 0)
	ptr = base = 0;
      else
	base = ptr = new uint8_t [ size ];
    }

    ~DynamicBuffer() { if (own) delete [] base; }

    size_t remaining() const { return size - (ptr-base); }

    size_t fill() const { return ptr-base; }

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
      ptr = base;
      add(data, len);
    }

    void clear() {
      ptr = base;
    }

    void free() {
      if (own)
	delete [] base;
      base = ptr = 0;
      size = 0;
    }

    uint8_t *release(size_t *lenp=0) {
      uint8_t *rbuf = base;
      if (lenp)
	*lenp = fill();
      ptr = base = 0;
      size = 0;
      return rbuf;
    }

    void grow(size_t newSize, bool nocopy = false) {
      uint8_t *newBuf = new uint8_t [ newSize ];

      if (!nocopy && base)
	memcpy(newBuf, base, ptr-base);

      ptr = newBuf + (ptr-base);
      if (own)
	delete [] base;
      base = newBuf;
      size = newSize;
    }

    uint8_t *ptr;

  private:
    DynamicBuffer&  operator = (const DynamicBuffer& other) { return *this; }
    DynamicBuffer(const DynamicBuffer& other) { }
  };

}

#endif // HYPERTABLE_DYNAMICBUFFER_H
