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

#include <cstring>

extern "C" {
#include <stdint.h>
}

#include "ReferenceCount.h"

namespace Hypertable {

  class DynamicBuffer : public ReferenceCount {
  public:

    explicit DynamicBuffer(size_t initialSize = 0, bool own_buffer = true) :
        size(initialSize), own(own_buffer) {
      if (size)
        base = ptr = new uint8_t[size];
      else
        base = ptr = 0;
    }

    ~DynamicBuffer() { if (own) delete [] base; }

    size_t remaining() const { return size - (ptr-base); }

    size_t fill() const { return ptr-base; }

    bool empty() const { return ptr==base; }

    /**
     * Ensure space for additional data
     * Will grow the space to 1.5 of the needed space with existing data
     * unchanged.
     *
     * @param len - additional data in bytes
     */
    void ensure(size_t len) {
      if (len > remaining())
        grow((fill() + len) * 3 / 2);
    }

    /**
     * Reserve space for additional data
     * Will grow the space to exactly what's needed. Existing data is NOT
     * preserved by default
     */
    void reserve(size_t len, bool nocopy = false) {
      if (len > remaining())
        grow(fill() + len, nocopy);
    }

    uint8_t *add_unchecked(const void *data, size_t len) {
      if (data == 0)
        return 0;
      uint8_t *rptr = ptr;
      memcpy(ptr, data, len);
      ptr += len;
      return rptr;
    }

    uint8_t *add(const void *data, size_t len) {
      ensure(len);
      return add_unchecked(data, len);
    }

    void set(const void *data, size_t len) {
      clear();
      reserve(len);
      add_unchecked(data, len);
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

    void grow(size_t new_size, bool nocopy = false) {
      uint8_t *new_buf = new uint8_t[new_size];

      if (!nocopy && base)
        memcpy(new_buf, base, ptr-base);

      ptr = new_buf + (ptr-base);
      if (own)
        delete [] base;
      base = new_buf;
      size = new_size;
    }

    uint8_t *base;
    uint8_t *ptr;
    uint32_t size;
    bool own;
  };
  typedef intrusive_ptr<DynamicBuffer> DynamicBufferPtr;

} // namespace Hypertable



#endif // HYPERTABLE_DYNAMICBUFFER_H
