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

#ifndef HYPERTABLE_STATICBUFFER_H
#define HYPERTABLE_STATICBUFFER_H

#include "DynamicBuffer.h"

namespace Hypertable {

  class StaticBuffer {
  public:
    explicit StaticBuffer(size_t len) :
        base(new uint8_t[len]), size(len), own(true) {}

    StaticBuffer(void *data, uint32_t len, bool take_ownership=true) :
        base((uint8_t *)data), size(len), own(take_ownership) { }

    StaticBuffer() : base(0), size(0), own(true) { }

    StaticBuffer(DynamicBuffer &dbuf) {
      base = dbuf.base;
      size = dbuf.fill();
      own = dbuf.own;
      if (own) {
        dbuf.base = dbuf.ptr = 0;
        dbuf.size = 0;
      }
    }

    ~StaticBuffer() {
      if (own)
        delete [] base;
    }

    /**
     * Copy constructor.
     *
     * WARNING: This assignment operator will cause the ownership of the buffer
     * to transfer to the lvalue buffer if the own flag is set to 'true' in the
     * buffer being copied.  The buffer being copied will be modified to have
     * it's 'own' flag set to false and the 'base' pointer will be set to NULL.
     * In other words, the buffer being copied is no longer usable after the
     * assignment.
     */
    StaticBuffer(StaticBuffer& other) {
      base = other.base;
      size = other.size;
      own = other.own;
      if (own) {
        other.own = false;
        other.base = 0;
      }
    }

    /**
     * Assignment operator.
     *
     * WARNING: This assignment operator will cause the ownership of the buffer
     * to transfer to the lvalue buffer if the own flag is set to 'true' in the
     * buffer being copied.  The buffer being copied will be modified to have
     * it's 'own' flag set to false and the 'base' pointer will be set to NULL.
     * In other words, the buffer being copied is no longer usable after the
     * assignment.
     */
    StaticBuffer &operator=(StaticBuffer &other) {
      base = other.base;
      size = other.size;
      own = other.own;
      if (own) {
        other.own = false;
        other.base = 0;
      }
      return *this;
    }

    /**
     * Assignment operator for DynamicBuffer
     */
    StaticBuffer &operator=(DynamicBuffer &dbuf) {
      base = dbuf.base;
      size = dbuf.fill();
      own = dbuf.own;
      if (own) {
        dbuf.base = dbuf.ptr = 0;
        dbuf.size = 0;
      }
      return *this;
    }

    void set(uint8_t *data, uint32_t len, bool take_ownership=true) {
      if (own)
        delete [] base;
      base = data;
      size = len;
      own = take_ownership;
    }

    void free() {
      if (own)
        delete [] base;
      base = 0;
      size = 0;
    }

    uint8_t *base;
    uint32_t size;
    bool own;
  };

} // namespace Hypertable

#endif // HYPERTABLE_STATICBUFFER_H

