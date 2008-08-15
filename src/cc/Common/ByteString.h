/**
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

#ifndef HYPERTABLE_BYTESTRING_H
#define HYPERTABLE_BYTESTRING_H

#include <iostream>

#include <boost/shared_array.hpp>
#include <boost/shared_ptr.hpp>

#include "DynamicBuffer.h"
#include "Serialization.h"

namespace Hypertable {

  class ByteString {
  public:
    ByteString() : ptr(0) { return; }
    ByteString(const uint8_t *buf) : ptr(buf) { return; }

    size_t length() const {
      if (ptr == 0)
        return 1;
      const uint8_t *tmp_ptr = ptr;
      uint32_t len = Serialization::decode_vi32(&tmp_ptr);
      return (tmp_ptr - ptr) + len;
    }

    uint8_t *next() {
      uint8_t *rptr = (uint8_t *)ptr;
      uint32_t len = Serialization::decode_vi32(&ptr);
      ptr += len;
      return rptr;
    }

    size_t decode_length(const uint8_t **dptr) const {
      *dptr = ptr;
      return Serialization::decode_vi32(dptr);
    }

    size_t write(uint8_t *dst) const {
      size_t len = length();
      if (ptr == 0)
        Serialization::encode_vi32(&dst, 0);
      else
        memcpy(dst, ptr, len);
      return len;
    }

    const char *str() const {
      const uint8_t *rptr = ptr;
      Serialization::decode_vi32(&rptr);
      return (const char *)rptr;
    }

    operator bool () const {
      return ptr != 0;
    }

    const uint8_t *ptr;
  };

  inline void append_as_byte_string(DynamicBuffer &dst_buf, const char *str) {
    size_t value_len = strlen(str);
    dst_buf.ensure(7 + value_len);
    Serialization::encode_vi32(&dst_buf.ptr, value_len);
    if (value_len > 0) {
      memcpy(dst_buf.ptr, str, value_len);
      dst_buf.ptr += value_len;
      *dst_buf.ptr = 0;
    }
  }

  inline void
  append_as_byte_string(DynamicBuffer &dst_buf, const void *value,
                        uint32_t value_len) {
    dst_buf.ensure(7 + value_len);
    Serialization::encode_vi32(&dst_buf.ptr, value_len);
    if (value_len > 0) {
      memcpy(dst_buf.ptr, value, value_len);
      dst_buf.ptr += value_len;
      *dst_buf.ptr = 0;
    }
  }
}


#endif // HYPERTABLE_BYTESTRING_H

