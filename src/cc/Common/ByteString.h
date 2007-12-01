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

#ifndef HYPERTABLE_BYTESTRING_H
#define HYPERTABLE_BYTESTRING_H

#include <ext/hash_map>
#include <iostream>

#include <boost/shared_array.hpp>
#include <boost/shared_ptr.hpp>

#include "DynamicBuffer.h"

namespace Hypertable {

  typedef struct {
    uint16_t   len;
    uint8_t    data[1];
  } __attribute__((packed)) ByteString16T;

  typedef struct {
    uint32_t   len;
    uint8_t    data[1];
  } __attribute__((packed)) ByteString32T;

  inline size_t Length(const ByteString32T *bs) {
    return (bs == 0) ? sizeof(int32_t) : bs->len + sizeof(bs->len);
  }

  inline const ByteString32T *Skip(const ByteString32T *bs) {
    return (const ByteString32T *)(((const uint8_t *)bs) + Length(bs));
  }

  inline ByteString32T *CreateCopy(const ByteString32T *bs) {
    size_t len = (bs == 0) ? sizeof(int32_t) : Length(bs);
    ByteString32T *newBs = (ByteString32T *)new uint8_t [ len  ];
    if (bs == 0)
      newBs->len = 0;
    else
      memcpy(newBs, bs, len);
    return newBs;
  }

  inline void CreateAndAppend(DynamicBuffer &dst_buf, const char *str) {
    ByteString32T *bs;
    dst_buf.ensure(strlen(str) + 6);
    bs = (ByteString32T *)dst_buf.ptr;
    bs->len = strlen(str);
    strcpy((char *)bs->data, str);
    dst_buf.ptr += Length(bs);
  }

  inline void CreateAndAppend(DynamicBuffer &dst_buf, const void *value, uint32_t value_len) {
    ByteString32T *bs;
    dst_buf.ensure(value_len + 4);
    bs = (ByteString32T *)dst_buf.ptr;
    bs->len = value_len;
    if (value_len > 0)
      memcpy(bs->data, value, value_len);
    dst_buf.ptr += Length(bs);
  }

  inline void Create(ByteString32T *bs, const void *data, uint32_t len) {
    bs->len = len;
    if (len)
      memcpy(bs->data, data, len);
  }

  inline ByteString32T *Create(const void *data, uint32_t len) {
    ByteString32T *bs = (ByteString32T *)new uint8_t [ sizeof(ByteString32T) + len ];
    Create(bs, data, len);
    bs->data[len] = 0;
    return bs;
  }

  inline void Destroy(ByteString32T *bs) {
    delete [] (uint8_t *)bs;
  }

  class ByteString32Ptr : public boost::shared_array<const uint8_t> {
  public:
    ByteString32Ptr() : boost::shared_array<const uint8_t>(0) { return; }
    ByteString32Ptr(ByteString32T *key) : boost::shared_array<const uint8_t>((uint8_t *)key) { return; }
    ByteString32T *get() const {
      return (ByteString32T *)boost::shared_array<const uint8_t>::get();
    }
    void reset(ByteString32T *bs) {
      boost::shared_array<const uint8_t>::reset((const uint8_t *)bs);
    }
    ByteString32T * operator-> () const // never throws
    {
      return (ByteString32T *)boost::shared_array<const uint8_t>::get();
    }
  };

  /**
   * Less than operator for ByteString32T
   */
  inline bool operator<(const ByteString32T &bs1, const ByteString32T &bs2) {
    uint32_t len = (bs1.len < bs2.len) ? bs1.len : bs2.len;
    int cmp = memcmp(bs1.data, bs2.data, len);
    return (cmp==0) ? bs1.len < bs2.len : cmp < 0;
  }

  /**
   * Equality operator for ByteString32T
   */
  inline bool operator==(const ByteString32T &bs1, const ByteString32T &bs2) {
    if (bs1.len != bs2.len)
      return false;
    return memcmp(bs1.data, bs2.data, bs1.len) == 0;
  }  

  /**
   * Inequality operator for ByteString32T
   */
  inline bool operator!=(const ByteString32T &bs1, const ByteString32T &bs2) {
    return !(bs1 == bs2);
  }

  struct ltByteString32 {
    bool operator()(const ByteString32T * bs1ptr, const ByteString32T *bs2ptr) const {
      return *bs1ptr < *bs2ptr;
    }
  };

  /**
   *  For testing
   */
  inline std::ostream &operator <<(std::ostream &os, const ByteString32T &bs) {
    DynamicBuffer dbuf(bs.len + 3);
    *dbuf.ptr++ = '\'';
    dbuf.addNoCheck(bs.data, bs.len);
    *dbuf.ptr++ = '\'';
    *dbuf.ptr = 0;
    os << (const char *)dbuf.buf;
    return os;
  }

  /**
   *  For testing
   */
  inline std::ostream &operator <<(std::ostream &os, const ByteString16T &bs) {
    DynamicBuffer dbuf(bs.len + 3);
    *dbuf.ptr++ = '\'';
    dbuf.addNoCheck(bs.data, bs.len);
    *dbuf.ptr++ = '\'';
    *dbuf.ptr = 0;
    os << (const char *)dbuf.buf;
    return os;
  }

  typedef boost::shared_array<uint8_t> ByteString16Ptr;

}


namespace __gnu_cxx {
  template<> struct hash< Hypertable::ByteString16T * >  {
    size_t operator()( const Hypertable::ByteString16T *bs ) const {
      if (bs == 0)
	return size_t(0);
      unsigned long __h = 0;
      for (int __i=0; __i<bs->len; __i++)
	__h = 5 * __h + bs->data[__i];
      return size_t(__h);
    }
  };
}


#endif // HYPERTABLE_BYTESTRING_H

