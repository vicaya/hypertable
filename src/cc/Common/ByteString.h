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

namespace hypertable {

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

  inline ByteString32T *CreateCopy(const ByteString32T *bs) {
    size_t len = (bs == 0) ? sizeof(int32_t) : Length(bs);
    ByteString32T *newBs = (ByteString32T *)new uint8_t [ len  ];
    if (bs == 0)
      newBs->len = 0;
    else
      memcpy(newBs, bs, len);
    return newBs;
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
  template<> struct hash< hypertable::ByteString16T * >  {
    size_t operator()( const hypertable::ByteString16T *bs ) const {
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

