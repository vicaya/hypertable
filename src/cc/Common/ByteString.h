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


#ifndef HYPERTABLE_BYTESTRING_H
#define HYPERTABLE_BYTESTRING_H

#include <ext/hash_map>
#include <iostream>

#include <boost/shared_array.hpp>
#include <boost/shared_ptr.hpp>

#include "DynamicBuffer.h"

using namespace hypertable;
using namespace std;

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
  template<> struct hash< ByteString16T * >  {
    size_t operator()( const ByteString16T *bs ) const {
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

