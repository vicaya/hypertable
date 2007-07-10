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


#ifndef HYPERTABLE_KEY_H
#define HYPERTABLE_KEY_H

#include <iostream>

#include <boost/shared_array.hpp>

#include "Common/ByteString.h"

using namespace hypertable;

namespace hypertable {

  static const uint32_t FLAG_DELETE_ROW   = 0x00;
  static const uint32_t FLAG_DELETE_CELL  = 0x01;
  static const uint32_t FLAG_INSERT       = 0xFF;

  typedef struct {
    uint32_t   len;
    uint8_t    data[1];
  } __attribute__((packed)) KeyT;

  typedef struct {
    const char    *rowKey;
    const char    *columnQualifier;
    const uint8_t *endPtr;
    uint64_t    timestamp;
    uint8_t     columnFamily;
    uint8_t     flag;
  } __attribute__((packed)) KeyComponentsT;

  inline size_t Length(const KeyT *key) {
    return key->len + sizeof(key->len);
  }

  KeyT *CreateKey(uint8_t flag, const char *rowKey, uint8_t columnFamily, const char *columnQualifier, uint64_t timestamp);

  inline KeyT *CreateCopy(const KeyT *key) {
    size_t len = Length(key);
    KeyT *newKey = (KeyT *)new uint8_t [ len ];
    memcpy(newKey, key, len);
    return newKey;
  }
  
  std::ostream &operator<<(std::ostream &os, const KeyT &key);

  bool Load(const KeyT *key, KeyComponentsT &comps);


  /**
   * Less than operator for KeyT
   */
  inline bool operator<(const KeyT &k1, const KeyT &k2) {
    uint32_t len = (k1.len < k2.len) ? k1.len : k2.len;
    int cmp = memcmp(k1.data, k2.data, len);
    return (cmp==0) ? k1.len < k2.len : cmp < 0;
  }

  /**
   * Equality operator for KeyT
   */
  inline bool operator==(const KeyT &k1, const KeyT &k2) {
    if (k1.len != k2.len)
      return false;
    return memcmp(k1.data, k2.data, k1.len) == 0;
  }  

  /**
   * Inequality operator for KeyT
   */
  inline bool operator!=(const KeyT &k1, const KeyT &k2) {
    return !(k1 == k2);
  }

  struct ltKey {
    bool operator()(const KeyT * k1ptr, const KeyT *k2ptr) const {
      return *k1ptr < *k2ptr;
    }
  };

  /**
   *  KeyPtr class (and less than functor)
   */

  class KeyPtr : public boost::shared_array<const uint8_t> {
  public:
    KeyPtr() : boost::shared_array<const uint8_t>(0) { return; }
    KeyPtr(KeyT *key) : boost::shared_array<const uint8_t>((uint8_t *)key) { return; }
    KeyT *get() const {
      return (KeyT *)boost::shared_array<const uint8_t>::get();
    }
    void reset(KeyT *key) {
      boost::shared_array<const uint8_t>::reset((const uint8_t *)key);
    }
  };

  struct ltKeyPtr {
    bool operator()(const KeyPtr &kp1, const KeyPtr &kp2) const {
      KeyT *k1ptr = (KeyT *)kp1.get();
      KeyT *k2ptr = (KeyT *)kp2.get();
      return *k1ptr < *k2ptr;
    }
  };


}

#endif // HYPERTABLE_KEY_H

