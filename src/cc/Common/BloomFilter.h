/**
 * Copyright (C) 2007 Naveen Koorakula
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

#ifndef INCLUDE_BLOOM_FILTER_H
#define INCLUDE_BLOOM_FILTER_H

#include <cmath>
#include "Common/SuperFastHash.h"
#include "Common/StaticBuffer.h"

#define BITS_PER_BYTE 8

namespace Hypertable {

class BloomFilter {
public:
  BloomFilter(const std::size_t& element_count, float false_positive_probability)
    {
      m_element_count = element_count;
      m_false_positive_probability = false_positive_probability;
      m_num_hash_functions = (size_t)(-1 * std::log(m_false_positive_probability) / std::log(2));
      m_num_bits = (size_t)(m_element_count * m_num_hash_functions / std::log(2));
      m_num_bytes = (m_num_bits / BITS_PER_BYTE) + static_cast<int>(m_num_bits % BITS_PER_BYTE != 0);
      m_bloom_bits = new uint8_t[m_num_bytes];
    }

  /* XXX/review static functions to expose the bloom filter parameters, given
   1) probablility and # keys
   2) #keys and fix the total size (m), to calculate the optimal
      probability - # hash functions to use
  */
  
  void insert(const void *key, const size_t len) {
    uint32_t hash = (uint32_t) len;
    for (size_t i = 0; i < m_num_hash_functions; ++i) {
      hash = SuperFastHash((const char*) key, len, hash) % m_num_bits;
      m_bloom_bits[hash/BITS_PER_BYTE] |= (uint8_t(1) << static_cast<int>(hash % BITS_PER_BYTE));
    }
  }

  void insert(const std::string& key) {
    insert(key.c_str(), key.length());
  }

  bool may_contain(const void *key, const std::size_t len) const {
    uint32_t hash = (uint32_t) len;
    for (size_t i = 0; i < m_num_hash_functions; ++i) {
      hash = SuperFastHash((const char*) key, len, hash) % m_num_bits;
      if ( (m_bloom_bits[hash/BITS_PER_BYTE] & (uint8_t(1) << static_cast<int>(hash % BITS_PER_BYTE))) == 0 ) {
        return false;
      }
    }
    return true;
  }
  
  bool may_contain(const std::string& key) const {
    return may_contain(key.c_str(), key.length());
  }

  void serialize(StaticBuffer& buf) {
    buf.set(m_bloom_bits, m_num_bytes);
  }
  
  uint8_t* ptr(void) {
    return m_bloom_bits;
  }

  size_t size(void) {
    return m_num_bytes;
  }
  
private:
  std::size_t     m_element_count;
  float           m_false_positive_probability;
  std::size_t     m_num_hash_functions;
  std::size_t     m_num_bits;
  std::size_t     m_num_bytes;

  uint8_t         *m_bloom_bits;
};

} //namespace Hypertable

#endif
