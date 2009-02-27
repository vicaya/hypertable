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

#ifndef HYPERTABLE_BLOOM_FILTER_H
#define HYPERTABLE_BLOOM_FILTER_H

#include <cmath>
#include <limits.h>
#include "Common/StaticBuffer.h"
#include "Common/MurmurHash.h"
#include "Common/Logger.h"


namespace Hypertable {

/**
 * A space-efficent probabilistic set for membership test, false postives
 * are possible, but false negatives are not.
 */
template <class HasherT = MurmurHash2>
class BasicBloomFilter {
public:
  BasicBloomFilter(size_t element_count, float false_positive_prob) {
    m_element_count = element_count;
    m_false_positive_prob = false_positive_prob;
    double num_hashes = -std::log(m_false_positive_prob) / std::log(2);
    m_num_hash_functions = (size_t)num_hashes;
    m_num_bits = (size_t)(m_element_count * num_hashes / std::log(2));
    HT_ASSERT(m_num_bits != 0);
    m_num_bytes = (m_num_bits / CHAR_BIT) + (m_num_bits % CHAR_BIT ? 1 : 0);
    m_bloom_bits = new uint8_t[m_num_bytes];

    for(unsigned ii = 0; ii< m_num_bytes; ++ii)
      m_bloom_bits[ii] = 0x00;

    HT_DEBUG_OUT <<"num funcs="<< m_num_hash_functions
                 <<" num bits="<< m_num_bits <<" num bytes="<< m_num_bytes
                 <<" bits per element="<< double(m_num_bits) / element_count
                 << HT_END;
  }

  ~BasicBloomFilter() {
    delete[] m_bloom_bits;
  }

  /* XXX/review static functions to expose the bloom filter parameters, given
   1) probablility and # keys
   2) #keys and fix the total size (m), to calculate the optimal
      probability - # hash functions to use
  */

  void insert(const void *key, size_t len) {
    uint32_t hash = len;

    for (size_t i = 0; i < m_num_hash_functions; ++i) {
      hash = m_hasher(key, len, hash) % m_num_bits;
      m_bloom_bits[hash / CHAR_BIT] |= (1 << (hash % CHAR_BIT));
    }
  }

  void insert(const String& key) {
    insert(key.c_str(), key.length());
  }

  bool may_contain(const void *key, size_t len) const {
    uint32_t hash = len;
    uint8_t byte_mask;
    uint8_t byte;

    for (size_t i = 0; i < m_num_hash_functions; ++i) {
      hash = m_hasher(key, len, hash) % m_num_bits;
      byte = m_bloom_bits[hash / CHAR_BIT];
      byte_mask = (1 << (hash % CHAR_BIT));

      if ( (byte & byte_mask) == 0 ) {
        return false;
      }
    }
    return true;
  }

  bool may_contain(const String& key) const {
    return may_contain(key.c_str(), key.length());
  }

  void serialize(StaticBuffer& buf) {
    buf.set(m_bloom_bits, m_num_bytes, false);
  }

  uint8_t* ptr(void) {
    return m_bloom_bits;
  }

  size_t size(void) {
    return m_num_bytes;
  }

private:
  HasherT    m_hasher;
  size_t     m_element_count;
  float      m_false_positive_prob;
  size_t     m_num_hash_functions;
  size_t     m_num_bits;
  size_t     m_num_bytes;
  uint8_t   *m_bloom_bits;
};

typedef BasicBloomFilter<> BloomFilter;

} //namespace Hypertable

#endif // HYPERTABLE_BLOOM_FILTER_H
