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

#ifndef HYPERTABLE_BLOOM_FILTER_WITH_CHECKSUM_H
#define HYPERTABLE_BLOOM_FILTER_WITH_CHECKSUM_H

#include <cmath>
#include <limits.h>
#include "Common/Checksum.h"
#include "Common/Filesystem.h"
#include "Common/Logger.h"
#include "Common/MurmurHash.h"
#include "Common/Serialization.h"
#include "Common/StaticBuffer.h"
#include "Common/StringExt.h"
#include "Common/System.h"

namespace Hypertable {

/**
 * A space-efficent probabilistic set for membership test, false postives
 * are possible, but false negatives are not.
 */
template <class HasherT = MurmurHash2>
class BasicBloomFilterWithChecksum {
public:
  BasicBloomFilterWithChecksum(size_t items_estimate, float false_positive_prob) {
    m_items_actual = 0;
    m_items_estimate = items_estimate;
    m_false_positive_prob = false_positive_prob;
    double num_hashes = -std::log(m_false_positive_prob) / std::log(2);
    m_num_hash_functions = (size_t)num_hashes;
    m_num_bits = (size_t)(m_items_estimate * num_hashes / std::log(2));
    if (m_num_bits == 0) {
      HT_THROWF(Error::EMPTY_BLOOMFILTER, "Num elements=%lu false_positive_prob=%.3f",
                (Lu)items_estimate, false_positive_prob);
    }
    m_num_bytes = (m_num_bits / CHAR_BIT) + (m_num_bits % CHAR_BIT ? 1 : 0);
    m_bloom_base = new uint8_t[4+m_num_bytes+HT_IO_ALIGNMENT_PADDING(4+m_num_bytes)];
    m_bloom_bits = m_bloom_base + 4;
    memset(m_bloom_base, 0, 4+m_num_bytes+HT_IO_ALIGNMENT_PADDING(4+m_num_bytes));

    HT_DEBUG_OUT <<"num funcs="<< m_num_hash_functions
                 <<" num bits="<< m_num_bits <<" num bytes="<< m_num_bytes
                 <<" bits per element="<< double(m_num_bits) / items_estimate
                 << HT_END;
  }

  BasicBloomFilterWithChecksum(size_t items_estimate, float bits_per_item, size_t num_hashes) {
    m_items_actual = 0;
    m_items_estimate = items_estimate;
    m_false_positive_prob = 0.0;
    m_num_hash_functions = num_hashes;
    m_num_bits = (size_t)((double)items_estimate * (double)bits_per_item);
    if (m_num_bits == 0) {
      HT_THROWF(Error::EMPTY_BLOOMFILTER, "Num elements=%lu bits_per_item=%.3f",
                (Lu)items_estimate, bits_per_item);
    }
    m_num_bytes = (m_num_bits / CHAR_BIT) + (m_num_bits % CHAR_BIT ? 1 : 0);
    m_bloom_base = new uint8_t[4+m_num_bytes+HT_IO_ALIGNMENT_PADDING(4+m_num_bytes)];
    m_bloom_bits = m_bloom_base + 4;
    memset(m_bloom_base, 0, 4+m_num_bytes+HT_IO_ALIGNMENT_PADDING(4+m_num_bytes));

    HT_DEBUG_OUT <<"num funcs="<< m_num_hash_functions
                 <<" num bits="<< m_num_bits <<" num bytes="<< m_num_bytes
                 <<" bits per element="<< double(m_num_bits) / items_estimate
                 << HT_END;
  }

  BasicBloomFilterWithChecksum(size_t items_estimate, size_t items_actual,
                 int64_t length, size_t num_hashes) {
    m_items_actual = items_actual;
    m_items_estimate = items_estimate;
    m_false_positive_prob = 0.0;
    m_num_hash_functions = num_hashes;
    m_num_bits = (size_t)length;
    if (m_num_bits == 0) {
      HT_THROWF(Error::EMPTY_BLOOMFILTER, "Estimated items=%lu actual items=%lu length=%lld num hashes=%lu",
                (Lu)items_estimate, (Lu)items_actual, (Lld)length, (Lu)num_hashes);
    }
    m_num_bytes = (m_num_bits / CHAR_BIT) + (m_num_bits % CHAR_BIT ? 1 : 0);
    m_bloom_base = new uint8_t[4+m_num_bytes+HT_IO_ALIGNMENT_PADDING(4+m_num_bytes)];
    m_bloom_bits = m_bloom_base + 4;
    memset(m_bloom_base, 0, 4+m_num_bytes+HT_IO_ALIGNMENT_PADDING(4+m_num_bytes));

    HT_DEBUG_OUT <<"num funcs="<< m_num_hash_functions
                 <<" num bits="<< m_num_bits <<" num bytes="<< m_num_bytes
                 <<" bits per element="<< double(m_num_bits) / items_estimate
                 << HT_END;
  }

  ~BasicBloomFilterWithChecksum() {
    delete[] m_bloom_base;
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
    m_items_actual++;
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
    buf.set(m_bloom_base, 4+m_num_bytes+HT_IO_ALIGNMENT_PADDING(4+m_num_bytes), false);
    uint8_t *ptr = buf.base;
    Serialization::encode_i32(&ptr, fletcher32(m_bloom_bits, m_num_bytes));
  }

  uint8_t* base(void) {
    return m_bloom_base;
  }
  
  void validate(String &filename) {
    const uint8_t *ptr = m_bloom_base;
    size_t remain = 4;
    uint32_t stored_checksum = Serialization::decode_i32(&ptr, &remain);
    uint32_t computed_checksum = fletcher32(m_bloom_bits, m_num_bytes);
    if (stored_checksum != computed_checksum)
      HT_THROW(Error::BLOOMFILTER_CHECKSUM_MISMATCH, filename.c_str());
  }

  size_t size(void) {
    return m_num_bytes;
  }

  size_t total_size(void) {
    return 4+m_num_bytes+HT_IO_ALIGNMENT_PADDING(4+m_num_bytes);
  }

  size_t get_num_hashes() { return m_num_hash_functions; }

  size_t get_length_bits() { return m_num_bits; }

  size_t get_items_estimate() { return m_items_estimate; }

  size_t get_items_actual() { return m_items_actual; }

private:
  HasherT    m_hasher;
  size_t     m_items_estimate;
  size_t     m_items_actual;
  float      m_false_positive_prob;
  size_t     m_num_hash_functions;
  size_t     m_num_bits;
  size_t     m_num_bytes;
  uint8_t   *m_bloom_bits;
  uint8_t   *m_bloom_base;
};

typedef BasicBloomFilterWithChecksum<> BloomFilterWithChecksum;

} //namespace Hypertable

#endif // HYPERTABLE_BLOOM_FILTER_WITH_CHECKSUM_H
