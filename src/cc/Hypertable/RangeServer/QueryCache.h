/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Hypertable, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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

#ifndef HYPERTABLE_QUERYCACHE_H
#define HYPERTABLE_QUERYCACHE_H

#include <cstring>

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/mem_fun.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index/member.hpp>

#include <boost/shared_array.hpp>

#include "Common/Mutex.h"
#include "Common/atomic.h"
#include "Common/Checksum.h"

namespace Hypertable {
  using namespace boost::multi_index;

  class QueryCache {

  public:

    class Key {
    public:
      bool operator==(const Key &other) const {
	return memcmp(digest, other.digest, 16) == 0;
      }
      uint8_t digest[16];
    };

    class RowKey {
    public:
      RowKey(uint32_t tid, const char *r) : table_id(tid), row(r) {
	hash = table_id ^ fletcher32(row, strlen(row));
      }
      bool operator==(const RowKey &other) const {
	return table_id == other.table_id && !strcmp(row, other.row);
      }
      uint32_t table_id;
      const char *row;
      uint32_t hash;
    };

    QueryCache(uint64_t max_memory)
      : m_max_memory(max_memory), m_avail_memory(max_memory),
        m_total_lookup_count(0), m_total_hit_count(0), m_recent_hit_count(0) { }

    bool insert(Key *key, uint32_t table_id, const char *row,
                boost::shared_array<uint8_t> &result, uint32_t result_length);

    bool lookup(Key *key, boost::shared_array<uint8_t> &result, uint32_t *lenp);

    void invalidate(uint32_t table_id, const char *row);

    void dump();

    uint64_t available_memory() { return m_avail_memory; }

    void get_stats(uint64_t &max_memory, uint64_t &available_memory,
                   uint64_t &total_lookups, uint64_t &total_hits);

  private:

    class QueryCacheEntry {
    public:
      QueryCacheEntry(Key &k, uint32_t table_id, const char *rw,
		      boost::shared_array<uint8_t> &res, uint32_t rlen) :
	key(k), row_key(table_id, rw), result(res), result_length(rlen) { }
      Key lookup_key() const { return key; }
      RowKey invalidate_key() const { return row_key; }
      void dump() { std::cout << row_key.table_id << ":" << row_key.row << "\n"; }
      Key key;
      RowKey row_key;
      boost::shared_array<uint8_t> result;
      uint32_t result_length;
    };

    struct KeyHash {
      std::size_t operator()(Key k) const {
        std::size_t *sptr = (std::size_t *)k.digest;
        return sptr[0];
      }
    };

    struct RowKeyHash {
      std::size_t operator()(RowKey k) const {
        return k.hash;
      }
    };

    typedef boost::multi_index_container<
      QueryCacheEntry,
      indexed_by<
        sequenced<>,
        hashed_unique<const_mem_fun<QueryCacheEntry, Key,
		      &QueryCacheEntry::lookup_key>, KeyHash>,
        hashed_non_unique<const_mem_fun<QueryCacheEntry, RowKey,
		          &QueryCacheEntry::invalidate_key>, RowKeyHash>
      >
    > Cache;

    typedef Cache::nth_index<0>::type Sequence;
    typedef Cache::nth_index<1>::type LookupHashIndex;
    typedef Cache::nth_index<2>::type InvalidateHashIndex;

    Mutex     m_mutex;
    Cache     m_cache;
    uint64_t  m_max_memory;
    uint64_t  m_avail_memory;
    uint64_t  m_total_lookup_count;
    uint64_t  m_total_hit_count;
    uint32_t  m_recent_hit_count;
  };

}


#endif // HYPERTABLE_QUERYCACHE_H
