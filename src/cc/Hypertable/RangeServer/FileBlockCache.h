/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERTABLE_FILEBLOCKCACHE_H
#define HYPERTABLE_FILEBLOCKCACHE_H

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/mem_fun.hpp>
#include <boost/multi_index/sequenced_index.hpp>

#include "Common/Mutex.h"
#include "Common/atomic.h"

namespace Hypertable {
  using namespace boost::multi_index;

  class FileBlockCache {

    static atomic_t ms_next_file_id;

  public:
    FileBlockCache(int64_t min_memory, int64_t max_memory)
      : m_min_memory(min_memory), m_max_memory(max_memory), m_limit(max_memory),
	m_available(max_memory), m_accesses(0), m_hits(0) { HT_ASSERT(min_memory <= max_memory); }
    ~FileBlockCache();

    bool checkout(int file_id, uint32_t file_offset, uint8_t **blockp,
                  uint32_t *lengthp);
    void checkin(int file_id, uint32_t file_offset);
    bool insert_and_checkout(int file_id, uint32_t file_offset,
                             uint8_t *block, uint32_t length);
    bool contains(int file_id, uint32_t file_offset);

    void increase_limit(int64_t amount);

    /**
     * Lowers the memory limit.  This method will free memory if necessary
     * to bring the limit down by 'amount'  It will not reduce the limit
     * below min_memory
     *
     * @param amount Amount to reduce limit by
     * @return amount of memory deallocated by the method
     */
    int64_t decrease_limit(int64_t amount);

    int64_t get_limit() {
      ScopedLock lock(m_mutex);
      return m_limit;
    }

    /**
     * Sets limit to memory currently used, it will not reduce the limit
     * below min_memory
     */
    void cap_memory_use() {
      ScopedLock lock(m_mutex);
      int64_t memory_used = m_limit - m_available;
      if (memory_used > m_min_memory) {
        m_limit -= m_available;
        m_available = 0;
      }
      else {
        m_limit = m_min_memory;
        m_available = m_limit - memory_used;
      }
    }

    int64_t memory_used() {
      ScopedLock lock(m_mutex);
      return (int64_t)(m_limit - m_available);
    }

    int64_t available() {
      ScopedLock lock(m_mutex);
      return m_available;
    }

    static int get_next_file_id() {
      return atomic_inc_return(&ms_next_file_id);
    }
    void get_stats(uint64_t *max_memoryp, uint64_t *available_memoryp,
                   uint64_t *accessesp, uint64_t *hitsp);
  private:

    int64_t make_room(int64_t amount);

    class BlockCacheEntry {
    public:
      BlockCacheEntry() : file_id(-1), file_offset(0), block(0), length(0),
          ref_count(0) { return; }
      BlockCacheEntry(int id, uint32_t offset) : file_id(id),
          file_offset(offset), block(0), length(0), ref_count(0) { return; }

      int      file_id;
      uint32_t file_offset;
      uint8_t  *block;
      uint32_t length;
      uint32_t ref_count;
      int64_t key() const { return ((int64_t)file_id << 32) | file_offset; }
    };

    struct DecrementRefCount {
      void operator()(BlockCacheEntry &entry) {
        entry.ref_count--;
      }
    };

    struct HashI64 {
      std::size_t operator()(int64_t x) const {
        return (std::size_t)(x >> 32) ^ (std::size_t)x;
      }
    };

    typedef boost::multi_index_container<
      BlockCacheEntry,
      indexed_by<
        sequenced<>,
        hashed_unique<const_mem_fun<BlockCacheEntry, int64_t,
                      &BlockCacheEntry::key>, HashI64>
      >
    > BlockCache;

    typedef BlockCache::nth_index<0>::type Sequence;
    typedef BlockCache::nth_index<1>::type HashIndex;

    Mutex         m_mutex;
    BlockCache    m_cache;
    int64_t      m_min_memory;
    int64_t      m_max_memory;
    int64_t      m_limit;
    int64_t      m_available;
    int32_t      m_blocks;
    uint64_t     m_accesses;
    uint64_t     m_hits;
  };

}


#endif // HYPERTABLE_FILEBLOCKCACHE_H
