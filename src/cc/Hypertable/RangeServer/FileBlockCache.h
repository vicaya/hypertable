/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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
#include <boost/thread/mutex.hpp>

#include "Common/atomic.h"

namespace boost {
  std::size_t hash_value(uint64_t llval);
}

namespace Hypertable {
  using namespace boost::multi_index;

  class FileBlockCache {

    static atomic_t ms_next_file_id;

  public:
    FileBlockCache(uint64_t max_memory)
        : m_max_memory(max_memory), m_avail_memory(max_memory) {  }
    ~FileBlockCache();

    bool checkout(int file_id, uint32_t file_offset, uint8_t **blockp,
                  uint32_t *lengthp);
    void checkin(int file_id, uint32_t file_offset);
    bool insert_and_checkout(int file_id, uint32_t file_offset,
                             uint8_t *block, uint32_t length);
    bool contains(int file_id, uint32_t file_offset);

    static int get_next_file_id() {
      return atomic_inc_return(&ms_next_file_id);
    }

  private:

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
      uint64_t key() const { return ((uint64_t)file_id << 32) | file_offset; }
    };

    struct DecrementRefCount {
      void operator()(BlockCacheEntry &entry) {
        entry.ref_count--;
      }
    };

    typedef boost::multi_index_container<
      BlockCacheEntry,
      indexed_by<
        sequenced<>,
        hashed_unique<const_mem_fun<BlockCacheEntry, uint64_t,
                      &BlockCacheEntry::key> >
      >
    > BlockCache;

    boost::mutex  m_mutex;
    BlockCache    m_cache;
    uint64_t      m_max_memory;
    uint64_t      m_avail_memory;
  };

}


#endif // HYPERTABLE_FILEBLOCKCACHE_H
