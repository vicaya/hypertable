/** -*- C++ -*-
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
#ifndef HYPERTABLE_FILEBLOCKCACHE_H
#define HYPERTABLE_FILEBLOCKCACHE_H

#include <boost/thread/mutex.hpp>

#include <ext/hash_map>

#include "Common/atomic.h"

namespace Hypertable {

  class FileBlockCache {

    typedef struct {
      int      fileId;
      uint32_t offset;
    } CacheKeyT;

    typedef struct cache_value {
      CacheKeyT  key;
      struct cache_value *prev;
      struct cache_value *next;
      uint8_t  *block;
      uint32_t length;
      uint32_t refCount;
    } CacheValueT;

    static atomic_t ms_next_file_id;

  public:

    FileBlockCache(uint64_t maxMemory) : m_mutex(), m_block_map(), m_head(0), m_tail(0) { return; }
    ~FileBlockCache();
    bool checkout(int fileId, uint32_t offset, uint8_t **blockp, uint32_t *lengthp);
    void checkin(int fileId, uint32_t offset);
    bool insert_and_checkout(int fileId, uint32_t offset, uint8_t *block, uint32_t length);

    static int get_next_file_id() {
      return atomic_inc_return(&ms_next_file_id);
    }

  private:

    void move_to_head(CacheValueT *cacheValue);

    struct hashCacheKey {
      size_t operator()( const CacheKeyT &key ) const {
	return key.fileId ^ key.offset;
      }
    };

    struct eqCacheKey {
      bool operator()(const CacheKeyT &k1, const CacheKeyT &k2) const {
	return k1.fileId == k2.fileId && k1.offset == k2.offset;
      }
    };

    typedef __gnu_cxx::hash_map<CacheKeyT, CacheValueT *, hashCacheKey, eqCacheKey> BlockMapT;

    boost::mutex  m_mutex;
    BlockMapT     m_block_map;
    CacheValueT  *m_head;
    CacheValueT  *m_tail;
  };

}


#endif // HYPERTABLE_FILEBLOCKCACHE_H
