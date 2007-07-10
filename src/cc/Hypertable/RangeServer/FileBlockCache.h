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
#ifndef HYPERTABLE_FILEBLOCKCACHE_H
#define HYPERTABLE_FILEBLOCKCACHE_H

#include <boost/thread/mutex.hpp>

#include <ext/hash_map>

#include "Common/atomic.h"

namespace hypertable {

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

    static atomic_t msNextFileId;

  public:

    FileBlockCache(uint64_t maxMemory) : mMutex(), mBlockMap(), mHead(0), mTail(0) { return; }
    bool Checkout(int fileId, uint32_t offset, uint8_t **blockp, uint32_t *lengthp);
    void Checkin(int fileId, uint32_t offset);
    bool InsertAndCheckout(int fileId, uint32_t offset, uint8_t *block, uint32_t length);

    static int GetNextFileId() {
      return atomic_inc_return(&msNextFileId);
    }

  private:

    void MoveToHead(CacheValueT *cacheValue);

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

    boost::mutex  mMutex;
    BlockMapT     mBlockMap;
    CacheValueT  *mHead;
    CacheValueT  *mTail;
  };

}


#endif // HYPERTABLE_FILEBLOCKCACHE_H
