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
#include <cassert>

#include "FileBlockCache.h"

using namespace hypertable;

atomic_t FileBlockCache::msNextFileId = ATOMIC_INIT(0);


bool FileBlockCache::Checkout(int fileId, uint32_t offset, uint8_t **blockp, uint32_t *lengthp) {
  boost::mutex::scoped_lock lock(mMutex);
  CacheKeyT key;
  BlockMapT::iterator iter;

  key.fileId = fileId;
  key.offset = offset;

  if ((iter = mBlockMap.find(key)) == mBlockMap.end())
    return false;

  MoveToHead((*iter).second);

  (*iter).second->refCount++;

  *blockp = (*iter).second->block;
  *lengthp = (*iter).second->length;
  return true;
}


void FileBlockCache::Checkin(int fileId, uint32_t offset) {
  boost::mutex::scoped_lock lock(mMutex);
  CacheKeyT key;
  BlockMapT::iterator iter;

  key.fileId = fileId;
  key.offset = offset;

  iter = mBlockMap.find(key);

  assert(iter != mBlockMap.end() && (*iter).second->refCount > 0);

  (*iter).second->refCount--;
}


bool FileBlockCache::InsertAndCheckout(int fileId, uint32_t offset, uint8_t *block, uint32_t length) {
  boost::mutex::scoped_lock lock(mMutex);
  CacheValueT *cacheValue = new CacheValueT;
  BlockMapT::iterator iter;

  cacheValue->key.fileId = fileId;
  cacheValue->key.offset = offset;

  if ((iter = mBlockMap.find(cacheValue->key)) != mBlockMap.end()) {
    MoveToHead((*iter).second);
    delete cacheValue;
    return false;
  }

  cacheValue->block = block;
  cacheValue->length = length;
  cacheValue->refCount = 1;

  if (mHead == 0)
    mHead = mTail = cacheValue;
  else {
    cacheValue->prev = mHead;
    cacheValue->next = 0;
    mHead = cacheValue;
  }

  mBlockMap[cacheValue->key] = cacheValue;
  return true;
}


void FileBlockCache::MoveToHead(CacheValueT *cacheValue) {

  if (mHead == mTail)
    return;

  if (cacheValue->next == 0) {
    mHead = cacheValue->prev;
    if (cacheValue->prev != 0)
      cacheValue->prev->next = 0;
    else
      mTail = 0;
  }
  else {
    cacheValue->next->prev = cacheValue->prev;
    if (cacheValue->prev != 0) {
      cacheValue->prev->next = cacheValue->next;
    }
    else
      mTail = cacheValue->next;
  }

  cacheValue->next = 0;
  cacheValue->prev = mHead;
  mHead = cacheValue;

}

