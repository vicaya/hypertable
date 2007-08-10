/**
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

  if (mHead == 0) {
    cacheValue->next = cacheValue->prev = 0;
    mHead = mTail = cacheValue;
  }
  else {
    mHead->next = cacheValue;
    cacheValue->prev = mHead;
    cacheValue->next = 0;
    mHead = cacheValue;
  }

  mBlockMap[cacheValue->key] = cacheValue;
  return true;
}


void FileBlockCache::MoveToHead(CacheValueT *cacheValue) {

  if (mHead == cacheValue)
    return;

  cacheValue->next->prev = cacheValue->prev;
  if (cacheValue->prev == 0)
    mTail = cacheValue->next;
  else
    cacheValue->prev->next = cacheValue->next;

  cacheValue->next = 0;
  mHead->next = cacheValue;
  cacheValue->prev = mHead;
  mHead = cacheValue;
}

