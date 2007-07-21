/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */


#include <cassert>
using namespace std;

#define DISABLE_LOG_DEBUG 1

#include "Common/Logger.h"

#include "RequestCache.h"
using namespace hypertable;

void RequestCache::Insert(uint32_t id, IOHandler *handler, CallbackHandler *cb, time_t expire) {
  boost::mutex::scoped_lock lock(mMutex);

  CacheNodeT *node = new CacheNodeT;

  LOG_VA_DEBUG("Adding id %d", id);

  node->id = id;
  node->handler = handler;
  node->cb = cb;
  node->expire = expire;

  if (mHead == 0) {
    node->next = node->prev = 0;
    mHead = mTail = node;
  }
  else {
    node->next = mTail;
    node->next->prev = node;
    node->prev = 0;
    mTail = node;
  }

  mIdMap[id] = node;
}


CallbackHandler *RequestCache::Remove(uint32_t id) {
  boost::mutex::scoped_lock lock(mMutex);

  LOG_VA_DEBUG("Removing id %d", id);

  IdHandlerMapT::iterator iter = mIdMap.find(id);

  if (iter == mIdMap.end()) {
    LOG_VA_DEBUG("ID %d not found in request cache", id);
    return 0;
  }

  CacheNodeT *node = (*iter).second;

  if (node->prev == 0)
    mTail = node->next;
  else
    node->prev->next = node->next;

  if (node->next == 0)
    mHead = node->prev;
  else
    node->next->prev = node->prev;

  mIdMap.erase(iter);

  if (node->handler != 0) {
    CallbackHandler *cb = node->cb;
    delete node;
    return cb;
  }
  delete node;
  return 0;
}



CallbackHandler *RequestCache::GetNextTimeout(time_t now, IOHandler *&handlerp)  {
  boost::mutex::scoped_lock lock(mMutex);

  while (mHead && mHead->expire < now) {
    IdHandlerMapT::iterator iter = mIdMap.find(mHead->id);
    assert (iter != mIdMap.end());
    CacheNodeT *node = mHead;
    if (mHead->prev) {
      mHead = mHead->prev;
      mHead->next = 0;
    }
    else
      mHead = mTail = 0;

    mIdMap.erase(iter);

    if (node->handler != 0) {
      handlerp = node->handler;
      CallbackHandler *cb = node->cb;
      delete node;
      return cb;
    }
    delete node;
  }
  return 0;
}



void RequestCache::PurgeRequests(IOHandler *handler) {
  boost::mutex::scoped_lock lock(mMutex);
  for (CacheNodeT *node = mTail; node != 0; node = node->next) {
    if (node->handler == handler) {
      LOG_VA_DEBUG("Purging request id %d", node->id);
      node->handler = 0;  // mark for deletion
    }
  }
}

