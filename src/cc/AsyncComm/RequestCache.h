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


#ifndef HYPERTABLE_REQUESTCACHE_H
#define HYPERTABLE_REQUESTCACHE_H

#include <boost/thread/mutex.hpp>

#include <ext/hash_map>

#include "CallbackHandler.h"

namespace hypertable {

  class IOHandler;

  class RequestCache {

    typedef struct cache_node {
      struct cache_node *prev, *next;
      time_t             expire;
      uint32_t           id;
      IOHandler         *handler;
      CallbackHandler   *cb;
    } CacheNodeT;

    typedef __gnu_cxx::hash_map<uint32_t, CacheNodeT *> IdHandlerMapT;

  public:
    RequestCache() : mIdMap(), mHead(0), mTail(0), mMutex() { return; }

    void Insert(uint32_t id, IOHandler *handler, CallbackHandler *cb, time_t expire);

    CallbackHandler *Remove(uint32_t id);

    CallbackHandler *GetNextTimeout(time_t now, IOHandler *&handlerp);

    void PurgeRequests(IOHandler *handler);

  private:
    IdHandlerMapT  mIdMap;
    CacheNodeT    *mHead, *mTail;
    boost::mutex   mMutex;
  };
}

#endif // HYPERTABLE_REQUESTCACHE_H
