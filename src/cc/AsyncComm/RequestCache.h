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


#ifndef HYPERTABLE_REQUESTCACHE_H
#define HYPERTABLE_REQUESTCACHE_H

#include <boost/thread/mutex.hpp>

#include <ext/hash_map>

#include "DispatchHandler.h"

namespace hypertable {

  class IOHandler;

  class RequestCache {

    typedef struct cache_node {
      struct cache_node *prev, *next;
      time_t             expire;
      uint32_t           id;
      IOHandler         *handler;
      DispatchHandler   *dh;
    } CacheNodeT;

    typedef __gnu_cxx::hash_map<uint32_t, CacheNodeT *> IdHandlerMapT;

  public:
    RequestCache() : mIdMap(), mHead(0), mTail(0), mMutex() { return; }

    void Insert(uint32_t id, IOHandler *handler, DispatchHandler *dh, time_t expire);

    DispatchHandler *Remove(uint32_t id);

    DispatchHandler *GetNextTimeout(time_t now, IOHandler *&handlerp);

    void PurgeRequests(IOHandler *handler);

  private:
    IdHandlerMapT  mIdMap;
    CacheNodeT    *mHead, *mTail;
    boost::mutex   mMutex;
  };
}

#endif // HYPERTABLE_REQUESTCACHE_H
