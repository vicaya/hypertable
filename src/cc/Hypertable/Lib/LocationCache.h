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

#ifndef HYPERTABLE_LOCATIONCACHE_H
#define HYPERTABLE_LOCATIONCACHE_H

#include <cstring>
#include <map>

#include <boost/thread/mutex.hpp>

extern "C" {
#include <netinet/in.h>
}

#include "Common/ReferenceCount.h"

namespace hypertable {

  /**
   * Key type for Range location cache
   */
  typedef struct {
    const char *tableName;
    const char *endRow;
  } LocationCacheKeyT;

  /**
   * Less than operator for LocationCacheKeyT
   */
  inline bool operator<(const LocationCacheKeyT &k1, const LocationCacheKeyT &k2) {
    if (strcmp(k1.tableName, k2.tableName))
      return strcmp(k1.tableName, k2.tableName) < 0;
    return strcmp(k1.endRow, k2.endRow) < 0;
  }

  /**
   * Equality operator for LocationCacheKeyT
   */
  inline bool operator==(const LocationCacheKeyT &k1, const LocationCacheKeyT &k2) {
    if (!strcmp(k1.tableName, k2.tableName))
      return !strcmp(k1.endRow, k2.endRow);
    return !strcmp(k1.tableName, k2.tableName);
  }

  /**
   * Equality operator for LocationCacheKeyT
   */
  inline bool operator!=(const LocationCacheKeyT &k1, const LocationCacheKeyT &k2) {
    return !(k1 == k2);
  }


  /**
   *  This class acts as a cache of Range location information.  It 
   */
  class LocationCache {

  public:

    /**
     * This class encapsulates the Range and location information for a cache entry.
     * It is derived from ReferenceCount so that we get cheap shared pointers.
     * intrusive_ptr to this type is what is returned in Lookup method
     */
    class Info : public ReferenceCount {
    public:
      virtual ~Info() {
	delete [] tableName;
	delete [] startRow;
	delete [] endRow;
      }
      const char *tableName;
      const char *startRow;
      const char *endRow;
      struct sockaddr_in addr;
    };

    /**
     * Shared (intrusive) pointer to
     */
    typedef boost::intrusive_ptr<Info> InfoPtr;

    /**
     * 
     */
    typedef struct cache_value {
      struct cache_value *prev;
      struct cache_value *next;
      std::map<LocationCacheKeyT, struct cache_value *>::iterator mapIter;
      InfoPtr infoPtr;
    } ValueT;

    LocationCache(uint32_t maxEntries) : mMutex(), mLocationMap(), mHead(0), mTail(0), mMaxEntries(maxEntries) { return; }
    void Insert(const char *tableName, const char *startRow, const char *endRow, struct sockaddr_in &addr);
    bool Lookup(const char *tableName, const char *rowKey, InfoPtr &infoPtr);

  private:

    void MoveToHead(ValueT *cacheValue);
    void Remove(ValueT *cacheValue);

    typedef std::map<LocationCacheKeyT, ValueT *> LocationMapT;

    boost::mutex   mMutex;
    LocationMapT   mLocationMap;
    ValueT        *mHead;
    ValueT        *mTail;
    uint32_t       mMaxEntries;
  };

}


#endif // HYPERTABLE_LOCATIONCACHE_H
