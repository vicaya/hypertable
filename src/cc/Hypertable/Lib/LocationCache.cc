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

#include "LocationCache.h"

using namespace hypertable;

/**
 * Insert
 */
void LocationCache::Insert(const char *tableName, const char *startRow, const char *endRow, struct sockaddr_in &addr) {
  boost::mutex::scoped_lock lock(mMutex);
  ValueT *newValue = new ValueT;
  ValueT *oldValue;
  InfoPtr infoPtr( new Info() );
  LocationMapT::iterator iter;
  LocationCacheKeyT key;

  // initialize Info object
  infoPtr->tableName = new char [ strlen(tableName) + 1 ];
  strcpy((char *)infoPtr->tableName, tableName);
  infoPtr->startRow = new char [ strlen(startRow) + 1 ];
  strcpy((char *)infoPtr->startRow, startRow);
  infoPtr->endRow = new char [ strlen(endRow) + 1 ];
  strcpy((char *)infoPtr->endRow, endRow);
  memcpy(&infoPtr->addr, &addr, sizeof(addr));

  key.tableName = infoPtr->tableName;
  key.endRow = infoPtr->endRow;

  // remove old entry
  if ((iter = mLocationMap.find(key)) != mLocationMap.end())
    Remove((*iter).second);

  // make room for the new entry
  while (mLocationMap.size() >= mMaxEntries)
    Remove(mTail);

  newValue->infoPtr = infoPtr;

  // add to head
  if (mHead == 0) {
    newValue->next = newValue->prev = 0;
    mHead = mTail = newValue;
  }
  else {
    mHead->next = newValue;
    newValue->prev = mHead;
    newValue->next = 0;
    mHead = newValue;
  }

  // Insert the new entry into the map, recording an iterator to the entry in the map
  {
    std::pair<LocationMapT::iterator, bool> old_entry;
    LocationMapT::value_type map_value(key, newValue);
    old_entry = mLocationMap.insert(map_value);
    assert(old_entry.second);
    newValue->mapIter = old_entry.first;
  }
  
}


/**
 * Lookup
 */
bool LocationCache::Lookup(const char *tableName, const char *rowKey, InfoPtr &infoPtr) {
  boost::mutex::scoped_lock lock(mMutex);
  LocationMapT::iterator iter;
  LocationCacheKeyT key;

  key.tableName = tableName;
  key.endRow = rowKey;

  if ((iter = mLocationMap.lower_bound(key)) == mLocationMap.end())
    return false;

  if (strcmp((*iter).first.tableName, tableName))
    return false;

  if (strcmp(rowKey, (*iter).second->infoPtr->startRow) <= 0)
    return false;

  MoveToHead((*iter).second);

  infoPtr = (*iter).second->infoPtr;

  return true;
}


/**
 * MoveToHead
 */
void LocationCache::MoveToHead(ValueT *cacheValue) {

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


/**
 * Remove
 */
void LocationCache::Remove(ValueT *cacheValue) {
  if (mTail == cacheValue) {
    mTail = cacheValue->next;
    mTail->prev = 0;
  }
  if (mHead = cacheValue)
    mHead = 0;
  mLocationMap.erase(cacheValue->mapIter);  
  delete cacheValue;
}


