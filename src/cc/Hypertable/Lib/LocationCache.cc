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
#include <cstring>

#include "LocationCache.h"

using namespace hypertable;

/**
 * Insert
 */
void LocationCache::Insert(uint32_t tableId, const char *startRow, const char *endRow, const char *serverId) {
  boost::mutex::scoped_lock lock(mMutex);
  ValueT *newValue = new ValueT;
  ValueT *oldValue;
  LocationMapT::iterator iter;
  LocationCacheKeyT key;

  serverId = GetConstantServerId(serverId);

  newValue->startRow = startRow;
  newValue->endRow   = endRow;
  newValue->serverId = serverId;

  key.tableId = tableId;
  key.endRow = newValue->endRow.c_str();

  // remove old entry
  if ((iter = mLocationMap.find(key)) != mLocationMap.end())
    Remove((*iter).second);

  // make room for the new entry
  while (mLocationMap.size() >= mMaxEntries)
    Remove(mTail);

  // add to head
  if (mHead == 0) {
    assert(mTail == 0);
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
 *
 */
LocationCache::~LocationCache() {
  for (std::set<const char *, lt_cstr>::iterator iter = mServerIdStrings.begin(); iter != mServerIdStrings.end(); iter++)
    delete *iter;
}


/**
 * Lookup
 */
bool LocationCache::Lookup(uint32_t tableId, const char *rowKey, const char **serverIdPtr) {
  boost::mutex::scoped_lock lock(mMutex);
  LocationMapT::iterator iter;
  LocationCacheKeyT key;

  key.tableId = tableId;
  key.endRow = rowKey;

  if ((iter = mLocationMap.lower_bound(key)) == mLocationMap.end())
    return false;

  if ((*iter).first.tableId != tableId)
    return false;

  if (strcmp(rowKey, (*iter).second->startRow.c_str()) <= 0)
    return false;

  MoveToHead((*iter).second);

  *serverIdPtr = (*iter).second->serverId;

  return true;
}


/**
 * MoveToHead
 */
void LocationCache::MoveToHead(ValueT *cacheValue) {

  if (mHead == cacheValue)
    return;

  // unstich entry from cache
  cacheValue->next->prev = cacheValue->prev;
  if (cacheValue->prev == 0)
    mTail = cacheValue->next;
  else
    cacheValue->prev->next = cacheValue->next;

  cacheValue->next = 0;
  cacheValue->prev = mHead;
  mHead->next = cacheValue;
  mHead = cacheValue;
}


/**
 * Remove
 */
void LocationCache::Remove(ValueT *cacheValue) {
  if (mTail == cacheValue) {
    mTail = cacheValue->next;
    if (mTail)
      mTail->prev = 0;
    else
      mHead = 0;
  }
  else if (mHead == cacheValue) {
    mHead = mHead->prev;
    if (mHead)
      mHead->next = 0;
  }
  mLocationMap.erase(cacheValue->mapIter);  
  delete cacheValue;
}



const char *LocationCache::GetConstantServerId(const char *serverId) {
  std::set<const char *, lt_cstr>::iterator iter = mServerIdStrings.find(serverId);

  if (iter != mServerIdStrings.end())
    return *iter;

  char *constantId = new char [ strlen(serverId) + 1 ];
  strcpy(constantId, serverId);
  mServerIdStrings.insert(constantId);
  return constantId;
}
