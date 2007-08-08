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
#include <iostream>

#include "Common/Logger.h"

#include "CellCache.h"
#include "CellCacheScanner.h"

using namespace hypertable;
using namespace std;


/**
 * 
 */
int CellCache::Add(const KeyT *key, const ByteString32T *value) {
  size_t addedMem = sizeof(CellMapT::value_type);
  KeyT *newKey;
  ByteString32T *newValue;

  newKey = CreateCopy(key);
  addedMem += Length(key);

  newValue = CreateCopy(value);
  addedMem += Length(value);

  KeyPtr keyPtr(newKey);
  CellMapT::iterator iter = mCellMap.lower_bound(keyPtr);

  mCellMap.insert(iter, CellMapT::value_type(keyPtr, ByteString32Ptr(newValue)));

  mMemoryUsed += addedMem;

  return 0;
}



CellCache *CellCache::SliceCopy(uint64_t timestamp) {
  boost::mutex::scoped_lock lock(mMutex);
  CellCache *cache = new CellCache();
  KeyComponentsT keyComps;
  KeyT *key;
  ByteString32T *value;


  for (CellMapT::iterator iter = mCellMap.begin(); iter != mCellMap.end(); iter++) {

    if (!Load((KeyT *)(*iter).first.get(), keyComps)) {
      LOG_ERROR("Problem deserializing key/value pair");
      continue;
    }

    if (keyComps.timestamp > timestamp)
      cache->Add((*iter).first.get(), (*iter).second.get());

  }

  return cache;
}

