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

#include "CellCacheScanner.h"

#include "Common/Logger.h"

#include "CellCache.h"

#include <iostream>

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
  CellMapT::iterator iter = mCellMap->lower_bound(keyPtr);

  mCellMap->insert(iter, CellMapT::value_type(keyPtr, ByteString32Ptr(newValue)));

  mMemoryUsed += addedMem;

  return 0;
}



/**
 *
 */
CellListScanner *CellCache::CreateScanner(bool suppressDeleted) {
  return new CellCacheScanner(this);
}



/**
 *
 */
void CellCache::Purge(uint64_t timestamp) {
  CellMapT *newMap = new CellMapT;
  KeyComponentsT keyComps;

  mMemoryUsed = 0;

  for (CellMapT::iterator iter = mCellMap->begin(); iter != mCellMap->end(); iter++) {

    if (!Load((KeyT *)(*iter).first.get(), keyComps)) {
      LOG_ERROR("Problem deserializing key/value pair");
      continue;
    }

    if (keyComps.timestamp > timestamp) {
      mMemoryUsed += sizeof(CellMapT::value_type);
      mMemoryUsed += Length((KeyT *)(*iter).first.get());
      mMemoryUsed += Length((ByteString32T *)(*iter).second.get());
      newMap->insert(CellMapT::value_type((*iter).first, (*iter).second));
    }

  }

  delete mCellMap;
  mCellMap = newMap;
}
