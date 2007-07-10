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
#ifndef HYPERTABLE_CELLLIST_H
#define HYPERTABLE_CELLLIST_H

#include "CellListScanner.h"

namespace hypertable {

  class CellListScanner;

  class CellList {
  public:
    virtual ~CellList() { return; }
    virtual int Add(const KeyT *key, const ByteString32T *value) = 0;
    virtual CellListScanner *CreateScanner(bool suppressDeleted=false) = 0;
    virtual void Lock() { return; }
    virtual void Unlock() { return; }
    virtual void LockShareable() { return; }
    virtual void UnlockShareable() { return; }
  };

}

#endif // HYPERTABLE_CELLLIST_H
