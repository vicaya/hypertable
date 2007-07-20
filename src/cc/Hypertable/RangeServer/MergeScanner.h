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

#ifndef HYPERTABLE_MERGESCANNER_H
#define HYPERTABLE_MERGESCANNER_H

#include <queue>
#include <string>
#include <vector>

#include "Common/DynamicBuffer.h"

#include "CellListScanner.h"
#include "Key.h"

using namespace hypertable;

namespace hypertable {

  class MergeScanner : public CellListScanner {
  public:

    typedef struct {
      CellListScanner *scanner;
      KeyT            *key;
      ByteString32T   *value;
    } ScannerStateT;

    struct ltScannerState {
      bool operator()(const ScannerStateT &ss1, const ScannerStateT &ss2) const {
	return !(*ss1.key < *ss2.key);
      }
    };

    MergeScanner(bool suppressDeleted);
    virtual ~MergeScanner();
    virtual void RestrictRange(KeyT *start, KeyT *end);
    virtual void RestrictColumns(const uint8_t *families, size_t count);
    virtual void Forward();
    virtual bool Get(KeyT **keyp, ByteString32T **valuep);
    virtual void Reset();
    void AddScanner(CellListScanner *scanner);

  private:
    std::vector<CellListScanner *>  mScanners;
    std::priority_queue<ScannerStateT, std::vector<ScannerStateT>, ltScannerState> mQueue;
    bool          mDeletePresent;
    DynamicBuffer mDeletedRow;
    uint64_t      mDeletedRowTimestamp;
    DynamicBuffer mDeletedCell;
    uint64_t      mDeletedCellTimestamp;
    bool          mSuppressDeleted;
  };
}

#endif // HYPERTABLE_MERGESCANNER_H

