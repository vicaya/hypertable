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

#ifndef HYPERTABLE_MERGESCANNER_H
#define HYPERTABLE_MERGESCANNER_H

#include <queue>
#include <string>
#include <vector>

#include "Common/ByteString.h"
#include "Common/DynamicBuffer.h"

#include "CellListScanner.h"

using namespace hypertable;

namespace hypertable {

  class MergeScanner : public CellListScanner {
  public:

    typedef struct {
      CellListScanner *scanner;
      ByteString32T   *key;
      ByteString32T   *value;
    } ScannerStateT;

    struct ltScannerState {
      bool operator()(const ScannerStateT &ss1, const ScannerStateT &ss2) const {
	return !(*ss1.key < *ss2.key);
      }
    };

    MergeScanner(ScanContextPtr &scanContextPtr, bool returnDeletes=true);
    virtual ~MergeScanner();
    virtual void Forward();
    virtual bool Get(ByteString32T **keyp, ByteString32T **valuep);
    void AddScanner(CellListScanner *scanner);

  private:

    void Initialize();

    bool          mDone;
    bool          mInitialized;
    std::vector<CellListScanner *>  mScanners;
    std::priority_queue<ScannerStateT, std::vector<ScannerStateT>, ltScannerState> mQueue;
    bool          mDeletePresent;
    DynamicBuffer mDeletedRow;
    uint64_t      mDeletedRowTimestamp;
    DynamicBuffer mDeletedCell;
    uint64_t      mDeletedCellTimestamp;
    bool          mReturnDeletes;
    uint32_t      mRowCount;
    uint32_t      mRowLimit;
    uint32_t      mCellCount;
    uint32_t      mCellLimit;
    uint64_t      mCellCutoff;
    DynamicBuffer mPrevKey;
  };
}

#endif // HYPERTABLE_MERGESCANNER_H

