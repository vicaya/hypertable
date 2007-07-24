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

