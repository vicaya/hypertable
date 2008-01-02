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

using namespace Hypertable;

namespace Hypertable {

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
    virtual void forward();
    virtual bool get(ByteString32T **keyp, ByteString32T **valuep);
    void add_scanner(CellListScanner *scanner);

  private:

    void initialize();

    bool          m_done;
    bool          m_initialized;
    std::vector<CellListScanner *>  m_scanners;
    std::priority_queue<ScannerStateT, std::vector<ScannerStateT>, ltScannerState> m_queue;
    bool          m_delete_present;
    DynamicBuffer m_deleted_row;
    uint64_t      m_deleted_row_timestamp;
    DynamicBuffer m_deleted_column_family;
    uint64_t      m_deleted_column_family_timestamp;
    DynamicBuffer m_deleted_cell;
    uint64_t      m_deleted_cell_timestamp;
    bool          m_return_deletes;
    uint32_t      m_row_count;
    uint32_t      m_row_limit;
    uint32_t      m_cell_count;
    uint32_t      m_cell_limit;
    uint64_t      m_cell_cutoff;
    uint64_t      m_start_timestamp;
    uint64_t      m_end_timestamp;
    DynamicBuffer m_prev_key;
  };
}

#endif // HYPERTABLE_MERGESCANNER_H

