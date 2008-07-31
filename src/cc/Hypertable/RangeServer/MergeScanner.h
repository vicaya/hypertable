/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#ifndef HYPERTABLE_MERGESCANNER_H
#define HYPERTABLE_MERGESCANNER_H

#include <queue>
#include <string>
#include <vector>

#include "Common/ByteString.h"
#include "Common/DynamicBuffer.h"

#include "CellListScanner.h"
#include "CellStoreReleaseCallback.h"


namespace Hypertable {

  class MergeScanner : public CellListScanner {
  public:

    struct ScannerState {
      CellListScanner *scanner;
      ByteString key;
      ByteString value;
    };

    struct LtScannerState {
      bool operator()(const ScannerState &ss1, const ScannerState &ss2) const {
        return !(ss1.key < ss2.key);
      }
    };

    MergeScanner(ScanContextPtr &scan_ctx, bool return_dels=true);
    virtual ~MergeScanner();
    virtual void forward();
    virtual bool get(ByteString &key, ByteString &value);
    void add_scanner(CellListScanner *scanner);

    void install_release_callback(CellStoreReleaseCallback &cb) {
      m_release_callback = cb;
    }

  private:

    void initialize();

    bool          m_done;
    bool          m_initialized;
    std::vector<CellListScanner *>  m_scanners;
    std::priority_queue<ScannerState, std::vector<ScannerState>, LtScannerState> m_queue;
    bool          m_delete_present;
    DynamicBuffer m_deleted_row;
    int64_t       m_deleted_row_timestamp;
    DynamicBuffer m_deleted_column_family;
    int64_t       m_deleted_column_family_timestamp;
    DynamicBuffer m_deleted_cell;
    int64_t       m_deleted_cell_timestamp;
    bool          m_return_deletes;
    int32_t       m_row_count;
    int32_t       m_row_limit;
    uint32_t      m_cell_count;
    uint32_t      m_cell_limit;
    uint64_t      m_cell_cutoff;
    int64_t       m_start_timestamp;
    int64_t       m_end_timestamp;
    DynamicBuffer m_prev_key;
    CellStoreReleaseCallback m_release_callback;
  };
}

#endif // HYPERTABLE_MERGESCANNER_H

