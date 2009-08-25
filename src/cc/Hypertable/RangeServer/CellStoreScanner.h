/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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

#ifndef HYPERTABLE_CELLSTORESCANNER_H
#define HYPERTABLE_CELLSTORESCANNER_H

#include "Common/DynamicBuffer.h"

#include "CellStore.h"
#include "CellListScanner.h"
#include "CellStoreScannerInterval.h"

namespace Hypertable {

  class CellStore;

  template <typename IndexT>
  class CellStoreScanner : public CellListScanner {
  public:

    CellStoreScanner(CellStore *cellstore, ScanContextPtr &scan_ctx,
                     IndexT *indexp=0);
    virtual ~CellStoreScanner();
    virtual void forward();
    virtual bool get(Key &key, ByteString &value);

  private:
    CellStorePtr              m_cellstore;
    CellStoreScannerInterval *m_interval_scanners[3];
    size_t                    m_interval_index;
    size_t                    m_interval_max;
    DynamicBuffer             m_key_buf;
    bool                      m_keys_only;
    bool                      m_eos;
  };

}

#endif // HYPERTABLE_CELLSTORESCANNER_H

