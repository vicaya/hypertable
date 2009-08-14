/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERTABLE_CELLSTORESCANNERINTERVALBLOCKINDEX_H
#define HYPERTABLE_CELLSTORESCANNERINTERVALBLOCKINDEX_H

#include "Common/DynamicBuffer.h"

#include "CellStore.h"
#include "CellStoreScannerInterval.h"
#include "ScanContext.h"

namespace Hypertable {

  class BlockCompressionCodec;
  class CellStore;

  template <typename IndexT>
  class CellStoreScannerIntervalBlockIndex : public CellStoreScannerInterval {
  public:

    typedef typename IndexT::iterator IndexIteratorT;

    CellStoreScannerIntervalBlockIndex(CellStore *cellstore, IndexT *index,
				       SerializedKey start_key, SerializedKey end_key, ScanContextPtr &scan_ctx);
    virtual ~CellStoreScannerIntervalBlockIndex();
    virtual void forward();
    virtual bool get(Key &key, ByteString &value);

  private:

    bool fetch_next_block();

    CellStorePtr          m_cellstore;
    IndexT               *m_index;
    IndexIteratorT        m_iter;
    BlockInfo             m_block;
    Key                   m_key;
    SerializedKey         m_cur_key;
    ByteString            m_cur_value;
    SerializedKey         m_start_key;
    SerializedKey         m_end_key;
    const char *          m_end_row;
    DynamicBuffer         m_key_buf;
    BlockCompressionCodec *m_zcodec;
    int32_t               m_fd;
    bool                  m_check_for_range_end;
    int                   m_file_id;
    ScanContextPtr         m_scan_ctx;

  };

}

#endif // HYPERTABLE_CELLSTORESCANNERINTERVALBLOCKINDEX_H
