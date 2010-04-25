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

#ifndef HYPERTABLE_CELLSTORESCANNERINTERVALREADAHEAD_H
#define HYPERTABLE_CELLSTORESCANNERINTERVALREADAHEAD_H

#include "Common/DynamicBuffer.h"

#include "CellStore.h"
#include "CellStoreScannerInterval.h"
#include "ScanContext.h"

namespace Hypertable {

  class BlockCompressionCodec;
  class CellStore;

  template <typename IndexT>
  class CellStoreScannerIntervalReadahead : public CellStoreScannerInterval {
  public:

    typedef typename IndexT::iterator IndexIteratorT;

    CellStoreScannerIntervalReadahead(CellStore *cellstore, IndexT *index,
                                      SerializedKey start_key,SerializedKey end_key, ScanContextPtr &scan_ctx);
    virtual ~CellStoreScannerIntervalReadahead();
    virtual void forward();
    virtual bool get(Key &key, ByteString &value);

  private:

    bool fetch_next_block_readahead(bool eob=false);

    CellStorePtr           m_cellstore;
    BlockInfo              m_block;
    Key                    m_key;
    SerializedKey          m_end_key;
    ByteString             m_cur_value;
    BlockCompressionCodec *m_zcodec;
    KeyDecompressor       *m_key_decompressor;
    int32_t                m_fd;
    int64_t                m_offset;
    int64_t                m_end_offset;
    bool                   m_check_for_range_end;
    bool                   m_eos;
    ScanContextPtr         m_scan_ctx;

  };

}

#endif // HYPERTABLE_CELLSTORESCANNERINTERVALREADAHEAD_H
