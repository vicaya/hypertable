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
#ifndef HYPERTABLE_CELLSTORESCANNERVERSION1_H
#define HYPERTABLE_CELLSTORESCANNERVERSION1_H

#include "Common/DynamicBuffer.h"

#include "CellStoreV0.h"
#include "CellListScanner.h"

namespace Hypertable {

  class BlockCompressionCodec;
  class CellStore;

  class CellStoreScannerV0 : public CellListScanner {
  public:
    CellStoreScannerV0(CellStorePtr &cellStorePtr, ScanContextPtr &scanContextPtr);
    virtual ~CellStoreScannerV0();
    virtual void forward();
    virtual bool get(ByteString32T **keyp, ByteString32T **valuep);

  private:

    typedef struct {
      uint32_t offset;
      uint32_t zlength;
      uint8_t *base;
      uint8_t *ptr;
      uint8_t *end;
    } BlockInfoT;

    bool fetch_next_block();
    bool fetch_next_block_readahead();
    bool initialize();

    CellStorePtr            m_cell_store_ptr;
    CellStoreV0            *m_cell_store_v0;
    CellStoreV0::IndexMapT &m_index;

    CellStoreV0::IndexMapT::iterator m_iter;
    CellStoreV0::IndexMapT::iterator m_end_iter;

    BlockInfoT            m_block;
    ByteString32T        *m_cur_key;
    ByteString32T        *m_cur_value;
    BlockCompressionCodec *m_zcodec;
    bool                  m_check_for_range_end;
    bool                  m_end_inclusive;
    int                   m_file_id;
    std::string           m_start_row;
    std::string           m_end_row;
    bool                  m_readahead;
    int32_t               m_fd;
    uint32_t              m_start_offset;
    uint32_t              m_end_offset;
    uint32_t              m_returned;
  };

}

#endif // HYPERTABLE_CELLSTORESCANNERVERSION1_H

