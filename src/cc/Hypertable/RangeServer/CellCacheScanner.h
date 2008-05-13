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

#ifndef HYPERTABLE_CELLCACHESCANNER_H
#define HYPERTABLE_CELLCACHESCANNER_H

#include "CellCache.h"
#include "CellListScanner.h"
#include "ScanContext.h"

using namespace Hypertable;

namespace Hypertable {

  /**
   * Provides a scanning interface to a CellCache.
   */
  class CellCacheScanner : public CellListScanner {
  public:
    CellCacheScanner(CellCachePtr &cellCachePtr, ScanContextPtr &scanContextPtr);
    virtual ~CellCacheScanner() { return; }
    virtual void forward();
    virtual bool get(ByteString &key, ByteString &value);

  private:
    CellCache::CellMapT::iterator  m_start_iter;
    CellCache::CellMapT::iterator  m_end_iter;
    CellCache::CellMapT::iterator  m_cur_iter;
    CellCachePtr                   m_cell_cache_ptr;
    boost::mutex                  &m_cell_cache_mutex;
    ByteString                     m_cur_key;
    ByteString                     m_cur_value;
    bool                           m_eos;

  };
}

#endif // HYPERTABLE_CELLCACHESCANNER_H

