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
#ifndef HYPERTABLE_CELLLIST_H
#define HYPERTABLE_CELLLIST_H

#include "Common/atomic.h"
#include "Common/ByteString.h"
#include "Common/ReferenceCount.h"

#include "ScanContext.h"

namespace Hypertable {

  class CellList;
  class CellListScanner;

  /**
   * Abstract base class for all Cell list classes.
   */
  class CellList : public ReferenceCount {
  public:
    virtual ~CellList() { return; }
    virtual int add(const ByteString32T *key, const ByteString32T *value) = 0;
    virtual CellListScanner *create_scanner(ScanContextPtr &scanContextPtr) { return 0; }
    virtual const char *get_split_row() = 0;
    virtual const char *get_start_row() { return m_start_row.c_str(); }
    virtual const char *get_end_row() { return m_end_row.c_str(); }
  protected:
    std::string m_start_row;
    std::string m_end_row;
  };
  typedef boost::intrusive_ptr<CellList> CellListPtr;

}

#endif // HYPERTABLE_CELLLIST_H
