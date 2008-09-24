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

#ifndef HYPERTABLE_CELLLIST_H
#define HYPERTABLE_CELLLIST_H

#include "Common/atomic.h"
#include "Common/ByteString.h"
#include "Common/ReferenceCount.h"

#include "ScanContext.h"

#include "Hypertable/Lib/Key.h"

namespace Hypertable {

  class CellList;
  class CellListScanner;

  /**
   * Abstract base class for cell lists (sorted lists of key/value
   * pairs).  Cell lists include cell stores and cell caches.
   */
  class CellList : public ReferenceCount {
  public:
    virtual ~CellList() { return; }

    /**
     * Inserts a key/value pair into the cell list.
     *
     * @param key key object
     * @param value ByteString representing value
     * @return Error::OK on success, error code on failure
     */
    virtual int add(const Key &key, const ByteString value) = 0;

    /**
     * Creates a scanner on this cell list.
     *
     * @param scan_ctx smart pointer to scan context
     * @return pointer to newly allocated scanner
     */
    virtual CellListScanner *create_scanner(ScanContextPtr &scan_ctx) { return 0; }

    /**
     * Returns a split row for this cell list.  This should be an approximation
     * of the middle row key in the list.
     *
     * @return the split row key
     */
    virtual const char *get_split_row() = 0;

    /**
     * Returns the start row of this cell list.  This value is used to restrict
     * the start range of the cell list to values that are greater than this
     * row key.  It is used in cell stores to allow them to be shared after
     * a split.
     *
     * @return the row key of the start row (not inclusive)
     */
    virtual const char *get_start_row() { return m_start_row.c_str(); }

    /**
     * Returns the end row of this cell list.  This value is used to restrict
     * the end range of the cell list to values that are less than or equal to
     * this row key.  It is used in cell stores to allow them to be shared after
     * a split.
     *
     * @return the row key of the end row (inclusive)
     */
    virtual const char *get_end_row() { return m_end_row.c_str(); }

  protected:
    std::string m_start_row;
    std::string m_end_row;
  };
  typedef boost::intrusive_ptr<CellList> CellListPtr;

}

#endif // HYPERTABLE_CELLLIST_H
