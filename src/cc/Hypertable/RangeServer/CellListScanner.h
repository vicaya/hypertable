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
#ifndef HYPERTABLE_CELLLISTSCANNER_H
#define HYPERTABLE_CELLLISTSCANNER_H

#include <boost/shared_array.hpp>
#include <boost/shared_ptr.hpp>

#include "Common/ByteString.h"
#include "Common/DynamicBuffer.h"

using namespace hypertable;

namespace hypertable {

  class CellListScanner {
  public:
    CellListScanner();
    virtual ~CellListScanner() { return; }
    virtual void RestrictRange(ByteString32T *start, ByteString32T *end);
    virtual void RestrictColumns(const uint8_t *families, size_t count);
    virtual void Forward() = 0;
    virtual bool Get(ByteString32T **keyp, ByteString32T **valuep) = 0;
    virtual void Reset() = 0;

  protected:
    bool  mReset;
    ByteString32T  *mRangeStart;
    ByteString32T  *mRangeEnd;
    ByteString32Ptr mRangeStartPtr;
    ByteString32Ptr mRangeEndPtr;
    boost::shared_array<bool> mFamilyMask;
  };

  typedef boost::shared_ptr<CellListScanner> CellListScannerPtr;

}

#endif // HYPERTABLE_CELLLISTSCANNER_H

