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
#ifndef HYPERTABLE_SCANNERMAP_H
#define HYPERTABLE_SCANNERMAP_H

#include <boost/thread/mutex.hpp>

#include <ext/hash_map>

#include "Common/atomic.h"

#include "CellListScanner.h"
#include "Range.h"

namespace hypertable {

  class ScannerMap {

  public:
    ScannerMap() : mMutex() { return; }
    uint32_t Put(CellListScannerPtr &scannerPtr, RangePtr &rangePtr);
    bool Get(uint32_t id, CellListScannerPtr &scannerPtr, RangePtr &rangePtr);
    bool Remove(uint32_t id);

  private:
    static atomic_t msNextId;

    boost::mutex   mMutex;
    typedef struct {
      CellListScannerPtr scannerPtr;
      RangePtr rangePtr;
    } ScanInfoT;
    typedef __gnu_cxx::hash_map<uint32_t, ScanInfoT> CellListScannerMapT;

    CellListScannerMapT mScannerMap;

  };

}


#endif // HYPERTABLE_SCANNERMAP_H
