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

#ifndef HYPERTABLE_SCANNERMAP_H
#define HYPERTABLE_SCANNERMAP_H

#include <boost/thread/mutex.hpp>

extern "C" {
#include <time.h>
}

#include "Common/atomic.h"
#include "Common/HashMap.h"

#include "CellListScanner.h"
#include "Range.h"

namespace Hypertable {

  class ScannerMap {

  public:
    ScannerMap() : m_mutex() { return; }
    uint32_t put(CellListScannerPtr &scanner_ptr, RangePtr &range_ptr);
    bool get(uint32_t id, CellListScannerPtr &scanner_ptr, RangePtr &range_ptr);
    bool remove(uint32_t id);
    void purge_expired(time_t expire_time);

  private:

    time_t get_timestamp();

    static atomic_t ms_next_id;

    Mutex          m_mutex;

    struct ScanInfo {
      CellListScannerPtr scanner_ptr;
      RangePtr range_ptr;
      time_t last_access;
    };
    typedef hash_map<uint32_t, ScanInfo> CellListScannerMap;

    CellListScannerMap m_scanner_map;

  };

}


#endif // HYPERTABLE_SCANNERMAP_H
