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

    /**
     * This method computes a unique scanner ID and puts the given scanner
     * and range pointers into a map using the scanner ID as the key.
     *
     * @param scanner_ptr smart pointer to scanner object
     * @param range_ptr smart pointer to range object
     * @param table table identifier for this scanner
     * @return unique scanner ID
     */
    uint32_t put(CellListScannerPtr &scanner_ptr, RangePtr &range_ptr,
                 const TableIdentifier *table);

    /**
     * This method retrieves the scanner and range mapped to the given scanner
     * id.  It also updates the 'last_access_millis' member of this scanner map
     * entry.
     *
     * @param id scanner id
     * @param scanner_ptr smart pointer to returned scanner object
     * @param range_ptr smart pointer to returned range object
     * @param table reference to (managed) table identifier
     * @return true if found, false if not
     */
    bool get(uint32_t id, CellListScannerPtr &scanner_ptr, RangePtr &range_ptr, TableIdentifierManaged &table);

    /**
     * This method removes the entry in the scanner map corresponding to the
     * given id
     *
     * @param id scanner id
     * @return true if removed, false if no mapping found
     */
    bool remove(uint32_t id);

    /**
     * This method iterates through the scanner map purging mappings that have
     * not been referenced for max_idle_ms or greater milliseconds.
     *
     * @param max_idle_ms maximum idle time
     */
    void purge_expired(uint32_t max_idle_ms);

  private:

    /**
     * Returns the number of milliseconds since the epoch
     */
    uint64_t get_timestamp_millis();

    static atomic_t ms_next_id;

    Mutex          m_mutex;

    struct ScanInfo {
      CellListScannerPtr scanner_ptr;
      RangePtr range_ptr;
      uint64_t last_access_millis;
      TableIdentifierManaged table;
    };
    typedef hash_map<uint32_t, ScanInfo> CellListScannerMap;

    CellListScannerMap m_scanner_map;

  };

}


#endif // HYPERTABLE_SCANNERMAP_H
