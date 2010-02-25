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

#ifndef HYPERTABLE_LOCATIONCACHE_H
#define HYPERTABLE_LOCATIONCACHE_H

#include <cstring>
#include <ostream>
#include <map>
#include <set>

#include "Common/Mutex.h"
#include "Common/InetAddr.h"
#include "Common/ReferenceCount.h"
#include "Common/StringExt.h"

#include "RangeLocationInfo.h"

namespace Hypertable {

  /**
   * Key type for Range location cache
   */
  struct LocationCacheKey {
    uint32_t    table_id;
    const char *end_row;
  };

  /**
   * Less than operator for LocationCacheKey
   */
  inline bool operator<(const LocationCacheKey &x, const LocationCacheKey &y) {
    if (x.table_id != y.table_id)
      return x.table_id < y.table_id;
    if (y.end_row == 0)
      return (x.end_row == 0) ? false : true;
    else if (x.end_row == 0)
      return false;
    return strcmp(x.end_row, y.end_row) < 0;
  }

  /**
   * Equality operator for LocationCacheKey
   */
  inline bool operator==(const LocationCacheKey &x, const LocationCacheKey &y) {
    if (x.table_id != y.table_id)
      return false;
    if (x.end_row == 0)
      return (y.end_row == 0) ? true : false;
    else if (y.end_row == 0)
      return false;
    return !strcmp(x.end_row, y.end_row);
  }

  /**
   * Equality operator for LocationCacheKey
   */
  inline bool operator!=(const LocationCacheKey &x, const LocationCacheKey &y) {
    return !(x == y);
  }


  /**
   *  This class acts as a cache of Range location information.  It
   */
  class LocationCache : public ReferenceCount {
  public:
    /**
     */
    struct Value {
      struct Value *prev, *next;
      std::map<LocationCacheKey, Value *>::iterator map_iter;
      std::string start_row;
      std::string end_row;
      const CommAddress *addrp;
      bool pegged;
    };

    LocationCache(uint32_t max_entries) : m_mutex(), m_location_map(),
        m_head(0), m_tail(0), m_max_entries(max_entries) { return; }
    ~LocationCache();

    void insert(uint32_t table_id, RangeLocationInfo &range_loc_info,
                bool pegged=false);
    bool lookup(uint32_t table_id, const char *rowkey,
                RangeLocationInfo *rane_loc_infop, bool inclusive=false);
    bool invalidate(uint32_t table_id, const char *rowkey);

    void display(std::ostream &);

  private:
    void move_to_head(Value *cacheval);
    void remove(Value *cacheval);

    const CommAddress *get_constant_address(const CommAddress &addr);

    /** STL Strict Weak Ordering for comparing CommAddress pointers */
    struct CommAddressPointerLt {
      bool operator()(const CommAddress *addr1, const CommAddress *addr2) const {
	return *addr1 < *addr2;
      }
    };

    typedef std::map<LocationCacheKey, Value *> LocationMap;
    typedef std::set<const CommAddress *, CommAddressPointerLt> AddressSet;

    Mutex          m_mutex;
    LocationMap    m_location_map;
    AddressSet     m_addresses;
    Value         *m_head;
    Value         *m_tail;
    uint32_t       m_max_entries;
  };

  typedef intrusive_ptr<LocationCache> LocationCachePtr;

} // namespace Hypertable


#endif // HYPERTABLE_LOCATIONCACHE_H
