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

#ifndef HYPERTABLE_LOCATIONCACHE_H
#define HYPERTABLE_LOCATIONCACHE_H

#include <cstring>
#include <fstream>
#include <map>
#include <set>

#include <boost/thread/mutex.hpp>

extern "C" {
#include <netinet/in.h>
}

#include "Common/ReferenceCount.h"
#include "Common/StringExt.h"

#include "RangeLocationInfo.h"

namespace Hypertable {

  /**
   * Key type for Range location cache
   */
  typedef struct {
    uint32_t    tableId;
    const char *end_row;
  } LocationCacheKeyT;

  /**
   * Less than operator for LocationCacheKeyT
   */
  inline bool operator<(const LocationCacheKeyT &k1, const LocationCacheKeyT &k2) {
    if (k1.tableId != k2.tableId)
      return k1.tableId < k2.tableId;
    if (k2.end_row == 0)
      return (k1.end_row == 0) ? false : true;
    else if (k1.end_row == 0)
      return false;
    return strcmp(k1.end_row, k2.end_row) < 0;
  }

  /**
   * Equality operator for LocationCacheKeyT
   */
  inline bool operator==(const LocationCacheKeyT &k1, const LocationCacheKeyT &k2) {
    if (k1.tableId != k2.tableId)
      return false;
    if (k1.end_row == 0)
      return (k2.end_row == 0) ? true : false;
    else if (k2.end_row == 0)
      return false;
    return !strcmp(k1.end_row, k2.end_row);
  }

  /**
   * Equality operator for LocationCacheKeyT
   */
  inline bool operator!=(const LocationCacheKeyT &k1, const LocationCacheKeyT &k2) {
    return !(k1 == k2);
  }


  /**
   *  This class acts as a cache of Range location information.  It 
   */
  class LocationCache {

  public:

    /** 
     */
    typedef struct cache_value {
      struct cache_value *prev;
      struct cache_value *next;
      std::map<LocationCacheKeyT, struct cache_value *>::iterator mapIter;
      std::string start_row;
      std::string end_row;
      const char *location;
      bool pegged;
    } ValueT;

    LocationCache(uint32_t maxEntries) : m_mutex(), m_location_map(), m_head(0), m_tail(0), m_max_entries(maxEntries) { return; }
    ~LocationCache();

    void insert(uint32_t tableId, RangeLocationInfo &range_loc_info, bool pegged=false);
    bool lookup(uint32_t tableId, const char *rowKey, RangeLocationInfo *range_loc_info_p, bool inclusive=false);

    void display(std::ofstream &outfile);

    static bool location_to_addr(const char *location, struct sockaddr_in &addr);

  private:

    void move_to_head(ValueT *cacheValue);
    void remove(ValueT *cacheValue);

    const char *get_constant_location_str(const char *location);

    typedef std::map<LocationCacheKeyT, ValueT *> LocationMapT;

    boost::mutex   m_mutex;
    LocationMapT   m_location_map;
    std::set<const char *, lt_cstr>  m_location_strings;
    ValueT        *m_head;
    ValueT        *m_tail;
    uint32_t       m_max_entries;
  };

}


#endif // HYPERTABLE_LOCATIONCACHE_H
