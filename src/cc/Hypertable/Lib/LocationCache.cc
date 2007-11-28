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
#include <cassert>
#include <cstring>
#include <fstream>
#include <iostream>

extern "C" {
#include <stdlib.h>
#include <limits.h>
}

#include "Common/InetAddr.h"

#include "LocationCache.h"

using namespace Hypertable;
using namespace std;

/**
 * Insert
 */
void LocationCache::insert(uint32_t tableId, RangeLocationInfo &range_loc_info, bool pegged) {
  boost::mutex::scoped_lock lock(m_mutex);
  ValueT *newValue = new ValueT;
  LocationMapT::iterator iter;
  LocationCacheKeyT key;

  //cout << tableId << " start=" << start_row << " end=" << end_row << " location=" << location << endl << flush;

  newValue->start_row = range_loc_info.start_row;
  newValue->end_row   = range_loc_info.end_row;
  newValue->location  = get_constant_location_str(range_loc_info.location.c_str());
  newValue->pegged    = pegged;

  key.tableId = tableId;
  key.end_row = (range_loc_info.end_row == "") ? 0 : newValue->end_row.c_str();

  // remove old entry
  if ((iter = m_location_map.find(key)) != m_location_map.end())
    remove((*iter).second);

  // make room for the new entry
  while (m_location_map.size() >= m_max_entries) {
    if (m_tail->pegged)
      move_to_head(m_tail);
    else
      remove(m_tail);
  }

  // add to head
  if (m_head == 0) {
    assert(m_tail == 0);
    newValue->next = newValue->prev = 0;
    m_head = m_tail = newValue;
  }
  else {
    m_head->next = newValue;
    newValue->prev = m_head;
    newValue->next = 0;
    m_head = newValue;
  }

  // Insert the new entry into the map, recording an iterator to the entry in the map
  {
    std::pair<LocationMapT::iterator, bool> old_entry;
    LocationMapT::value_type map_value(key, newValue);
    old_entry = m_location_map.insert(map_value);
    assert(old_entry.second);
    newValue->mapIter = old_entry.first;
  }

}

/**
 *
 */
LocationCache::~LocationCache() {
  for (std::set<const char *, lt_cstr>::iterator iter = m_location_strings.begin(); iter != m_location_strings.end(); iter++)
    delete [] *iter;
  for (LocationMapT::iterator lmIter = m_location_map.begin(); lmIter != m_location_map.end(); lmIter++)
    delete (*lmIter).second;
}


/**
 * Lookup
 */
bool LocationCache::lookup(uint32_t tableId, const char *rowKey, RangeLocationInfo *range_loc_info_p, bool inclusive) {
  boost::mutex::scoped_lock lock(m_mutex);
  LocationMapT::iterator iter;
  LocationCacheKeyT key;

  //cout << tableId << " row=" << rowKey << endl << flush;

  key.tableId = tableId;
  key.end_row = rowKey;

  if ((iter = m_location_map.lower_bound(key)) == m_location_map.end())
    return false;

  if ((*iter).first.tableId != tableId)
    return false;

  if (inclusive) {
    if (strcmp(rowKey, (*iter).second->start_row.c_str()) < 0)
      return false;
  }
  else {
    if (strcmp(rowKey, (*iter).second->start_row.c_str()) <= 0)
      return false;
  }

  move_to_head((*iter).second);

  range_loc_info_p->start_row = (*iter).second->start_row;
  range_loc_info_p->end_row   = (*iter).second->end_row;
  range_loc_info_p->location  = (*iter).second->location;

  return true;
}


void LocationCache::display(std::ofstream &outfile) {
  for (ValueT *value = m_head; value; value = value->prev)
    outfile << "DUMP: end=" << value->end_row << " start=" << value->start_row << endl;
}



/**
 * MoveToHead
 */
void LocationCache::move_to_head(ValueT *cacheValue) {

  if (m_head == cacheValue)
    return;

  // unstich entry from cache
  cacheValue->next->prev = cacheValue->prev;
  if (cacheValue->prev == 0)
    m_tail = cacheValue->next;
  else
    cacheValue->prev->next = cacheValue->next;

  cacheValue->next = 0;
  cacheValue->prev = m_head;
  m_head->next = cacheValue;
  m_head = cacheValue;
}


/**
 * remove
 */
void LocationCache::remove(ValueT *cacheValue) {
  assert(cacheValue);
  if (m_tail == cacheValue) {
    m_tail = cacheValue->next;
    if (m_tail)
      m_tail->prev = 0;
    else {
      assert (m_head == cacheValue);
      m_head = 0;
    }
  }
  else if (m_head == cacheValue) {
    m_head = m_head->prev;
    m_head->next = 0;
  }
  else {
    cacheValue->next->prev = cacheValue->prev;
    cacheValue->prev->next = cacheValue->next;
  }
  m_location_map.erase(cacheValue->mapIter);  
  delete cacheValue;
}



const char *LocationCache::get_constant_location_str(const char *location) {
  std::set<const char *, lt_cstr>::iterator iter = m_location_strings.find(location);

  if (iter != m_location_strings.end())
    return *iter;

  char *constantId = new char [ strlen(location) + 1 ];
  strcpy(constantId, location);
  m_location_strings.insert(constantId);
  return constantId;
}


/** 
 * 
 */
bool LocationCache::location_to_addr(const char *location, struct sockaddr_in &addr) {
  const char *ptr = location + strlen(location);
  std::string host;
  uint16_t port;
  int us_seen = 0;

  for (--ptr; ptr >= location; --ptr) {
    if (*ptr == '_') {
      if (us_seen > 0)
	break;
      us_seen++;
    }
  }

  port = (uint16_t)strtol(ptr+1, 0, 10);

  host = std::string(location, ptr-location);

  return InetAddr::initialize(&addr, host.c_str(), port);
}
