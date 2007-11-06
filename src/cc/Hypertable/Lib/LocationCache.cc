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

#include "LocationCache.h"

using namespace hypertable;
using namespace std;

/**
 * Insert
 */
void LocationCache::insert(uint32_t tableId, const char *startRow, const char *endRow, const char *serverId) {
  boost::mutex::scoped_lock lock(m_mutex);
  ValueT *newValue = new ValueT;
  LocationMapT::iterator iter;
  LocationCacheKeyT key;

  //cout << tableId << " start=" << startRow << " end=" << endRow << " serverId=" << serverId << endl << flush;

  serverId = get_constant_server_id(serverId);

  newValue->startRow = (startRow) ? startRow : "";
  newValue->endRow   = (endRow) ? endRow : "";
  newValue->serverId = serverId;

  key.tableId = tableId;
  key.endRow = (endRow) ? newValue->endRow.c_str() : 0;

  // remove old entry
  if ((iter = m_location_map.find(key)) != m_location_map.end())
    remove((*iter).second);

  // make room for the new entry
  while (m_location_map.size() >= m_max_entries)
    remove(m_tail);

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
  for (std::set<const char *, lt_cstr>::iterator iter = m_server_id_strings.begin(); iter != m_server_id_strings.end(); iter++)
    delete [] *iter;
  for (LocationMapT::iterator lmIter = m_location_map.begin(); lmIter != m_location_map.end(); lmIter++)
    delete (*lmIter).second;
}


/**
 * Lookup
 */
bool LocationCache::lookup(uint32_t tableId, const char *rowKey, const char **serverIdPtr) {
  boost::mutex::scoped_lock lock(m_mutex);
  LocationMapT::iterator iter;
  LocationCacheKeyT key;

  //cout << tableId << " row=" << rowKey << endl << flush;

  key.tableId = tableId;
  key.endRow = rowKey;

  if ((iter = m_location_map.lower_bound(key)) == m_location_map.end())
    return false;

  if ((*iter).first.tableId != tableId)
    return false;

  if (strcmp(rowKey, (*iter).second->startRow.c_str()) <= 0)
    return false;

  cout << "Moving to head '" << (*iter).second->endRow << "'" << endl;
  move_to_head((*iter).second);

  *serverIdPtr = (*iter).second->serverId;

  return true;
}


void LocationCache::display(ofstream &outfile) {
  for (ValueT *value = m_head; value; value = value->prev)
    outfile << "DUMP: end=" << value->endRow << " start=" << value->startRow << endl;
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



const char *LocationCache::get_constant_server_id(const char *serverId) {
  std::set<const char *, lt_cstr>::iterator iter = m_server_id_strings.find(serverId);

  if (iter != m_server_id_strings.end())
    return *iter;

  char *constantId = new char [ strlen(serverId) + 1 ];
  strcpy(constantId, serverId);
  m_server_id_strings.insert(constantId);
  return constantId;
}
