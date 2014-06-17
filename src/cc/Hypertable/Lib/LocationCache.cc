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

#include "Common/Compat.h"
#include <cassert>
#include <cstring>
#include <fstream>
#include <iostream>

#include "Common/InetAddr.h"

#include "LocationCache.h"

using namespace Hypertable;
using namespace std;

/**
 * Insert
 */
void
LocationCache::insert(const char *table_name, RangeLocationInfo &range_loc_info,
                      bool pegged) {
  ScopedLock lock(m_mutex);
  Value *newval = new Value;
  LocationMap::iterator iter;
  LocationCacheKey key;

  assert(table_name);

  /*
  HT_DEBUG_OUT << table_name << " start=" << start_row << " end=" << end_row
      << " location=" << location << HT_END;
  */

  newval->start_row = range_loc_info.start_row;
  newval->end_row = range_loc_info.end_row;
  newval->addrp = get_constant_address(range_loc_info.addr);
  newval->pegged = pegged;

  key.table_name = m_strings.get(table_name);
  key.end_row = (range_loc_info.end_row == "") ? 0 : newval->end_row.c_str();

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
    newval->next = newval->prev = 0;
    m_head = m_tail = newval;
  }
  else {
    m_head->next = newval;
    newval->prev = m_head;
    newval->next = 0;
    m_head = newval;
  }

  // Insert the new entry into the map, recording an iterator to the entry
  {
    std::pair<LocationMap::iterator, bool> old_entry;
    LocationMap::value_type map_value(key, newval);
    old_entry = m_location_map.insert(map_value);
    assert(old_entry.second);
    newval->map_iter = old_entry.first;
  }

}

/**
 *
 */
LocationCache::~LocationCache() {
  for (AddressSet::iterator iter = m_addresses.begin();
       iter != m_addresses.end(); ++iter)
    delete *iter;
  for (LocationMap::iterator lm_it = m_location_map.begin();
       lm_it != m_location_map.end(); ++lm_it)
    delete (*lm_it).second;
}


/**
 * Lookup
 */
bool
LocationCache::lookup(const char * table_name, const char *rowkey,
                      RangeLocationInfo *rane_loc_infop, bool inclusive) {
  ScopedLock lock(m_mutex);
  LocationMap::iterator iter;
  LocationCacheKey key;

  assert(table_name);

  //cout << table_name << " row=" << rowkey << endl << flush;

  key.table_name = table_name;
  key.end_row = rowkey;

  if ((iter = m_location_map.lower_bound(key)) == m_location_map.end())
    return false;

  if (strcmp((*iter).first.table_name, table_name))
    return false;

  if (inclusive) {
    if (strcmp(rowkey, (*iter).second->start_row.c_str()) < 0)
      return false;
  }
  else {
    if (strcmp(rowkey, (*iter).second->start_row.c_str()) <= 0)
      return false;
  }

  move_to_head((*iter).second);

  rane_loc_infop->start_row = (*iter).second->start_row;
  rane_loc_infop->end_row   = (*iter).second->end_row;
  rane_loc_infop->addr      = *(*iter).second->addrp;

  return true;
}

bool LocationCache::invalidate(const char * table_name, const char *rowkey) {
  ScopedLock lock(m_mutex);
  LocationMap::iterator iter;
  LocationCacheKey key;

  assert(table_name);

  //cout << table_name << " row=" << rowkey << endl << flush;

  key.table_name = table_name;
  key.end_row = rowkey;

  if ((iter = m_location_map.lower_bound(key)) == m_location_map.end())
    return false;

  if (strcmp((*iter).first.table_name, table_name))
    return false;

  if (strcmp(rowkey, (*iter).second->start_row.c_str()) < 0)
    return false;

  remove((*iter).second);
  return true;
}


void LocationCache::display(std::ostream &out) {
  for (Value *value = m_head; value; value = value->prev)
    out << "DUMP: end=" << value->end_row << " start=" << value->start_row
        << endl;
}


/**
 * MoveToHead
 */
void LocationCache::move_to_head(Value *cacheval) {

  if (m_head == cacheval)
    return;

  // unstich entry from cache
  cacheval->next->prev = cacheval->prev;
  if (cacheval->prev == 0)
    m_tail = cacheval->next;
  else
    cacheval->prev->next = cacheval->next;

  cacheval->next = 0;
  cacheval->prev = m_head;
  m_head->next = cacheval;
  m_head = cacheval;
}


/**
 * remove
 */
void LocationCache::remove(Value *cacheval) {
  assert(cacheval);
  if (m_tail == cacheval) {
    m_tail = cacheval->next;
    if (m_tail)
      m_tail->prev = 0;
    else {
      assert (m_head == cacheval);
      m_head = 0;
    }
  }
  else if (m_head == cacheval) {
    m_head = m_head->prev;
    m_head->next = 0;
  }
  else {
    cacheval->next->prev = cacheval->prev;
    cacheval->prev->next = cacheval->next;
  }
  m_location_map.erase(cacheval->map_iter);
  delete cacheval;
}


const CommAddress *LocationCache::get_constant_address(const CommAddress &addr) {
  AddressSet::iterator iter = m_addresses.find(&addr);

  if (iter != m_addresses.end())
    return *iter;

  CommAddress *new_addr = new CommAddress(addr);
  m_addresses.insert(new_addr);
  return new_addr;
}

