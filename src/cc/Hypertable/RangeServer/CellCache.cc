/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
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
#include <iostream>

#include "Common/Logger.h"
#include "Common/Serialization.h"

#include "Hypertable/Lib/Key.h"

#include "Config.h"
#include "CellCache.h"
#include "CellCacheScanner.h"
#include "Global.h"

using namespace Hypertable;
using namespace std;


CellCache::CellCache()
  : m_arena(), m_cell_map(std::less<const SerializedKey>(), Alloc(m_arena)),
    m_deletes(0), m_collisions(0), m_key_bytes(0), m_value_bytes(0), 
    m_frozen(false), m_have_counter_deletes(false) {
  assert(Config::properties); // requires Config::init* first
  m_arena.set_page_size((size_t)
      Config::get_i32("Hypertable.RangeServer.AccessGroup.CellCache.PageSize"));
}


/**
 */
void CellCache::add(const Key &key, const ByteString value) {
  SerializedKey new_key;
  uint8_t *ptr;
  size_t total_len = key.length + value.length();

  m_key_bytes += key.length;
  m_value_bytes += value.length();

  assert(!m_frozen);

  new_key.ptr = ptr = m_arena.alloc(total_len);

  memcpy(ptr, key.serial.ptr, key.length);
  ptr += key.length;

  value.write(ptr);

  if (! m_cell_map.insert(CellMap::value_type(new_key, key.length)).second) {
    m_collisions++;
    HT_WARNF("Collision detected key insert (row = %s)", new_key.row());
  }
  else {
    if (key.flag <= FLAG_DELETE_CELL)
      m_deletes++;
  }
}


/**
 */
void CellCache::add_counter(const Key &key, const ByteString value) {

  // Check for counter reset
  if (*value.ptr == 9) {
    HT_ASSERT(value.ptr[9] == '=');
    add(key, value);
    return;
  }
  else if (m_have_counter_deletes || key.flag != FLAG_INSERT) {
    add(key, value);
    m_have_counter_deletes = true;
    return;
  }

  CellMap::iterator iter = m_cell_map.lower_bound(key.serial);

  if (iter == m_cell_map.end()) {
    add(key, value);
    return;
  }

  const uint8_t *ptr;

  size_t len = (*iter).first.decode_length(&ptr);

  // If the lengths differ, assume they're different keys and do a normal add
  if (len + (ptr-(*iter).first.ptr) != key.length) {
    add(key, value);
    return;
  }

  if (memcmp(ptr+1, key.row, (key.flag_ptr+1)-(const uint8_t *)key.row)) {
    add(key, value);
    return;
  }

  ByteString old_value;
  old_value.ptr = (*iter).first.ptr + (*iter).second;

  /*
   * Sanity check the old value, if it's a reset just insert the new value
   */
  if (*old_value.ptr != 8) {
    add(key, value);
    return;
  }

  /*
   * copy timestamp/revision info from insert key to the one in the map
   */
  size_t offset = (key.flag_ptr-((const uint8_t *)key.serial.ptr)) + 1;
  len = (*iter).second - offset;
  memcpy(((uint8_t *)(*iter).first.ptr) + offset, key.flag_ptr+1, len);

  // read old value
  ptr = old_value.ptr+1;
  size_t remaining = 8;
  int64_t old_count = (int64_t)Serialization::decode_i64(&ptr, &remaining);

  // read new value
  ptr = value.ptr+1;
  remaining = 8;
  int64_t new_count = (int64_t)Serialization::decode_i64(&ptr, &remaining);

  uint8_t *write_ptr = (uint8_t *)old_value.ptr+1;

  Serialization::encode_i64(&write_ptr, old_count+new_count);
}



const char *CellCache::get_split_row() {
  assert(!"CellCache::get_split_row not implemented!");
  return 0;
}



void CellCache::get_split_rows(std::vector<std::string> &split_rows) {
  ScopedLock lock(m_mutex);
  if (m_cell_map.size() > 2) {
    CellMap::const_iterator iter = m_cell_map.begin();
    size_t i=0, mid = m_cell_map.size() / 2;
    for (i=0; i<mid; i++)
      ++iter;
    split_rows.push_back((*iter).first.row());
  }
}



void CellCache::get_rows(std::vector<std::string> &rows) {
  ScopedLock lock(m_mutex);
  const char *row, *last_row = "";
  for (CellMap::const_iterator iter = m_cell_map.begin();
       iter != m_cell_map.end(); ++iter) {
    row = (*iter).first.row();
    if (strcmp(row, last_row)) {
      rows.push_back(row);
      last_row = row;
    }
  }
}



CellListScanner *CellCache::create_scanner(ScanContextPtr &scan_ctx) {
  CellCachePtr cellcache(this);
  return new CellCacheScanner(cellcache, scan_ctx);
}
