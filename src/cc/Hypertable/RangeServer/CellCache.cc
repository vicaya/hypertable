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
#include <iostream>

#include "Common/Logger.h"

#include "Hypertable/Lib/Key.h"

#include "CellCache.h"
#include "CellCacheScanner.h"
#include "Global.h"

using namespace Hypertable;
using namespace std;


CellCache::~CellCache() {
  for (CellMap::iterator iter = m_cell_map.begin();
       iter != m_cell_map.end(); ++iter)
    delete [] (uint8_t *)(*iter).first.ptr;
  Global::memory_tracker.remove_memory(m_memory_used);
  Global::memory_tracker.remove_items(m_cell_map.size());
}



/**
 */
int CellCache::add(const Key &key, const ByteString value) {
  SerializedKey new_key;
  uint8_t *ptr;
  size_t total_len = key.length + value.length();

  assert(!m_frozen);

  new_key.ptr = ptr = new uint8_t [total_len];

  memcpy(ptr, key.serial.ptr, key.length);
  ptr += key.length;

  value.write(ptr);

  if (! m_cell_map.insert(CellMap::value_type(new_key, key.length)).second) {
    m_collisions++;
    HT_WARNF("Collision detected key insert (row = %s)", new_key.row());
    delete [] new_key.ptr;
  }
  else {
    m_memory_used += total_len;
    if (key.flag <= FLAG_DELETE_CELL)
      m_deletes++;
  }

  return 0;
}



const char *CellCache::get_split_row() {
  assert(!"CellCache::get_split_row not implemented!");
  return 0;
}



void CellCache::get_split_rows(std::vector<std::string> &split_rows) {
  boost::mutex::scoped_lock lock(m_mutex);
  if (m_cell_map.size() > 2) {
    CellMap::const_iterator iter = m_cell_map.begin();
    size_t i=0, mid = m_cell_map.size() / 2;
    for (i=0; i<mid; i++)
      ++iter;
    split_rows.push_back((*iter).first.row());
  }
}



void CellCache::get_rows(std::vector<std::string> &rows) {
  boost::mutex::scoped_lock lock(m_mutex);
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
