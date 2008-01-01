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
#include <iostream>

#include "Common/Logger.h"

#include "Hypertable/Lib/Key.h"

#include "CellCache.h"
#include "CellCacheScanner.h"
#include "Global.h"

using namespace Hypertable;
using namespace std;


const uint32_t CellCache::ALLOC_BIT_MASK  = 0x80000000;
const uint32_t CellCache::OFFSET_BIT_MASK = 0x7FFFFFFF;


CellCache::~CellCache() {
  uint64_t mem_freed = 0;
  uint32_t offset;
  uint32_t skipped = 0;

  /**
   * If our reference count is greater than zero, the we still have a parent
   * that is referencing us and our memory, so wait for our parent to
   * be destructed and then deallocate.
   * Remember: parents share some k/v pair memory with their children
   * and it is the child's responsibility to delete it.
   */
  {
    boost::mutex::scoped_lock lock(m_refcount_mutex);
    while (m_refcount > 0) 
      m_refcount_cond.wait(lock);
  }

  for (CellMapT::iterator iter = m_cell_map.begin(); iter != m_cell_map.end(); iter++) {
    if (((*iter).second & ALLOC_BIT_MASK) == 0) {
      offset = (*iter).second & OFFSET_BIT_MASK;
      mem_freed += offset + Length((ByteString32T *)(((uint8_t *)(*iter).first) + offset));
      delete [] (*iter).first;
    }
    else
      skipped++;
  }

  LOG_VA_INFO("dougo mem=%ld skipped=%ld total=%ld", mem_freed, skipped, m_cell_map.size());

  Global::memory_tracker.remove_memory(mem_freed);
  Global::memory_tracker.remove_items(m_cell_map.size());

  if (m_child)
    m_child->decrement_refcount();
}


/**
 * 
 */
int CellCache::add(const ByteString32T *key, const ByteString32T *value) {
  size_t kv_len = key->len + (2*sizeof(int32_t));
  ByteString32T *newKey;
  ByteString32T *newValue;

  if (value) {
    kv_len += value->len;
    newKey = (ByteString32T *)new uint8_t [ kv_len ];
    newValue = (ByteString32T *)(newKey->data + key->len);
    memcpy(newKey, key, sizeof(int32_t) + key->len);
    memcpy(newValue, value, sizeof(int32_t) + value->len);
  }
  else {
    newKey = (ByteString32T *)new uint8_t [ kv_len ];
    newValue = (ByteString32T *)(newKey->data + key->len);
    memcpy(newKey, key, sizeof(int32_t) + key->len);
    newValue->len = 0;
  }

  m_cell_map.insert(CellMapT::value_type(newKey, Length(newKey)));

  m_memory_used += kv_len;

  return 0;
}



const char *CellCache::get_split_row() {
  assert(!"CellCache::get_split_row not implemented!");
  return 0;
}



void CellCache::get_split_rows(std::vector<std::string> &split_rows) {
  boost::mutex::scoped_lock lock(m_mutex);
  if (m_cell_map.size() > 2) {
    CellMapT::const_iterator iter = m_cell_map.begin();
    size_t i=0, mid = m_cell_map.size() / 2;
    for (i=0; i<mid; i++)
      iter++;
    split_rows.push_back((const char *)(*iter).first->data);
  }
}



void CellCache::get_rows(std::vector<std::string> &rows) {
  boost::mutex::scoped_lock lock(m_mutex);
  const char *last_row = "";
  for (CellMapT::const_iterator iter = m_cell_map.begin(); iter != m_cell_map.end(); iter++) {
    if (strcmp((const char *)(*iter).first->data, last_row)) {
      rows.push_back((const char *)(*iter).first->data);
      last_row = (const char *)(*iter).first->data;
    }
  }
}



CellListScanner *CellCache::create_scanner(ScanContextPtr &scanContextPtr) {
  CellCachePtr cellCachePtr(this);
  return new CellCacheScanner(cellCachePtr, scanContextPtr);
}



/**
 * This must be called with the cell cache locked
 */
CellCache *CellCache::slice_copy(uint64_t timestamp) {
  Key keyComps;

  m_child = new CellCache();

  for (CellMapT::iterator iter = m_cell_map.begin(); iter != m_cell_map.end(); iter++) {

    if (!keyComps.load((*iter).first)) {
      LOG_ERROR("Problem deserializing key/value pair");
      continue;
    }

    if (keyComps.timestamp > timestamp) {
      m_child->m_cell_map.insert(CellMapT::value_type((*iter).first, (*iter).second));
      (*iter).second |= ALLOC_BIT_MASK;  // mark this entry in the "old" map so it doesn't get deleted
    }
  }

  Global::memory_tracker.add_items(m_child->m_cell_map.size());

  m_child->increment_refcount();

  return m_child;
}

