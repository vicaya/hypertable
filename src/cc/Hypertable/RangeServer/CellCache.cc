/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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


//#define STAT


CellCache::~CellCache() {
  uint64_t mem_freed = 0;
  uint32_t offset;
#ifdef STAT  
  uint32_t skipped = 0;
#endif

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
#ifdef STAT
    else
      skipped++;
#endif
  }

#ifdef STAT
  cout << flush;
  cout << "STAT[~CellCache]\tmemory freed\t" << mem_freed << endl;
  cout << "STAT[~CellCache]\tentries skipped\t" << skipped << endl;
  cout << "STAT[~CellCache]\tentries total\t" << m_cell_map.size() << endl;
#endif

  Global::memory_tracker.remove_memory(mem_freed);
  Global::memory_tracker.remove_items(m_cell_map.size());

  if (m_child)
    m_child->decrement_refcount();
}


/**
 * 
 */
int CellCache::add(const ByteString32T *key, const ByteString32T *value, uint64_t real_timestamp) {
  size_t kv_len = key->len + (2*sizeof(int32_t));
  ByteString32T *newKey;
  ByteString32T *newValue;

  (void)real_timestamp;

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

  if ( ! m_cell_map.insert(CellMapT::value_type(newKey, Length(newKey))).second ) {
    m_collisions++;
    HT_WARNF("Collision detected key insert (row = %s)", (const char *)newKey->data);
    delete [] newKey;
  }
  else {
    m_memory_used += kv_len;
    if ( key->data[ key->len - 9] <= FLAG_DELETE_CELL )
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
#ifdef STAT
  uint64_t dropped = 0;
#endif

  m_child = new CellCache();

  for (CellMapT::iterator iter = m_cell_map.begin(); iter != m_cell_map.end(); iter++) {

    if (!keyComps.load((*iter).first)) {
      HT_ERROR("Problem deserializing key/value pair");
      continue;
    }

    if (keyComps.timestamp > timestamp) {
      m_child->m_cell_map.insert(CellMapT::value_type((*iter).first, (*iter).second));
      (*iter).second |= ALLOC_BIT_MASK;  // mark this entry in the "old" map so it doesn't get deleted
    }
#ifdef STAT
    else
      dropped++;
#endif
  }

#ifdef STAT
  cout << flush;
  cout << "STAT[slice_copy]\tdropped\t" << dropped << endl;
#endif

  Global::memory_tracker.add_items(m_child->m_cell_map.size());

  m_child->increment_refcount();

  return m_child;
}


/**
 *
 */
void CellCache::purge_deletes() {
  Key key_comps;
  size_t len;
  bool          delete_present = false;
  DynamicBuffer deleted_row(0);
  uint64_t      deleted_row_timestamp = 0;
  DynamicBuffer deleted_column_family(0);
  uint64_t      deleted_column_family_timestamp = 0;
  DynamicBuffer deleted_cell(0);
  uint64_t      deleted_cell_timestamp = 0;
  CellMapT::iterator iter, tmp_iter;

  iter = m_cell_map.begin();

  while (iter != m_cell_map.end()) {
    
    if (!key_comps.load((*iter).first)) {
      HT_ERROR("Problem deserializing key/value pair");
      iter++;
      continue;
    }

    if (key_comps.flag == FLAG_INSERT) {

      if (delete_present) {
	if (deleted_cell.fill() > 0) {
	  len = (key_comps.column_qualifier - key_comps.row) + strlen(key_comps.column_qualifier) + 1;
	  if (deleted_cell.fill() == len && !memcmp(deleted_cell.buf, key_comps.row, len)) {
	    if (key_comps.timestamp < deleted_cell_timestamp) {
	      tmp_iter = iter;
	      iter++;
	      m_cell_map.erase(tmp_iter);
	    }
	    else
	      iter++;
	    continue;
	  }
	  deleted_cell.clear();
	}
	if (deleted_column_family.fill() > 0) {
	  len = key_comps.column_qualifier - key_comps.row;
	  if (deleted_column_family.fill() == len && !memcmp(deleted_column_family.buf, key_comps.row, len)) {
	    if (key_comps.timestamp < deleted_column_family_timestamp) {
	      tmp_iter = iter;
	      iter++;
	      m_cell_map.erase(tmp_iter);
	    }
	    else
	      iter++;
	    continue;
	  }
	  deleted_column_family.clear();
	}
	if (deleted_row.fill() > 0) {
	  len = strlen(key_comps.row) + 1;
	  if (deleted_row.fill() == len && !memcmp(deleted_row.buf, key_comps.row, len)) {
	    if (key_comps.timestamp < deleted_row_timestamp) {
	      tmp_iter = iter;
	      iter++;
	      m_cell_map.erase(tmp_iter);
	    }
	    else
	      iter++;
	    continue;
	  }
	  deleted_row.clear();
	}
	delete_present = false;
      }
      iter++;
    }
    else {
      if (key_comps.flag == FLAG_DELETE_ROW) {
	len = strlen(key_comps.row) + 1;
	if (delete_present && deleted_row.fill() == len && !memcmp(deleted_row.buf, key_comps.row, len)) {
	  if (deleted_row_timestamp < key_comps.timestamp)
	    deleted_row_timestamp = key_comps.timestamp;
	}
	else {
	  deleted_row.set(key_comps.row, len);
	  deleted_row_timestamp = key_comps.timestamp;
	  delete_present = true;
	}
      }
      else if (key_comps.flag == FLAG_DELETE_COLUMN_FAMILY) {
	len = key_comps.column_qualifier - key_comps.row;
	if (delete_present && deleted_column_family.fill() == len && !memcmp(deleted_column_family.buf, key_comps.row, len)) {
	  if (deleted_column_family_timestamp < key_comps.timestamp)
	    deleted_column_family_timestamp = key_comps.timestamp;
	}
	else {
	  deleted_column_family.set(key_comps.row, len);
	  deleted_column_family_timestamp = key_comps.timestamp;
	  delete_present = true;
	}
      }
      else if (key_comps.flag == FLAG_DELETE_CELL) {
	len = (key_comps.column_qualifier - key_comps.row) + strlen(key_comps.column_qualifier) + 1;
	if (delete_present && deleted_cell.fill() == len && !memcmp(deleted_cell.buf, key_comps.row, len)) {
	  if (deleted_cell_timestamp < key_comps.timestamp)
	    deleted_cell_timestamp = key_comps.timestamp;
	}
	else {
	  deleted_cell.set(key_comps.row, len);
	  deleted_cell_timestamp = key_comps.timestamp;
	  delete_present = true;
	}
      }
      tmp_iter = iter;
      iter++;
      m_cell_map.erase(tmp_iter);
    }
  }
  
}
