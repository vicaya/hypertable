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

#include "FileBlockCache.h"

using namespace Hypertable;

atomic_t FileBlockCache::ms_next_file_id = ATOMIC_INIT(0);


FileBlockCache::~FileBlockCache() {
  for (BlockMapT::iterator iter = m_block_map.begin(); iter != m_block_map.end(); iter++) {
    delete [] (*iter).second->block;
    delete (*iter).second;
  }
}

bool FileBlockCache::checkout(int fileId, uint32_t offset, uint8_t **blockp, uint32_t *lengthp) {
  boost::mutex::scoped_lock lock(m_mutex);
  CacheKeyT key;
  BlockMapT::iterator iter;

  key.fileId = fileId;
  key.offset = offset;

  if ((iter = m_block_map.find(key)) == m_block_map.end())
    return false;

  move_to_head((*iter).second);

  (*iter).second->refCount++;

  *blockp = (*iter).second->block;
  *lengthp = (*iter).second->length;
  return true;
}


void FileBlockCache::checkin(int fileId, uint32_t offset) {
  boost::mutex::scoped_lock lock(m_mutex);
  CacheKeyT key;
  BlockMapT::iterator iter;

  key.fileId = fileId;
  key.offset = offset;

  iter = m_block_map.find(key);

  assert(iter != m_block_map.end() && (*iter).second->refCount > 0);

  (*iter).second->refCount--;
}


bool FileBlockCache::insert_and_checkout(int fileId, uint32_t offset, uint8_t *block, uint32_t length) {
  boost::mutex::scoped_lock lock(m_mutex);
  CacheValueT *cacheValue = new CacheValueT;
  BlockMapT::iterator iter;

  cacheValue->key.fileId = fileId;
  cacheValue->key.offset = offset;

  if ((iter = m_block_map.find(cacheValue->key)) != m_block_map.end()) {
    move_to_head((*iter).second);
    delete cacheValue;
    return false;
  }

  cacheValue->block = block;
  cacheValue->length = length;
  cacheValue->refCount = 1;

  if (m_head == 0) {
    cacheValue->next = cacheValue->prev = 0;
    m_head = m_tail = cacheValue;
  }
  else {
    m_head->next = cacheValue;
    cacheValue->prev = m_head;
    cacheValue->next = 0;
    m_head = cacheValue;
  }

  m_block_map[cacheValue->key] = cacheValue;
  return true;
}


void FileBlockCache::move_to_head(CacheValueT *cacheValue) {

  if (m_head == cacheValue)
    return;

  cacheValue->next->prev = cacheValue->prev;
  if (cacheValue->prev == 0)
    m_tail = cacheValue->next;
  else
    cacheValue->prev->next = cacheValue->next;

  cacheValue->next = 0;
  m_head->next = cacheValue;
  cacheValue->prev = m_head;
  m_head = cacheValue;
}

