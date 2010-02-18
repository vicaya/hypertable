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

#include "FileBlockCache.h"

using namespace Hypertable;
using std::pair;

atomic_t FileBlockCache::ms_next_file_id = ATOMIC_INIT(0);

FileBlockCache::~FileBlockCache() {
  for (BlockCache::const_iterator iter = m_cache.begin();
       iter != m_cache.end(); ++iter)
    delete [] (*iter).block;
}

bool
FileBlockCache::checkout(int file_id, uint32_t file_offset, uint8_t **blockp,
                         uint32_t *lengthp) {
  ScopedLock lock(m_mutex);
  HashIndex &hash_index = m_cache.get<1>();
  HashIndex::iterator iter;
  int64_t key = ((int64_t)file_id << 32) | file_offset;

  if ((iter = hash_index.find(key)) == hash_index.end())
    return false;

  BlockCacheEntry entry = *iter;
  entry.ref_count++;

  hash_index.erase(iter);

  pair<Sequence::iterator, bool> insert_result = m_cache.push_back(entry);
  assert(insert_result.second);

  *blockp = (*insert_result.first).block;
  *lengthp = (*insert_result.first).length;

  return true;
}


void FileBlockCache::checkin(int file_id, uint32_t file_offset) {
  ScopedLock lock(m_mutex);
  HashIndex &hash_index = m_cache.get<1>();
  HashIndex::iterator iter;
  int64_t key = ((int64_t)file_id << 32) | file_offset;

  iter = hash_index.find(key);

  assert(iter != hash_index.end() && (*iter).ref_count > 0);

  hash_index.modify(iter, DecrementRefCount());
}


bool
FileBlockCache::insert_and_checkout(int file_id, uint32_t file_offset,
                                    uint8_t *block, uint32_t length) {
  ScopedLock lock(m_mutex);
  HashIndex &hash_index = m_cache.get<1>();
  int64_t key = ((int64_t)file_id << 32) | file_offset;

  if (length > m_limit || hash_index.find(key) != hash_index.end())
    return false;

  if (m_available < length)
    make_room(length);

  if (m_available < length)
    return false;

  BlockCacheEntry entry(file_id, file_offset);
  entry.block = block;
  entry.length = length;
  entry.ref_count = 1;

  pair<Sequence::iterator, bool> insert_result = m_cache.push_back(entry);
  assert(insert_result.second);

  m_available -= length;

  return true;
}


bool FileBlockCache::contains(int file_id, uint32_t file_offset) {
  ScopedLock lock(m_mutex);
  HashIndex &hash_index = m_cache.get<1>();
  int64_t key = ((int64_t)file_id << 32) | file_offset;

  return (hash_index.find(key) != hash_index.end());
}


void FileBlockCache::increase_limit(int64_t amount) {
  ScopedLock lock(m_mutex);
  int64_t adjusted_amount = amount;
  if ((m_max_memory-m_limit) < amount)
    adjusted_amount = m_max_memory - m_limit;
  m_limit += adjusted_amount;
  m_available += adjusted_amount;
}


int64_t FileBlockCache::decrease_limit(int64_t amount) {
  ScopedLock lock(m_mutex);
  int64_t memory_freed = 0;
  if (m_available < amount) {
    if (amount > (m_limit - m_min_memory))
      amount = m_limit - m_min_memory;
    memory_freed = make_room(amount);
    if (m_available < amount)
      amount = m_available;
  }
  m_available -= amount;
  m_limit -= amount;
  return memory_freed;
}


int64_t FileBlockCache::make_room(int64_t amount) {
  BlockCache::iterator iter = m_cache.begin();
  int64_t amount_freed = 0;
  while (iter != m_cache.end()) {
    if ((*iter).ref_count == 0) {
      m_available += (*iter).length;
      amount_freed += (*iter).length;
      delete [] (*iter).block;
      iter = m_cache.erase(iter);
      if (m_available >= amount)
	break;
    }
    else
      ++iter;
  }
  return amount_freed;
}
