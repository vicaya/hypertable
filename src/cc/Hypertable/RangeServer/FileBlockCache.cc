/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 * 
 * This file is part of Hypertable.
 * 
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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

#include <cassert>
#include <iostream>

#include "FileBlockCache.h"

using namespace Hypertable;

#if ULONG_MAX == 4294967295 // 2**32 -1
namespace boost {
  std::size_t hash_value(uint64_t llval) {
    return (std::size_t)(llval >> 32) ^ (std::size_t)llval;
  }
}
#endif

atomic_t FileBlockCache::ms_next_file_id = ATOMIC_INIT(0);

FileBlockCache::~FileBlockCache() {
  for (BlockCache::const_iterator iter = m_cache.begin(); iter != m_cache.end(); iter++)
    delete [] (*iter).block;
}

bool FileBlockCache::checkout(int file_id, uint32_t file_offset, uint8_t **blockp, uint32_t *lengthp) {
  boost::mutex::scoped_lock lock(m_mutex);
  BlockCache::nth_index<1>::type &hash_index = m_cache.get<1>();
  BlockCache::nth_index<1>::type::iterator iter;
  uint64_t key = ((uint64_t)file_id << 32) | file_offset;

  if ((iter = hash_index.find(key)) == hash_index.end())
    return false;

  block_cache_entry entry = *iter;
  entry.ref_count++;

  hash_index.erase(iter);
  
  std::pair<BlockCache::nth_index<0>::type::iterator, bool> insert_result = m_cache.push_back(entry);
  assert(insert_result.second);

  *blockp = (*insert_result.first).block;
  *lengthp = (*insert_result.first).length;

  return true;
}


void FileBlockCache::checkin(int file_id, uint32_t file_offset) {
  boost::mutex::scoped_lock lock(m_mutex);
  BlockCache::nth_index<1>::type &hash_index = m_cache.get<1>();
  BlockCache::nth_index<1>::type::iterator iter;
  uint64_t key = ((uint64_t)file_id << 32) | file_offset;

  iter = hash_index.find(key);

  assert(iter != hash_index.end() && (*iter).ref_count > 0);

  hash_index.modify(iter, decrement_ref_count());

}


bool FileBlockCache::insert_and_checkout(int file_id, uint32_t file_offset, uint8_t *block, uint32_t length) {
  boost::mutex::scoped_lock lock(m_mutex);
  BlockCache::nth_index<1>::type &hash_index = m_cache.get<1>();
  uint64_t key = ((uint64_t)file_id << 32) | file_offset;

  if (length > m_max_memory || hash_index.find(key) != hash_index.end())
    return false;

  // make room
  if (m_avail_memory < length) {
    BlockCache::iterator iter = m_cache.begin();
    while (iter != m_cache.end()) {
      if ((*iter).ref_count == 0) {
	m_avail_memory += (*iter).length;
	delete [] (*iter).block;
	iter = m_cache.erase(iter);
	if (m_avail_memory >= length)
	  break;
      }
      else
	iter++;
    }
  }

  block_cache_entry entry(file_id, file_offset);
  entry.block = block;
  entry.length = length;
  entry.ref_count = 1;

  std::pair<BlockCache::nth_index<0>::type::iterator, bool> insert_result = m_cache.push_back(entry);
  assert(insert_result.second);

  m_avail_memory -= length;
  
  return true;
}


bool FileBlockCache::contains(int file_id, uint32_t file_offset) {
  boost::mutex::scoped_lock lock(m_mutex);
  BlockCache::nth_index<1>::type &hash_index = m_cache.get<1>();
  uint64_t key = ((uint64_t)file_id << 32) | file_offset;

  return (hash_index.find(key) != hash_index.end());
}
