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

#ifndef HYPERTABLE_MEMORYTRACKER_H
#define HYPERTABLE_MEMORYTRACKER_H

#include <boost/thread/mutex.hpp>

#include "FileBlockCache.h"

namespace Hypertable {

  class MemoryTracker {
  public:
    MemoryTracker(FileBlockCache *block_cache) 
      : m_memory_used(0), m_block_cache(block_cache) { }

    void add(int64_t amount) {
      ScopedLock lock(m_mutex);
      m_memory_used += amount;
    }

    void subtract(int64_t amount) {
      ScopedLock lock(m_mutex);
      m_memory_used -= amount;
    }

    int64_t balance() {
      ScopedLock lock(m_mutex);
      return m_memory_used + m_block_cache->memory_used();
    }

  private:
    Mutex m_mutex;
    int64_t m_memory_used;
    FileBlockCache *m_block_cache;
  };

}

#endif // HYPERTABLE_MEMORYTRACKER_H
