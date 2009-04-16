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

namespace Hypertable {

  class MemoryTracker {
  public:
    MemoryTracker() : m_memory_used(0) { return; }

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
      return m_memory_used;
    }

  private:
    Mutex m_mutex;
    int64_t m_memory_used;
  };

}

#endif // HYPERTABLE_MEMORYTRACKER_H
