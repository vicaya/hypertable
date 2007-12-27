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

#ifndef HYPERTABLE_MEMORYTRACKER_H
#define HYPERTABLE_MEMORYTRACKER_H

#include <boost/thread/mutex.hpp>

namespace Hypertable {

  class MemoryTracker {
  public:
    MemoryTracker() : m_memory_used(0), m_item_count(0) { return; }

    void add_memory(uint64_t amount) {
      boost::mutex::scoped_lock lock(m_mutex);
      m_memory_used += amount;
    }

    void remove_memory(uint64_t amount) {
      boost::mutex::scoped_lock lock(m_mutex);
      m_memory_used -= amount;
    }

    uint64_t get_memory() {
      boost::mutex::scoped_lock lock(m_mutex);
      return m_memory_used;
    }

    void add_items(uint64_t count) {
      boost::mutex::scoped_lock lock(m_mutex);
      m_item_count += count;
    }

    void remove_items(uint64_t count) {
      boost::mutex::scoped_lock lock(m_mutex);
      m_item_count -= count;
    }

    uint64_t get_items() {
      boost::mutex::scoped_lock lock(m_mutex);
      return m_item_count;
    }

  private:
    boost::mutex m_mutex;
    uint64_t m_memory_used;
    uint64_t m_item_count;
  };

}

#endif // HYPERTABLE_MEMORYTRACKER_H
