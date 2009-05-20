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

#ifndef HYPERTABLE_CELLCACHE_H
#define HYPERTABLE_CELLCACHE_H

#include <map>

#include "Common/Mutex.h"

#include "CellListScanner.h"
#include "CellList.h"

#include "Hypertable/Lib/SerializedKey.h"

#include "CellCachePool.h"
#include "CellCachePoolAllocator.h"

namespace Hypertable {

  /**
   * Represents  a sorted list of key/value pairs in memory.
   * All updates get written to the CellCache and later get "compacted"
   * into a CellStore on disk.
   */
  class CellCache : public CellList {

  public:
    CellCache();
    virtual ~CellCache() { }
    /**
     * Adds a key/value pair to the CellCache.  This method assumes that
     * the CellCache has been locked by a call to #lock.  Copies of
     * the key and value are created and inserted into the underlying cell map
     *
     * @param key key to be inserted
     * @param value value to inserted
     */
    virtual void add(const Key &key, const ByteString value);

    virtual const char *get_split_row();

    virtual void get_split_rows(std::vector<std::string> &split_rows);

    virtual void get_rows(std::vector<std::string> &rows);

    virtual int64_t get_total_entries() { return m_cell_map.size(); }

    /** Creates a CellCacheScanner object that contains an shared pointer
     * (intrusive_ptr) to this CellCache.
     */
    virtual CellListScanner *create_scanner(ScanContextPtr &scan_ctx);

    void lock()   { if (!m_frozen) m_mutex.lock(); }
    void unlock() { if (!m_frozen) m_mutex.unlock(); }

    size_t size() { return m_cell_map.size(); }

    /** Returns the amount of memory used by the CellCache.  This is the
     * summation of the lengths of all the keys and values in the map.
     */
    uint64_t memory_used() {
      ScopedLock lock(m_mutex);
      return m_alloc.memory_used();
    }

    uint32_t get_collision_count() { return m_collisions; }

    uint32_t get_delete_count() { return m_deletes; }

    void freeze() { m_frozen = true; }
    void unfreeze() { m_frozen = false; }

    friend class CellCacheScanner;

  protected:
    typedef std::pair<const SerializedKey, uint32_t> Value;
    typedef CellCachePoolAllocator<Value> Alloc;
    typedef std::map<const SerializedKey, uint32_t,
                     std::less<const SerializedKey>, Alloc> CellMap;

    Mutex              m_mutex;
    CellCachePool      m_alloc;
    CellMap            m_cell_map;
    uint32_t           m_deletes;
    uint32_t           m_collisions;
    bool               m_frozen;

  };

  typedef intrusive_ptr<CellCache> CellCachePtr;

} // namespace Hypertable;

#endif // HYPERTABLE_CELLCACHE_H
