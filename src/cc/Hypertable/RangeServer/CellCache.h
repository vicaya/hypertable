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
#ifndef HYPERTABLE_CELLCACHE_H
#define HYPERTABLE_CELLCACHE_H

#include <map>

#include <boost/thread/mutex.hpp>
#include <boost/intrusive_ptr.hpp>

#include "CellListScanner.h"
#include "CellList.h"

namespace Hypertable {

  /**
   * Represents  a sorted list of key/value pairs in memory.
   * All updates get written to the CellCache and later get "compacted"
   * into a CellStore on disk.
   */
  class CellCache : public CellList {

  public:
    CellCache() : CellList(), m_mutex(), m_lock(m_mutex,false), m_memory_used(0) { return; }
    virtual ~CellCache();

    /**
     * Adds a key/value pair to the CellCache.  This method assumes that
     * the CellCache has been locked by a call to {@link Lock}.  Copies of
     * the key and value are created and inserted into the underlying cell map
     *
     * @param key key to be inserted
     * @param value value to inserted
     * @return zero
     */
    virtual int add(const ByteString32T *key, const ByteString32T *value);

    virtual const char *get_split_row();

    virtual void get_split_rows(std::vector<std::string> &split_rows);

    /**
     * Creates a CellCacheScanner object that contains an shared pointer (intrusive_ptr)
     * to this CellCache.
     */
    virtual CellListScanner *create_scanner(ScanContextPtr &scanContextPtr);

    void lock()   { m_lock.lock(); }
    void unlock() { m_lock.unlock(); }

    /**
     * Makes a copy of this CellCache, but only includes the key/value
     * pairs that have a timestamp greater than the timestamp argument.
     * This method is called after a compaction to drop the key/value
     * pairs that were compacted to disk.
     * 
     * @param timestamp cutoff timestamp
     * @return The new "sliced" copy of the cell cache
     */
    CellCache *slice_copy(uint64_t timestamp);

    /**
     * Returns the amount of memory used by the CellCache.  This is the summation
     * of the lengths of all the keys and values in the map.
     */
    uint64_t memory_used() {
      boost::mutex::scoped_lock lock(m_mutex);
      return m_memory_used;
    }

    friend class CellCacheScanner;

  protected:
    typedef std::map<const ByteString32T *, const ByteString32T *, ltByteString32> CellMapT;

    boost::mutex               m_mutex;
    boost::mutex::scoped_lock  m_lock;
    CellMapT                   m_cell_map;
    uint64_t                   m_memory_used;
  };

  typedef boost::intrusive_ptr<CellCache> CellCachePtr;

}

#endif // HYPERTABLE_CELLCACHE_H

