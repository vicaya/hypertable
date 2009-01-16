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

#ifndef HYPERTABLE_LIFEFILETRACKER_H
#define HYPERTABLE_LIFEFILETRACKER_H

#include "Common/Mutex.h"
#include "Common/String.h"

#include "Hypertable/Lib/Schema.h"
#include "Hypertable/Lib/Types.h"

namespace Hypertable {

  typedef hash_map<String, uint32_t> FileRefCountMap;

  /**
   * Tracks files that are live or referenced for purposes of
   * maintaining the 'Files' METADATA column.
   */
  class LiveFileTracker {
  public:

    LiveFileTracker(const TableIdentifier *identifier,
                    SchemaPtr &schema_ptr, 
                    const RangeSpec *range, const String &ag_name);

    /**
     *
     */
    void change_range(const String &start_row, const String &end_row) {
      ScopedLock lock(m_mutex);
      m_start_row = start_row;
      m_end_row = end_row;
    }

    /**
     * Clears the live file set.  Sets the m_need_update flag to true.
     */
    void clear_live() { 
      ScopedLock lock(m_mutex);
      m_live.clear();
      m_need_update = true;
    }

    /**
     * Adds a file to the live file set.
     *
     * @param fname file to add
     */
    void add_live(const String &fname) {
      ScopedLock lock(m_mutex);
      m_live.insert(fname);
      m_need_update = true;
    }

    /**
     * Adds a set of files to the referenced file set.  If they already
     * exist in the referenced file set, then their reference count is
     * incremented.
     *
     * @param filev vector of filenames to add reference
     */
    void add_references(const std::vector<String> &filev);

    /**
     * Decrements the reference count of each file in the given vector.
     * If any of the reference counts drop to zero, then the corresponding
     * file is removed from the reference set.
     *
     * @param filev vector of filenames to remove reference
     */
    void remove_references(const std::vector<String> &filev);


    /**
     * Updates the 'Files' METADATA column if it needs updating.
     */
    void update_files_column();

  private:
    Mutex            m_mutex;
    Mutex            m_update_mutex;
    TableIdentifierManaged m_identifier;
    SchemaPtr        m_schema_ptr;
    String           m_start_row;
    String           m_end_row;
    String           m_ag_name;
    FileRefCountMap  m_referenced;
    std::set<String> m_live;
    std::set<String> m_blocked;
    bool             m_need_update;
    bool             m_is_root;
  };

}

#endif // HYPERTABLE_LIFEFILETRACKER_H
