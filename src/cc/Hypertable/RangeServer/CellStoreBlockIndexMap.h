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

#ifndef HYPERTABLE_CELLSTOREBLOCKINDEXMAP_H
#define HYPERTABLE_CELLSTOREBLOCKINDEXMAP_H

#include <cassert>
#include <map>

#include "Common/StaticBuffer.h"

#include "Hypertable/Lib/SerializedKey.h"


namespace Hypertable {

  /**
   * Provides an STL-style iterator on CellStoreBlockIndex objects.
   */
  template <typename OffsetT>
  class CellStoreBlockIndexIteratorMap {
  public:
    typedef typename std::map<const SerializedKey, OffsetT>::iterator MapIteratorT;

    CellStoreBlockIndexIteratorMap() { }
    CellStoreBlockIndexIteratorMap(MapIteratorT iter) : m_iter(iter) { }
    SerializedKey key() { return (*m_iter).first; }
    int64_t value() { return (int64_t)(*m_iter).second; }
    CellStoreBlockIndexIteratorMap &operator++() { ++m_iter; return *this; }
    CellStoreBlockIndexIteratorMap operator++(int) { 
      CellStoreBlockIndexIteratorMap<OffsetT> copy(*this);
      ++(*this);
      return copy;
    }
    bool operator==(const CellStoreBlockIndexIteratorMap &other) {
      return m_iter == other.m_iter;
    }
    bool operator!=(const CellStoreBlockIndexIteratorMap &other) {
      return m_iter != other.m_iter;
    }
  protected:
    MapIteratorT m_iter;
  };


  /**
   *
   */
  template <typename OffsetT>
  class CellStoreBlockIndexMap {
  public:
    typedef typename Hypertable::CellStoreBlockIndexIteratorMap<OffsetT> iterator;
    typedef typename std::map<const SerializedKey, OffsetT> MapT;
    typedef typename std::map<const SerializedKey, OffsetT>::iterator MapIteratorT;
    typedef typename std::map<const SerializedKey, OffsetT>::value_type value_type;

    CellStoreBlockIndexMap() : m_disk_used(0) { }

    void load(DynamicBuffer &fixed, DynamicBuffer &variable,int64_t end_of_data,
              const String &start_row="", const String &end_row="") {
      size_t total_entries = fixed.fill() / sizeof(OffsetT);
      size_t index_entries = total_entries;
      SerializedKey key;
      OffsetT offset, last_offset = 0;
      const uint8_t *key_ptr;
      bool in_scope = (start_row == "") ? true : false;
      bool check_for_end_row = end_row != "";

      assert(variable.own);

      m_keydata = variable;
      fixed.ptr = fixed.base;
      key_ptr   = m_keydata.base;

      for (size_t i=0; i<index_entries; ++i) {

        // variable portion
        key.ptr = key_ptr;
        key_ptr += key.length();

        // fixed portion (e.g. offset)
        memcpy(&offset, fixed.ptr, sizeof(offset));
        fixed.ptr += sizeof(offset);

        if (!in_scope) {
          if (strcmp(key.row(), start_row.c_str()) < 0)
            continue;
          in_scope = true;
        }
        else if (check_for_end_row && 
                 strcmp(key.row(), end_row.c_str()) > 0) {
          index_entries = std::min(i+2, total_entries);
          check_for_end_row = false;
        }

        m_map.insert(m_map.end(), value_type(key, offset));
        last_offset = offset;
      }

      HT_ASSERT(key_ptr <= (m_keydata.base + m_keydata.size));
      
      if (!m_map.empty()) {

        /** compute space covered by this index scope **/
        if (index_entries == total_entries)
          m_disk_used = end_of_data - (*m_map.begin()).second;
        else
          m_disk_used = last_offset - (*m_map.begin()).second;

        /** determine split key **/
        MapIteratorT iter = m_map.begin();
        for (size_t i=1; i<(m_map.size()+1)/2; ++iter,++i)
          ;
        m_middle_key = (*iter).first;
      }

    }

    const SerializedKey middle_key() { return m_middle_key; }

    size_t memory_used() { return m_keydata.size + (m_map.size() * 32); }

    int64_t disk_used() { return m_disk_used; }

    iterator begin() {
      return iterator(m_map.begin());
    }

    iterator end() {
      return iterator(m_map.end());
    }

    iterator lower_bound(const SerializedKey& k) {
      return iterator(m_map.lower_bound(k));
    }

    iterator upper_bound(const SerializedKey& k) {
      return iterator(m_map.upper_bound(k));
    }

  private:
    MapT m_map;
    StaticBuffer m_keydata;
    SerializedKey m_middle_key;
    int64_t m_disk_used;
  };


} // namespace Hypertable

#endif // HYPERTABLE_CELLSTOREBLOCKINDEXMAP_H
