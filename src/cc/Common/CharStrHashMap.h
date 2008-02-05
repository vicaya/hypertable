/**
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#ifndef HYPERTABLE_CHARSTR_HASHMAP_H
#define HYPERTABLE_CHARSTR_HASHMAP_H

#include "HashMap.h"
#include "CharStrHashTraits.h"

namespace Hypertable {

/**
 * A hash map for storing and lookup char *strings efficiently
 */
template <typename DataT, class TraitsT = CharStrHashTraits>
class CharStrHashMap : public hash_map<const char *, DataT, 
                                       typename TraitsT::hasher, 
                                       typename TraitsT::key_equal> {
private:
  typedef hash_map<const char *, DataT, typename TraitsT::hasher, 
                   typename TraitsT::key_equal> Base;
public:
  typedef typename Base::iterator iterator;
  typedef typename Base::key_type key_type;
  typedef typename Base::value_type value_type;
  typedef typename TraitsT::key_allocator key_allocator;
  typedef std::pair<iterator, bool> InsRet;

private:
  key_allocator m_alloc;

private:
  InsRet
  insert_key(const char *key, InsRet rv) {
    if (rv.second) {
      char *keycopy = m_alloc.dup(key);
      const_cast<key_type &>(rv.first->first) = keycopy;
    }
    return rv;
  }

public: 
  CharStrHashMap() {}
  CharStrHashMap(size_t n_buckets) : Base(n_buckets) {}
  ~CharStrHashMap() {}

  // hides all insert methods in base class
  InsRet 
  insert(const char *key, const DataT &data) {
    return insert_key(key, Base::insert(value_type(key, data)));
  }

  InsRet 
  insert_noresize(const char *key, const DataT &data) {
    return insert_key(key, Base::insert_noresize(value_type(key, data)));
  }

  DataT&
  operator[](const char *key) {
    return insert(key, DataT()).first->second;
  }

  key_allocator &
  key_alloc() { return m_alloc; }

  void
  clear() {
    Base::clear();
    m_alloc.free();
  }
};

} // namespace Hypertable

#endif //! HYPERTABLE_CHARSTR_HASHMAP_H
