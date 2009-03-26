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

#ifndef HYPERTABLE_TABLEIDCACHE_H
#define HYPERTABLE_TABLEIDCACHE_H

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/mem_fun.hpp>
#include <boost/multi_index/sequenced_index.hpp>

#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"

namespace Hypertable {
  using namespace boost::multi_index;

  class TableIdCache : public ReferenceCount {

  public:
    TableIdCache(int max_entries) : m_max_entries(max_entries) { }
    bool contains(int id);
    void insert(int id);

  private:

    typedef boost::multi_index_container<
      int,
      indexed_by<
        sequenced<>,
        hashed_unique< identity<int> >
      >
    > TableIdCacheT;

    typedef TableIdCacheT::nth_index<1>::type HashIndex;

    Mutex          m_mutex;
    TableIdCacheT  m_cache;
    int            m_max_entries;
  };
  typedef intrusive_ptr<TableIdCache> TableIdCachePtr;
  

}


#endif // HYPERTABLE_TABLEIDCACHE_H
