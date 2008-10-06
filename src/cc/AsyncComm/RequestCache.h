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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */


#ifndef HYPERTABLE_REQUESTCACHE_H
#define HYPERTABLE_REQUESTCACHE_H

#include <boost/thread/xtime.hpp>

#include "Common/HashMap.h"

#include "DispatchHandler.h"

namespace Hypertable {

  class IOHandler;

  class RequestCache {

    struct CacheNode {
      struct CacheNode  *prev, *next;
      boost::xtime       expire;
      uint32_t           id;
      IOHandler         *handler;
      DispatchHandler   *dh;
    };

    typedef hash_map<uint32_t, CacheNode *> IdHandlerMap;

  public:

    RequestCache() : m_id_map(), m_head(0), m_tail(0) { return; }

    void insert(uint32_t id, IOHandler *handler, DispatchHandler *dh,
                boost::xtime &expire);

    DispatchHandler *remove(uint32_t id);

    DispatchHandler *get_next_timeout(boost::xtime &now, IOHandler *&handlerp,
                                      boost::xtime *next_timeout);

    void purge_requests(IOHandler *handler);

  private:
    IdHandlerMap  m_id_map;
    CacheNode    *m_head, *m_tail;
  };
}

#endif // HYPERTABLE_REQUESTCACHE_H
