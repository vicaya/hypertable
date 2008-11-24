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

#include "Common/Compat.h"

#include <cassert>
using namespace std;

#define HT_DISABLE_LOG_DEBUG 1

#include "Common/Logger.h"

#include "IOHandlerData.h"
#include "RequestCache.h"
using namespace Hypertable;

void
RequestCache::insert(uint32_t id, IOHandler *handler, DispatchHandler *dh,
                     boost::xtime &expire) {
  CacheNode *node = new CacheNode;

  HT_DEBUGF("Adding id %d", id);

  IdHandlerMap::iterator iter = m_id_map.find(id);

  HT_ASSERT(iter == m_id_map.end());

  node->id = id;
  node->handler = handler;
  node->dh = dh;
  memcpy(&node->expire, &expire, sizeof(expire));

  if (m_head == 0) {
    node->next = node->prev = 0;
    m_head = m_tail = node;
  }
  else {
    node->next = m_tail;
    node->next->prev = node;
    node->prev = 0;
    m_tail = node;
  }

  m_id_map[id] = node;
}


DispatchHandler *RequestCache::remove(uint32_t id) {

  HT_DEBUGF("Removing id %d", id);

  IdHandlerMap::iterator iter = m_id_map.find(id);

  if (iter == m_id_map.end()) {
    HT_DEBUGF("ID %d not found in request cache", id);
    return 0;
  }

  CacheNode *node = (*iter).second;

  if (node->prev == 0)
    m_tail = node->next;
  else
    node->prev->next = node->next;

  if (node->next == 0)
    m_head = node->prev;
  else
    node->next->prev = node->prev;

  m_id_map.erase(iter);

  DispatchHandler *dh = node->dh;
  delete node;
  return dh;
}



DispatchHandler *
RequestCache::get_next_timeout(boost::xtime &now, IOHandler *&handlerp,
                               boost::xtime *next_timeout) {

  while (m_head && xtime_cmp(m_head->expire, now) <= 0) {

    IdHandlerMap::iterator iter = m_id_map.find(m_head->id);
    assert (iter != m_id_map.end());
    CacheNode *node = m_head;
    if (m_head->prev) {
      m_head = m_head->prev;
      m_head->next = 0;
    }
    else
      m_head = m_tail = 0;

    m_id_map.erase(iter);

    if (node->handler != 0) {
      handlerp = node->handler;
      DispatchHandler *dh = node->dh;
      delete node;
      return dh;
    }
    delete node;
  }

  if (m_head)
    memcpy(next_timeout, &m_head->expire, sizeof(boost::xtime));
  else
    memset(next_timeout, 0, sizeof(boost::xtime));

  return 0;
}



void RequestCache::purge_requests(IOHandler *handler) {
  for (CacheNode *node = m_tail; node != 0; node = node->next) {
    if (node->handler == handler) {
      HT_DEBUGF("Purging request id %d", node->id);
      handler->deliver_event(new Event(Event::ERROR,
          ((IOHandlerData *)handler)->get_address(),
          Error::COMM_REQUEST_TIMEOUT), node->dh);
      node->handler = 0;  // mark for deletion
    }
  }
}

