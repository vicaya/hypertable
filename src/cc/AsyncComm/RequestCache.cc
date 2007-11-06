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

#include <cassert>
using namespace std;

#define DISABLE_LOG_DEBUG 1

#include "Common/Logger.h"

#include "RequestCache.h"
using namespace hypertable;

void RequestCache::insert(uint32_t id, IOHandler *handler, DispatchHandler *dh, boost::xtime &expire) {
  CacheNodeT *node = new CacheNodeT;

  LOG_VA_DEBUG("Adding id %d", id);

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

  LOG_VA_DEBUG("Removing id %d", id);

  IdHandlerMapT::iterator iter = m_id_map.find(id);

  if (iter == m_id_map.end()) {
    LOG_VA_DEBUG("ID %d not found in request cache", id);
    return 0;
  }

  CacheNodeT *node = (*iter).second;

  if (node->prev == 0)
    m_tail = node->next;
  else
    node->prev->next = node->next;

  if (node->next == 0)
    m_head = node->prev;
  else
    node->next->prev = node->prev;

  m_id_map.erase(iter);

  if (node->handler != 0) {
    DispatchHandler *dh = node->dh;
    delete node;
    return dh;
  }
  delete node;
  return 0;
}



DispatchHandler *RequestCache::get_next_timeout(boost::xtime &now, IOHandler *&handlerp, boost::xtime *nextTimeout) {

  while (m_head && xtime_cmp(m_head->expire, now) <= 0) {

    IdHandlerMapT::iterator iter = m_id_map.find(m_head->id);
    assert (iter != m_id_map.end());
    CacheNodeT *node = m_head;
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
    memcpy(nextTimeout, &m_head->expire, sizeof(boost::xtime));
  else
    memset(nextTimeout, 0, sizeof(boost::xtime));

  return 0;
}



void RequestCache::purge_requests(IOHandler *handler) {
  for (CacheNodeT *node = m_tail; node != 0; node = node->next) {
    if (node->handler == handler) {
      LOG_VA_DEBUG("Purging request id %d", node->id);
      node->handler = 0;  // mark for deletion
    }
  }
}

