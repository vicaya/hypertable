/*
 * Author:  Liu Kejia (liukejia@baidu.com)
 *	  Kong Linghua (konglinghua@baidu.com) 
 *	  Yang Dong (yangdong01@baidu.com)
 *
 * Company:  Baidu.com, Inc.
 *
 * Description: Memory pool used for the CellCache <key, value> pairs and CellMap 
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

#include "CellCachePool.h"
#include "Global.h"

using namespace Hypertable;

uint8_t *CellCachePool::get_buf(size_t sz) {
  m_cur_buf = new BufNode(sz, m_pre_buf);
  if (!m_cur_buf || !m_cur_buf->m_buf) {
    return NULL;
  }
  m_total_allocated += sz + sizeof(BufNode);
  Global::memory_tracker.add(sz + sizeof(BufNode));
  m_pre_buf = m_cur_buf;

  m_head_ptr = m_cur_buf->m_buf; 
  m_tail_ptr = m_cur_buf->m_buf + sz;

  return m_head_ptr;
}

CellCachePool::~CellCachePool() {
  pool_free();
  Global::memory_tracker.subtract(m_total_allocated);
}
