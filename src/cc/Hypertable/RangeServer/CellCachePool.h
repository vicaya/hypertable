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

#ifndef CELL_CACHE_POOL_H
#define CELL_CACHE_POOL_H

#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>

#include <cstdlib>				/* malloc, free */

#include "Common/Mutex.h"

/* By default, we malloc 512KB for each buffer. 512KB is an experience value */
#define BUF_SIZE 	(512*1024)

namespace Hypertable {

  class CellCachePool {
    /* list node, we use a reverse list here */
    struct BufNode {
      uint8_t	*m_buf;
      BufNode *m_prev;

      BufNode(size_t sz, BufNode *m_prev_node = NULL) {
        this->m_prev = m_prev_node;

        /* alloc memory */
        m_buf = (uint8_t*)malloc(sz);
        if (!m_buf) {
          /* TODO: wait and try for several times */
          std::cerr << "Out of memory!\n";	
        }
      }

      ~BufNode() {
        if (m_buf) {
          free(m_buf);
          m_buf = NULL;
        }
      }
    };

  public:
    CellCachePool(size_t sz = BUF_SIZE): m_buf_size(sz), m_total_allocated(0), m_pre_buf(NULL), m_cur_buf(NULL), m_head_ptr(NULL), m_tail_ptr(NULL) {}

      ~CellCachePool();

      void pool_free() {
        boost::mutex::scoped_lock lock(m_mutex);

        /* free all the memory in the pool */
        while (m_cur_buf) {
          BufNode *tmp = m_cur_buf->m_prev;
          delete m_cur_buf;
          m_cur_buf = tmp;
        }

        /* reset pointers */
        m_pre_buf = NULL;
        m_head_ptr = NULL;
        m_tail_ptr = NULL;
      }

      /* We put data of the the same type together, "is_map" is used for CellMap */
      void *allocate(size_t size, bool is_map = false) {
        boost::mutex::scoped_lock lock(m_mutex);

        /* 
         * If current buffer is full, we create a new buffer automatically
         * and if the request size is larger than the "m_buf_size", 
         * we used the larger one. < REGRESSION >
         */
        if (m_head_ptr + size > m_tail_ptr) {
          int sz = size > m_buf_size? size:m_buf_size;
          if (!get_buf(sz)) return NULL;
        }

        if (is_map) {
          m_tail_ptr -= size;
          return m_tail_ptr;
        } else {
          uint8_t	*ptr = m_head_ptr;
          m_head_ptr += size;
          return ptr;
        }
      }

      void deallocate(void *ptr, size_t sz= 0) {
        /* Unimplemented. No need here. We free all the memory when the CellCache is destructing. */
      }

      /* For debug, not very accurate, but enough */
      void dump_stat() {
        int i = 1;
        BufNode *p = m_cur_buf;
        while((p = p->m_prev)) i++;

        std::cout << "Current Pool Size : " << i * m_buf_size + m_head_ptr - m_tail_ptr <<"; Free size : " << m_tail_ptr - m_head_ptr << std::endl;
      }

  private:
      uint8_t *get_buf(size_t sz);

      Mutex    m_mutex;
      size_t   m_buf_size;		/* buffer's size */
      uint64_t m_total_allocated;
      BufNode *m_pre_buf;
      BufNode *m_cur_buf;

      /* pointers used to manage the current buffer */
      uint8_t	*m_head_ptr;                   
      uint8_t	*m_tail_ptr;                   
  };

}

#endif
