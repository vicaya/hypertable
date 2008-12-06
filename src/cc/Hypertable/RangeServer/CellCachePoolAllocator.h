/*
 * Author:  Liu Kejia (liukejia@baidu.com)
 *        Kong Linghua (konglinghua@baidu.com)
 *        Yang Dong (yangdong01@baidu.com)
 *
 * Company:  Baidu.com, Inc.
 *
 * Description: STL Allocator interface for CellCachePool, used for CellMap
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

#ifndef CELL_CACHE_POOL_ALLOCATOR_H
#define CELL_CACHE_POOL_ALLOCATOR_H

#include "CellCachePool.h"

namespace Hypertable {

  template <typename T> class CellCachePoolAllocator;

  template <> class CellCachePoolAllocator<void> {
  public:
    typedef void* pointer;
    typedef const void* const_pointer;
    // reference to void members are impossible.
    typedef void value_type;

    template <class U>
      struct rebind { typedef CellCachePoolAllocator<U> other; };
  };

  namespace pool_alloc{
    inline void destruct(char *){}
    inline void destruct(wchar_t*){}

    template <typename T>
      inline void destruct(T *t){t->~T();}
  }

  template <typename T>
    class CellCachePoolAllocator {
  public:
    typedef size_t size_type;
    typedef ptrdiff_t difference_type;
    typedef T* pointer;
    typedef const T* const_pointer;
    typedef T& reference;
    typedef const T& const_reference;
    typedef T value_type;

    template <class U>
    struct rebind { typedef CellCachePoolAllocator<U> other; };

    CellCachePoolAllocator(){}
    CellCachePoolAllocator(CellCachePool &p) {m_mem = &p;}
    pointer address(reference x) const {return &x;}
    const_pointer address(const_reference x) const {return &x;}

    template <class U>
    CellCachePoolAllocator(const CellCachePoolAllocator<U>& other) {
      this->m_mem = other.m_mem;
    }

    // implement the following 2 methods
    pointer allocate(size_type sz) {
      return static_cast<pointer>(m_mem->allocate(sz*sizeof(T), true));
    }

    void deallocate(pointer p, size_type n) {
      m_mem->deallocate(p, n);
    }

    size_type max_size() const throw() {return size_t(-1) / sizeof(value_type);}

    void construct(pointer p, const T& val) {
      new(static_cast<void*>(p)) T(val);
    }

    void construct(pointer p) {
      new(static_cast<void*>(p)) T();
    }

    void destroy(pointer p){pool_alloc::destruct(p);}

    CellCachePool *get_pool() { return m_mem; }
    void set_pool(CellCachePool * p) { m_mem = p; }

    void dump(){m_mem->dump_stat();};

    CellCachePool *m_mem;
  };

  template <typename T, typename U>
    inline bool operator==(const CellCachePoolAllocator<T>&,
                           const CellCachePoolAllocator<U>){return true;}

  template <typename T, typename U>
    inline bool operator!=(const CellCachePoolAllocator<T>&,
                           const CellCachePoolAllocator<U>){return false;}

}

#endif
