/** -*- C++ -*-
 * Copyright (C) 2009  Luke Lu (llu@hypertable.org)
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

#ifndef HYPERTABLE_ALLOCATOR_H
#define HYPERTABLE_ALLOCATOR_H

#include "Error.h"

namespace Hypertable {

// There used to be std::destroy's, but they never made to the final standard
inline void destruct(char *) {}
inline void destruct(unsigned char *) {}
inline void destruct(short *) {}
inline void destruct(unsigned short *) {}
inline void destruct(int *) {}
inline void destruct(unsigned int *) {}
inline void destruct(long *) {}
inline void destruct(unsigned long *) {}
inline void destruct(long long *) {}
inline void destruct(unsigned long long *) {}
inline void destruct(float *) {}
inline void destruct(double *) {}
inline void destruct(long double *) {}

template <typename T>
inline void destruct(T *p) { p->~T(); }


// convenient function for memory alignment
inline size_t get_align_offset(void *p) {
  if (sizeof(void *) == 8)
    return 8 - (((uint64_t)p) & 0x7);

  return 4 - (((uint32_t)p) & 0x3);
}


// base classes for our stl allocators
template <typename T> class AllocatorBase;

template <>
struct AllocatorBase<void> {
  typedef size_t size_type;
  typedef ptrdiff_t difference_type;
  typedef void *pointer;
  typedef const void *const_pointer;
  // reference to void members are impossible.
  typedef void value_type;
};

template <typename T>
struct AllocatorBase {
  typedef size_t size_type;
  typedef ptrdiff_t difference_type;
  typedef T* pointer;
  typedef const T* const_pointer;
  typedef T& reference;
  typedef const T& const_reference;
  typedef T value_type;

  pointer address(reference x) const { return &x; }
  const_pointer address(const_reference x) const { return &x; }

  // implement the following 2 methods
  // pointer allocate(size_type sz);
  // void deallocate(pointer p, size_type n);

  size_type max_size() const throw() {
    return size_t(-1) / sizeof(value_type);
  }

  void construct(pointer p, const T& val) {
    new(static_cast<void*>(p)) T(val);
  }

  void construct(pointer p) {
    new(static_cast<void*>(p)) T();
  }

  void destroy(pointer p) { destruct(p); }
};


template <typename T, class ArenaT>
class ArenaAllocatorBase : public AllocatorBase<T> {
  typedef AllocatorBase<T> Base;

  ArenaT *m_arenap;

 public:
  typedef typename Base::pointer pointer;
  typedef typename Base::size_type size_type;

  ArenaAllocatorBase() : m_arenap(NULL) {}
  ArenaAllocatorBase(ArenaT &arena) { m_arenap = &arena; }

  template <typename U>
  ArenaAllocatorBase(const ArenaAllocatorBase<U, ArenaT> &copy)
    : m_arenap(copy.m_arenap) {}

  // arena allocators only needs to implement
  // pointer allocate(size_type sz);

  // sanity helper
  inline void check_allocate_size(size_type sz) const {
    if (HT_UNLIKELY(sz > Base::max_size()))
      HT_THROW_(Error::BAD_MEMORY_ALLOCATION);
  }

  // default behavior
  inline pointer default_allocate(size_type sz) {
    return (pointer)(::operator new(sz * sizeof(T)));
  }

  void deallocate(pointer, size_type) {}

  template <typename U>
  bool operator==(const ArenaAllocatorBase<U, ArenaT> &x) const {
    return m_arenap == x.m_arenap;
  }

  template <typename U>
  bool operator!=(const ArenaAllocatorBase<U, ArenaT> &x) const {
    return m_arenap != x.m_arenap;
  }

  template <typename U>
  void swap(ArenaAllocatorBase<U, ArenaT> &other) {
    ArenaT *tmp = m_arenap;
    m_arenap = other.m_arenap;
    other.m_arenap = tmp;
  }

  ArenaT *arena() const { return m_arenap; }
  void set_arena(ArenaT *arena) { m_arenap = arena; }
};

} // namespace Hypertable

#endif /* HYPERTABLE_ALLOCATOR_H */
