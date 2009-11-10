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

#ifndef HYPERTABLE_PAGEARENA_ALLOCATOR_H
#define HYPERTABLE_PAGEARENA_ALLOCATOR_H

#include "PageArena.h"
#include "Allocator.h"

namespace Hypertable {

template <typename T, class ArenaT = PageArena<> >
struct PageArenaAllocator : public ArenaAllocatorBase<T, ArenaT> {
  typedef ArenaAllocatorBase<T, ArenaT> Base;
  typedef typename Base::pointer pointer;
  typedef typename Base::size_type size_type;

  template <typename U>
  struct rebind { typedef PageArenaAllocator<U, ArenaT> other; };

  PageArenaAllocator() {}
  PageArenaAllocator(ArenaT &arena) : Base(arena) {}

  template <typename U>
  PageArenaAllocator(const PageArenaAllocator<U, ArenaT> &copy) {
    set_arena(copy.arena());
  }

  pointer allocate(size_type sz) {
    check_allocate_size(sz);

    if (HT_LIKELY(Base::arena() != NULL))
      return (pointer)(Base::arena()->alloc_aligned(sz * sizeof(T)));

    return default_allocate(sz);
  }
};

template <typename T, class ArenaT = PageArena<> >
struct PageArenaDownAllocator : public ArenaAllocatorBase<T, ArenaT> {
  typedef ArenaAllocatorBase<T, ArenaT> Base;
  typedef typename Base::pointer pointer;
  typedef typename Base::size_type size_type;

  template <typename U>
  struct rebind { typedef PageArenaDownAllocator<U, ArenaT> other; };

  PageArenaDownAllocator() {}
  PageArenaDownAllocator(ArenaT &arena) : Base(arena) {}

  template <typename U>
  PageArenaDownAllocator(const PageArenaDownAllocator<U, ArenaT> &copy) {
    set_arena(copy.arena());
  }

  pointer allocate(size_type sz) {
    check_allocate_size(sz);

    if (HT_LIKELY(Base::arena() != NULL))
      return (pointer)(Base::arena()->alloc_down(sz * sizeof(T)));

    return default_allocate(sz);
  }
};

} // namespace Hypertable

#endif /* HYPERTABLE_PAGEARENA_ALLOCATOR_H */
